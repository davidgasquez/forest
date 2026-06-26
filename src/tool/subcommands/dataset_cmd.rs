// Copyright 2019-2026 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, create_dir_all},
    io::{BufWriter, Write as _},
    path::{Path, PathBuf},
    str::FromStr as _,
    sync::Arc,
};

use anyhow::{Context as _, bail};
use chrono::{NaiveDate, TimeZone as _, Utc};
use clap::{Args, Subcommand};
use num_bigint::BigInt;
use serde::Serialize;

use crate::{
    blocks::{CachingBlockHeader, Tipset, TipsetKey},
    chain::{ChainStore, index::ResolveNullTipset},
    cli_shared::{chain_path, read_config},
    daemon::db_util::load_all_forest_cars,
    db::{
        CAR_DB_DIR_NAME, DbImpl, MemoryDB,
        car::ManyCar,
        db_engine::{db_root, open_db},
    },
    genesis::read_genesis_header,
    lotus_json::HasLotusJson as _,
    message::ChainMessage,
    networks::{ChainConfig, NetworkChain},
    prelude::ShallowClone as _,
    rpc::{
        state::ApiInvocResult,
        types::{Event, SectorPreCommitOnChainInfo},
    },
    shim::{
        actors::{MinerActorStateLoad as _, is_miner_actor, miner, reward},
        address::Address,
        clock::ChainEpoch,
        econ::TokenAmount,
        executor::Receipt,
        state_tree::ActorState,
    },
    state_manager::{ExecutedTipset, StateManager},
};
use fil_actors_shared::fvm_ipld_bitfield::BitField;
use fvm_ipld_blockstore::Blockstore;

#[derive(Debug, Subcommand)]
pub enum DatasetCommands {
    /// Export canonical chain data as JSONL files.
    Export(ExportCommand),
}

impl DatasetCommands {
    pub async fn run(&self) -> anyhow::Result<()> {
        match self {
            Self::Export(cmd) => cmd.run().await,
        }
    }
}

#[derive(Debug, Args)]
pub struct ExportCommand {
    /// Filecoin network chain.
    #[arg(long, required = true)]
    chain: NetworkChain,
    /// Optional TOML configuration file. Ignored for DB selection when `--db` is set.
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Path to the Forest database root. Defaults to the configured chain data path.
    #[arg(long)]
    db: Option<PathBuf>,
    /// Path to a snapshot CAR, which may be zstd compressed. May be provided multiple times.
    #[arg(long = "snapshot", value_name = "SNAPSHOT", num_args = 1.., conflicts_with = "db")]
    snapshots: Vec<PathBuf>,
    /// Highest epoch to export, inclusive. Conflicts with `--date`.
    #[arg(long, conflicts_with = "date")]
    from: Option<ChainEpoch>,
    /// Lowest epoch to export, inclusive. Conflicts with `--date`.
    #[arg(long, conflicts_with = "date")]
    to: Option<ChainEpoch>,
    /// UTC calendar date to export, in YYYY-MM-DD format.
    #[arg(long, conflicts_with_all = ["from", "to"])]
    date: Option<NaiveDate>,
    /// Directory where JSONL files and manifest.json are written.
    #[arg(short, long)]
    out: PathBuf,
    /// Export only tipsets and block headers. Use this for snapshots without historical messages/state.
    #[arg(long)]
    skip_execution: bool,
    /// Export VM execution traces. Implies execution and can be significantly slower.
    #[arg(long)]
    include_traces: bool,
    /// Export Lily-style Reward actor state rows, filled to one row per epoch.
    #[arg(long)]
    include_chain_rewards: bool,
    /// Export state-derived miner sector lifecycle events.
    #[arg(long)]
    include_sector_events: bool,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "snake_case")]
struct ExportCounts {
    tipsets: u64,
    blocks: u64,
    messages: u64,
    receipts: u64,
    events: u64,
    traces: u64,
    chain_rewards: u64,
    sector_events: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct Manifest {
    chain: NetworkChain,
    source: ExportSource,
    from_epoch: ChainEpoch,
    to_epoch: ChainEpoch,
    date_utc: Option<NaiveDate>,
    skipped_execution: bool,
    included_traces: bool,
    included_chain_rewards: bool,
    included_sector_events: bool,
    counts: ExportCounts,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum ExportSource {
    Database { path: PathBuf },
    Snapshots { paths: Vec<PathBuf> },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct TipsetRow {
    epoch: ChainEpoch,
    tipset_key: String,
    parent_tipset_key: String,
    block_count: usize,
    tipset: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct BlockRow {
    epoch: ChainEpoch,
    tipset_key: String,
    block_cid: String,
    block_index: usize,
    block: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct MessageRow {
    epoch: ChainEpoch,
    tipset_key: String,
    message_cid: String,
    message_index: usize,
    message_type: &'static str,
    message: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct ReceiptRow {
    epoch: ChainEpoch,
    tipset_key: String,
    message_cid: String,
    message_index: usize,
    receipt: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct EventRow {
    epoch: ChainEpoch,
    tipset_key: String,
    message_cid: String,
    message_index: usize,
    event_index: usize,
    event: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct TraceRow {
    epoch: ChainEpoch,
    tipset_key: String,
    message_cid: String,
    message_index: Option<usize>,
    trace_index: usize,
    trace: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct SectorEventRow {
    epoch: ChainEpoch,
    tipset_key: String,
    parent_tipset_key: String,
    state_root_before: String,
    state_root_after: String,
    miner_id: String,
    sector_number: u64,
    event_kind: &'static str,
    confidence: &'static str,
    actor_code_cid_before: Option<String>,
    actor_code_cid_after: String,
    actor_state_cid_before: Option<String>,
    actor_state_cid_after: String,
    sector_size: Option<u64>,
    sector_info_before: Option<serde_json::Value>,
    sector_info_after: Option<serde_json::Value>,
    precommit_info_before: Option<serde_json::Value>,
    precommit_info_after: Option<serde_json::Value>,
    details: serde_json::Value,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case")]
struct ChainRewardRow {
    height: ChainEpoch,
    state_root: String,
    tipset_key: String,
    parent_tipset_key: String,
    source_epoch: ChainEpoch,
    source_state_root: String,
    is_null_epoch: bool,
    actor_state_cid: String,
    actor_code_cid: String,
    actor_balance: String,
    actor_sequence: u64,
    network_version: String,
    reward_actor_version: &'static str,
    reward_state_epoch: ChainEpoch,
    total_mined_reward: String,
    new_reward: String,
    new_reward_smoothed_position_estimate: String,
    new_reward_smoothed_velocity_estimate: String,
    new_baseline_power: String,
    effective_baseline_power: String,
    effective_network_time: ChainEpoch,
    cum_sum_baseline: String,
    cum_sum_realized: String,
    simple_total: String,
    baseline_total: String,
    per_epoch_mined_reward: Option<String>,
    per_epoch_effective_network_time_delta: Option<ChainEpoch>,
    per_epoch_cum_sum_baseline_delta: Option<String>,
    per_epoch_cum_sum_realized_delta: Option<String>,
}

#[derive(Clone)]
struct ChainRewardSource {
    source_epoch: ChainEpoch,
    state_root: String,
    tipset_key: String,
    parent_tipset_key: String,
    actor_state_cid: String,
    actor_code_cid: String,
    actor_balance: String,
    actor_sequence: u64,
    network_version: String,
    reward_actor_version: &'static str,
    reward_state_epoch: ChainEpoch,
    total_mined_reward: String,
    new_reward: String,
    new_reward_smoothed_position_estimate: String,
    new_reward_smoothed_velocity_estimate: String,
    new_baseline_power: String,
    effective_baseline_power: String,
    effective_network_time: ChainEpoch,
    cum_sum_baseline: String,
    cum_sum_realized: String,
    simple_total: String,
    baseline_total: String,
}

struct Writers {
    tipsets: BufWriter<File>,
    blocks: BufWriter<File>,
    messages: BufWriter<File>,
    receipts: BufWriter<File>,
    events: BufWriter<File>,
    traces: BufWriter<File>,
    chain_rewards: BufWriter<File>,
    sector_events: BufWriter<File>,
}

impl Writers {
    fn new(out: &Path) -> anyhow::Result<Self> {
        create_dir_all(out).with_context(|| format!("creating output dir {}", out.display()))?;
        Ok(Self {
            tipsets: create_jsonl(out, "tipsets")?,
            blocks: create_jsonl(out, "blocks")?,
            messages: create_jsonl(out, "messages")?,
            receipts: create_jsonl(out, "receipts")?,
            events: create_jsonl(out, "events")?,
            traces: create_jsonl(out, "traces")?,
            chain_rewards: create_jsonl(out, "chain_rewards")?,
            sector_events: create_jsonl(out, "sector_events")?,
        })
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.tipsets.flush()?;
        self.blocks.flush()?;
        self.messages.flush()?;
        self.receipts.flush()?;
        self.events.flush()?;
        self.traces.flush()?;
        self.chain_rewards.flush()?;
        self.sector_events.flush()?;
        Ok(())
    }
}

impl ExportCommand {
    pub async fn run(&self) -> anyhow::Result<()> {
        let (db, source): (DbImpl, ExportSource) = if self.snapshots.is_empty() {
            let (db_root_path, config_db_config) = if let Some(db) = &self.db {
                (db.clone(), Default::default())
            } else {
                let (_, config) = read_config(self.config.as_ref(), Some(self.chain.clone()))?;
                (db_root(&chain_path(&config))?, config.db_config().clone())
            };

            let db_writer = open_db(db_root_path.clone(), &config_db_config)?;
            let db = Arc::new(ManyCar::new(db_writer));
            load_all_forest_cars(&db, &db_root_path.join(CAR_DB_DIR_NAME))?;
            (db.into(), ExportSource::Database { path: db_root_path })
        } else {
            let snapshots = self.snapshots.clone();
            let db = Arc::new(
                ManyCar::<MemoryDB>::try_from(snapshots.clone()).context("loading snapshots")?,
            );
            (db.into(), ExportSource::Snapshots { paths: snapshots })
        };

        let chain_config = Arc::new(ChainConfig::from_chain(&self.chain));
        let genesis_header =
            read_genesis_header(None, chain_config.genesis_bytes(&db).await?.as_deref(), &db)
                .await?;
        let genesis_timestamp = genesis_header.timestamp;
        let chain_store = ChainStore::new(db.shallow_clone(), chain_config, genesis_header)?;
        let state_manager = StateManager::new(chain_store.shallow_clone())?;
        let (from_epoch, to_epoch) = self.epoch_range(genesis_timestamp)?;

        if from_epoch < to_epoch {
            bail!("--from must be greater than or equal to --to");
        }
        if self.skip_execution && self.include_traces {
            bail!("--include-traces cannot be used with --skip-execution");
        }

        let start_tipset = chain_store.chain_index().load_required_tipset_by_height(
            from_epoch,
            chain_store.heaviest_tipset(),
            ResolveNullTipset::TakeOlder,
        )?;

        let mut writers = Writers::new(&self.out)?;
        let mut counts = ExportCounts::default();

        for tipset in start_tipset
            .chain(&db)
            .take_while(|tipset| tipset.epoch() >= to_epoch)
        {
            export_tipset(&tipset, &mut writers, &mut counts)?;

            if !self.skip_execution {
                let executed = state_manager.load_executed_tipset(&tipset).await?;
                for (message_index, executed_message) in
                    executed.executed_messages.iter().enumerate()
                {
                    export_message(
                        &tipset,
                        message_index,
                        &executed_message.message,
                        &mut writers,
                        &mut counts,
                    )?;
                    export_receipt(
                        &tipset,
                        message_index,
                        executed_message.message.cid(),
                        &executed_message.receipt,
                        &mut writers,
                        &mut counts,
                    )?;

                    for (event_index, event) in executed_message
                        .events
                        .iter()
                        .flat_map(|events| events.iter())
                        .enumerate()
                    {
                        export_event(
                            &tipset,
                            message_index,
                            executed_message.message.cid(),
                            event_index,
                            event.clone().into(),
                            &mut writers,
                            &mut counts,
                        )?;
                    }
                }

                let execution_traces = if self.include_traces || self.include_sector_events {
                    Some(state_manager.execution_trace(&tipset).await?.1)
                } else {
                    None
                };

                if self.include_traces {
                    export_traces(
                        &tipset,
                        &executed,
                        execution_traces
                            .as_ref()
                            .context("execution traces were not loaded")?
                            .iter(),
                        &mut writers,
                        &mut counts,
                    )?;
                }
                if self.include_sector_events {
                    export_sector_events(
                        &state_manager,
                        &tipset,
                        &executed,
                        execution_traces
                            .as_deref()
                            .context("execution traces were not loaded")?,
                        &mut writers,
                        &mut counts,
                    )
                    .with_context(|| {
                        format!("exporting sector events for epoch {}", tipset.epoch())
                    })?;
                }
            }
        }

        if self.include_chain_rewards {
            export_chain_rewards(
                &chain_store,
                &state_manager,
                from_epoch,
                to_epoch,
                &mut writers,
                &mut counts,
            )?;
        }

        writers.flush()?;
        write_manifest(
            &self.out,
            Manifest {
                chain: self.chain.clone(),
                source,
                from_epoch,
                to_epoch,
                date_utc: self.date,
                skipped_execution: self.skip_execution,
                included_traces: self.include_traces,
                included_chain_rewards: self.include_chain_rewards,
                included_sector_events: self.include_sector_events,
                counts,
            },
        )?;
        Ok(())
    }

    fn epoch_range(&self, genesis_timestamp: u64) -> anyhow::Result<(ChainEpoch, ChainEpoch)> {
        match (self.date, self.from, self.to) {
            (Some(date), None, None) => {
                let start = Utc
                    .from_utc_datetime(
                        &date
                            .and_hms_opt(0, 0, 0)
                            .context("invalid date start time")?,
                    )
                    .timestamp();
                let end = Utc
                    .from_utc_datetime(
                        &date
                            .succ_opt()
                            .context("date overflow")?
                            .and_hms_opt(0, 0, 0)
                            .context("invalid date end time")?,
                    )
                    .timestamp();
                epoch_range_for_timestamps(genesis_timestamp, start, end)
            }
            (None, Some(from), Some(to)) => Ok((from, to)),
            (None, None, None) => bail!("provide either --date or both --from and --to"),
            _ => bail!("provide both --from and --to, or use --date"),
        }
    }
}

fn epoch_range_for_timestamps(
    genesis_timestamp: u64,
    start_timestamp: i64,
    end_timestamp: i64,
) -> anyhow::Result<(ChainEpoch, ChainEpoch)> {
    if end_timestamp <= start_timestamp {
        bail!("end timestamp must be greater than start timestamp");
    }
    let genesis_timestamp = genesis_timestamp as i64;
    let start_delta = start_timestamp - genesis_timestamp;
    let end_delta = end_timestamp - genesis_timestamp;
    if end_delta <= 0 {
        bail!("date range is before genesis");
    }
    let to = start_delta.div_euclid(30).max(0);
    let from = (end_delta - 1).div_euclid(30);
    Ok((from, to))
}

fn export_tipset(
    tipset: &Tipset,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let tipset_key = format_tipset_key(tipset.key());
    write_json_line(
        &mut writers.tipsets,
        &TipsetRow {
            epoch: tipset.epoch(),
            tipset_key: tipset_key.clone(),
            parent_tipset_key: format_tipset_key(tipset.parents()),
            block_count: tipset.len(),
            tipset: tipset.clone().into_lotus_json_value()?,
        },
    )?;
    counts.tipsets += 1;

    for (block_index, block) in tipset.block_headers().iter().enumerate() {
        export_block(
            tipset.epoch(),
            &tipset_key,
            block_index,
            block,
            writers,
            counts,
        )?;
    }

    Ok(())
}

fn export_block(
    epoch: ChainEpoch,
    tipset_key: &str,
    block_index: usize,
    block: &CachingBlockHeader,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    write_json_line(
        &mut writers.blocks,
        &BlockRow {
            epoch,
            tipset_key: tipset_key.to_owned(),
            block_cid: block.cid().to_string(),
            block_index,
            block: block.clone().into_lotus_json_value()?,
        },
    )?;
    counts.blocks += 1;
    Ok(())
}

fn export_message(
    tipset: &Tipset,
    message_index: usize,
    message: &ChainMessage,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let (message_type, message_json) = match message {
        ChainMessage::Unsigned(message) => ("unsigned", message.clone().into_lotus_json_value()?),
        ChainMessage::Signed(message) => ("signed", message.clone().into_lotus_json_value()?),
    };

    write_json_line(
        &mut writers.messages,
        &MessageRow {
            epoch: tipset.epoch(),
            tipset_key: format_tipset_key(tipset.key()),
            message_cid: message.cid().to_string(),
            message_index,
            message_type,
            message: message_json,
        },
    )?;
    counts.messages += 1;
    Ok(())
}

fn export_receipt(
    tipset: &Tipset,
    message_index: usize,
    message_cid: cid::Cid,
    receipt: &Receipt,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    write_json_line(
        &mut writers.receipts,
        &ReceiptRow {
            epoch: tipset.epoch(),
            tipset_key: format_tipset_key(tipset.key()),
            message_cid: message_cid.to_string(),
            message_index,
            receipt: receipt.clone().into_lotus_json_value()?,
        },
    )?;
    counts.receipts += 1;
    Ok(())
}

fn export_event(
    tipset: &Tipset,
    message_index: usize,
    message_cid: cid::Cid,
    event_index: usize,
    event: Event,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    write_json_line(
        &mut writers.events,
        &EventRow {
            epoch: tipset.epoch(),
            tipset_key: format_tipset_key(tipset.key()),
            message_cid: message_cid.to_string(),
            message_index,
            event_index,
            event: event.into_lotus_json_value()?,
        },
    )?;
    counts.events += 1;
    Ok(())
}

fn export_traces<'a>(
    tipset: &Tipset,
    executed: &ExecutedTipset,
    traces: impl Iterator<Item = &'a Arc<ApiInvocResult>>,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    for (trace_index, trace) in traces.enumerate() {
        write_json_line(
            &mut writers.traces,
            &TraceRow {
                epoch: tipset.epoch(),
                tipset_key: format_tipset_key(tipset.key()),
                message_cid: trace.msg_cid.to_string(),
                message_index: executed
                    .executed_messages
                    .iter()
                    .position(|executed_message| executed_message.message.cid() == trace.msg_cid),
                trace_index,
                trace: trace.as_ref().clone().into_lotus_json_value()?,
            },
        )?;
        counts.traces += 1;
    }
    Ok(())
}

fn export_sector_events(
    state_manager: &StateManager,
    tipset: &Tipset,
    executed: &ExecutedTipset,
    traces: &[Arc<ApiInvocResult>],
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let changed_miners = changed_miner_actors(
        state_manager,
        tipset.parent_state(),
        &executed.state_root,
        traces,
    )?;
    for changed_miner in changed_miners {
        let post_state = miner::State::load(
            state_manager.db(),
            changed_miner.post_actor.code,
            changed_miner.post_actor.state,
        )
        .with_context(|| format!("loading current miner {}", changed_miner.address))?;
        let pre_state = changed_miner
            .pre_actor
            .as_ref()
            .map(|actor| {
                miner::State::load(state_manager.db(), actor.code, actor.state)
                    .with_context(|| format!("loading previous miner {}", changed_miner.address))
            })
            .transpose()?;
        export_miner_sector_events(
            state_manager,
            tipset,
            executed,
            &changed_miner,
            pre_state.as_ref(),
            &post_state,
            writers,
            counts,
        )?;
    }
    Ok(())
}

struct ChangedMinerActor {
    address: Address,
    pre_actor: Option<ActorState>,
    post_actor: ActorState,
}

fn changed_miner_actors(
    state_manager: &StateManager,
    pre_root: &cid::Cid,
    post_root: &cid::Cid,
    traces: &[Arc<ApiInvocResult>],
) -> anyhow::Result<Vec<ChangedMinerActor>> {
    let pre_tree = state_manager.get_state_tree(pre_root)?;
    let post_tree = state_manager.get_state_tree(post_root)?;
    let mut candidates = BTreeMap::new();
    for trace in traces {
        if let Some(execution_trace) = &trace.execution_trace {
            collect_trace_recipients(execution_trace, &mut candidates);
        }
    }
    let mut changed = Vec::new();
    for address in candidates.into_values() {
        let Some(post_actor) = post_tree.get_actor(&address)? else {
            continue;
        };
        if !is_miner_actor(&post_actor.code) {
            continue;
        }
        let pre_actor = pre_tree.get_actor(&address)?;
        if pre_actor.as_ref() == Some(&post_actor) {
            continue;
        }
        changed.push(ChangedMinerActor {
            address,
            pre_actor,
            post_actor,
        });
    }
    Ok(changed)
}

fn collect_trace_recipients(
    trace: &crate::rpc::state::ExecutionTrace,
    recipients: &mut BTreeMap<String, Address>,
) {
    recipients.insert(trace.msg.to.to_string(), trace.msg.to);
    for subcall in &trace.subcalls {
        collect_trace_recipients(subcall, recipients);
    }
}

fn export_miner_sector_events(
    state_manager: &StateManager,
    tipset: &Tipset,
    executed: &ExecutedTipset,
    changed_miner: &ChangedMinerActor,
    pre_state: Option<&miner::State>,
    post_state: &miner::State,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let store = state_manager.db();
    let policy = &state_manager.chain_config().policy;
    let sector_size = post_state
        .info(store)
        .ok()
        .map(|info| info.sector_size() as u64);
    let pre_sector_sets = pre_state
        .map(|state| load_sector_sets(state, policy, store))
        .transpose()?
        .unwrap_or_default();
    let post_sector_sets = load_sector_sets(post_state, policy, store)?;

    emit_bitfield_events(
        &pre_sector_sets.live,
        &post_sector_sets.live,
        "sector_activated",
        "exact",
        &SectorEventContext {
            state_manager,
            tipset,
            executed,
            changed_miner,
            pre_state,
            post_state,
            sector_size,
        },
        writers,
        counts,
    )?;
    emit_bitfield_events(
        &pre_sector_sets.faulty,
        &post_sector_sets.faulty,
        "sector_faulted",
        "exact",
        &SectorEventContext {
            state_manager,
            tipset,
            executed,
            changed_miner,
            pre_state,
            post_state,
            sector_size,
        },
        writers,
        counts,
    )?;
    emit_bitfield_events(
        &pre_sector_sets.recovering,
        &post_sector_sets.recovering,
        "sector_recovering",
        "exact",
        &SectorEventContext {
            state_manager,
            tipset,
            executed,
            changed_miner,
            pre_state,
            post_state,
            sector_size,
        },
        writers,
        counts,
    )?;
    emit_recovered_events(
        &pre_sector_sets.faulty,
        &post_sector_sets.active,
        &SectorEventContext {
            state_manager,
            tipset,
            executed,
            changed_miner,
            pre_state,
            post_state,
            sector_size,
        },
        writers,
        counts,
    )?;
    emit_removed_live_events(
        &pre_sector_sets.live,
        &post_sector_sets.live,
        &SectorEventContext {
            state_manager,
            tipset,
            executed,
            changed_miner,
            pre_state,
            post_state,
            sector_size,
        },
        writers,
        counts,
    )?;

    if pre_state.map(miner::State::allocated_sectors) != Some(post_state.allocated_sectors()) {
        let pre_allocated = pre_state
            .map(|state| state.load_allocated_sector_numbers(store))
            .transpose()?
            .unwrap_or_else(BitField::new);
        let post_allocated = post_state.load_allocated_sector_numbers(store)?;
        emit_bitfield_events(
            &pre_allocated,
            &post_allocated,
            "sector_allocated",
            "exact",
            &SectorEventContext {
                state_manager,
                tipset,
                executed,
                changed_miner,
                pre_state,
                post_state,
                sector_size,
            },
            writers,
            counts,
        )?;
    }

    if pre_state.map(miner::State::pre_committed_sectors)
        != Some(post_state.pre_committed_sectors())
    {
        let pre_precommits = pre_state
            .map(|state| load_precommit_map(store, state))
            .transpose()?
            .unwrap_or_default();
        let post_precommits = load_precommit_map(store, post_state)?;
        emit_map_key_events(
            &pre_precommits,
            &post_precommits,
            "precommit_added",
            "precommit_removed",
            &SectorEventContext {
                state_manager,
                tipset,
                executed,
                changed_miner,
                pre_state,
                post_state,
                sector_size,
            },
            writers,
            counts,
        )?;
    }

    if pre_state.map(miner::State::sectors) != Some(post_state.sectors()) {
        let pre_sectors = pre_state
            .map(|state| load_sector_map(store, state))
            .transpose()?
            .unwrap_or_default();
        let post_sectors = load_sector_map(store, post_state)?;
        emit_sector_info_events(
            &pre_sectors,
            &post_sectors,
            &SectorEventContext {
                state_manager,
                tipset,
                executed,
                changed_miner,
                pre_state,
                post_state,
                sector_size,
            },
            writers,
            counts,
        )?;
    }

    Ok(())
}

#[derive(Default)]
struct SectorSets {
    live: BitField,
    active: BitField,
    faulty: BitField,
    recovering: BitField,
}

fn load_sector_sets(
    state: &miner::State,
    policy: &crate::shim::runtime::Policy,
    store: &impl Blockstore,
) -> anyhow::Result<SectorSets> {
    let mut live = Vec::new();
    let mut active = Vec::new();
    let mut faulty = Vec::new();
    let mut recovering = Vec::new();
    state.for_each_deadline(policy, store, |_deadline_index, deadline| {
        deadline.for_each(store, |_partition_index, partition| {
            live.push(partition.live_sectors());
            active.push(partition.active_sectors());
            faulty.push(partition.faulty_sectors().clone());
            recovering.push(partition.recovering_sectors().clone());
            Ok(())
        })
    })?;
    Ok(SectorSets {
        live: BitField::union(live.iter()),
        active: BitField::union(active.iter()),
        faulty: BitField::union(faulty.iter()),
        recovering: BitField::union(recovering.iter()),
    })
}

struct SectorEventContext<'a> {
    state_manager: &'a StateManager,
    tipset: &'a Tipset,
    executed: &'a ExecutedTipset,
    changed_miner: &'a ChangedMinerActor,
    pre_state: Option<&'a miner::State>,
    post_state: &'a miner::State,
    sector_size: Option<u64>,
}

fn emit_bitfield_events(
    previous: &BitField,
    current: &BitField,
    event_kind: &'static str,
    confidence: &'static str,
    context: &SectorEventContext<'_>,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let added = current - previous;
    for sector_number in bitfield_numbers(&added) {
        write_sector_event(
            context,
            sector_number,
            event_kind,
            confidence,
            serde_json::json!({ "from_bitfield": false, "to_bitfield": true }),
            writers,
            counts,
        )?;
    }
    Ok(())
}

fn emit_recovered_events(
    previous_faulty: &BitField,
    current_active: &BitField,
    context: &SectorEventContext<'_>,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let recovered = previous_faulty & current_active;
    for sector_number in bitfield_numbers(&recovered) {
        write_sector_event(
            context,
            sector_number,
            "sector_recovered",
            "exact",
            serde_json::json!({ "previous_faulty": true, "current_active": true }),
            writers,
            counts,
        )?;
    }
    Ok(())
}

fn emit_removed_live_events(
    previous_live: &BitField,
    current_live: &BitField,
    context: &SectorEventContext<'_>,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let removed = previous_live - current_live;
    let pre_sectors = context
        .pre_state
        .map(|state| load_sector_map(context.state_manager.db(), state))
        .transpose()?
        .unwrap_or_default();
    for sector_number in bitfield_numbers(&removed) {
        let previous_sector = pre_sectors.get(&sector_number);
        let (event_kind, confidence) = if previous_sector
            .map(|sector| sector.expiration <= context.tipset.epoch())
            .unwrap_or(false)
        {
            ("sector_expired", "classified")
        } else {
            ("sector_terminated_or_removed", "classified")
        };
        write_sector_event(
            context,
            sector_number,
            event_kind,
            confidence,
            serde_json::json!({
                "previous_live": true,
                "current_live": false,
                "previous_expiration": previous_sector.map(|sector| sector.expiration),
            }),
            writers,
            counts,
        )?;
    }
    Ok(())
}

fn emit_map_key_events(
    previous: &BTreeMap<u64, SectorPreCommitOnChainInfo>,
    current: &BTreeMap<u64, SectorPreCommitOnChainInfo>,
    added_kind: &'static str,
    removed_kind: &'static str,
    context: &SectorEventContext<'_>,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    for sector_number in current
        .keys()
        .filter(|sector| !previous.contains_key(sector))
    {
        write_sector_event(
            context,
            *sector_number,
            added_kind,
            "exact",
            serde_json::json!({ "from_map": false, "to_map": true }),
            writers,
            counts,
        )?;
    }
    for sector_number in previous
        .keys()
        .filter(|sector| !current.contains_key(sector))
    {
        write_sector_event(
            context,
            *sector_number,
            removed_kind,
            "exact",
            serde_json::json!({ "from_map": true, "to_map": false }),
            writers,
            counts,
        )?;
    }
    Ok(())
}

fn emit_sector_info_events(
    previous: &BTreeMap<u64, miner::SectorOnChainInfo>,
    current: &BTreeMap<u64, miner::SectorOnChainInfo>,
    context: &SectorEventContext<'_>,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    for (sector_number, current_sector) in current {
        let Some(previous_sector) = previous.get(sector_number) else {
            continue;
        };
        if previous_sector.expiration != current_sector.expiration {
            write_sector_event(
                context,
                *sector_number,
                "sector_extended",
                "exact",
                serde_json::json!({
                    "expiration_before": previous_sector.expiration,
                    "expiration_after": current_sector.expiration,
                }),
                writers,
                counts,
            )?;
        }
        if previous_sector.sector_key_cid.is_none() && current_sector.sector_key_cid.is_some() {
            write_sector_event(
                context,
                *sector_number,
                "sector_snapped",
                "exact",
                serde_json::json!({
                    "sector_key_cid_before": previous_sector.sector_key_cid.map(|cid| cid.to_string()),
                    "sector_key_cid_after": current_sector.sector_key_cid.map(|cid| cid.to_string()),
                }),
                writers,
                counts,
            )?;
        }
        if previous_sector.deal_ids != current_sector.deal_ids {
            write_sector_event(
                context,
                *sector_number,
                "sector_deals_changed",
                "exact",
                serde_json::json!({
                    "deal_ids_before": previous_sector.deal_ids,
                    "deal_ids_after": current_sector.deal_ids,
                }),
                writers,
                counts,
            )?;
        }
    }
    Ok(())
}

fn write_sector_event(
    context: &SectorEventContext<'_>,
    sector_number: u64,
    event_kind: &'static str,
    confidence: &'static str,
    details: serde_json::Value,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let store = context.state_manager.db();
    let sector_filter = BitField::try_from_bits([sector_number])?;
    let sector_info_before = match context.pre_state {
        Some(state) => try_load_one_sector_info(store, state, &sector_filter),
        None => None,
    }
    .map(serde_json::to_value)
    .transpose()?;
    let sector_info_after = try_load_one_sector_info(store, context.post_state, &sector_filter)
        .map(serde_json::to_value)
        .transpose()?;
    let precommit_info_before = context
        .pre_state
        .and_then(|state| try_load_precommit_info(store, state, sector_number))
        .map(|precommit| precommit.into_lotus_json_value())
        .transpose()?;
    let precommit_info_after = try_load_precommit_info(store, context.post_state, sector_number)
        .map(|precommit| precommit.into_lotus_json_value())
        .transpose()?;

    write_json_line(
        &mut writers.sector_events,
        &SectorEventRow {
            epoch: context.tipset.epoch(),
            tipset_key: format_tipset_key(context.tipset.key()),
            parent_tipset_key: format_tipset_key(context.tipset.parents()),
            state_root_before: context.tipset.parent_state().to_string(),
            state_root_after: context.executed.state_root.to_string(),
            miner_id: context.changed_miner.address.to_string(),
            sector_number,
            event_kind,
            confidence,
            actor_code_cid_before: context
                .changed_miner
                .pre_actor
                .as_ref()
                .map(|actor| actor.code.to_string()),
            actor_code_cid_after: context.changed_miner.post_actor.code.to_string(),
            actor_state_cid_before: context
                .changed_miner
                .pre_actor
                .as_ref()
                .map(|actor| actor.state.to_string()),
            actor_state_cid_after: context.changed_miner.post_actor.state.to_string(),
            sector_size: context.sector_size,
            sector_info_before,
            sector_info_after,
            precommit_info_before,
            precommit_info_after,
            details,
        },
    )?;
    counts.sector_events += 1;
    Ok(())
}

fn load_one_sector_info(
    store: &impl Blockstore,
    state: &miner::State,
    sector_filter: &BitField,
) -> anyhow::Result<Option<miner::SectorOnChainInfo>> {
    Ok(state
        .load_sectors(store, Some(sector_filter))?
        .into_iter()
        .next())
}

fn try_load_one_sector_info(
    store: &impl Blockstore,
    state: &miner::State,
    sector_filter: &BitField,
) -> Option<miner::SectorOnChainInfo> {
    load_one_sector_info(store, state, sector_filter)
        .ok()
        .flatten()
}

fn try_load_precommit_info(
    store: &impl Blockstore,
    state: &miner::State,
    sector_number: u64,
) -> Option<SectorPreCommitOnChainInfo> {
    state
        .load_precommit_on_chain_info(store, sector_number)
        .ok()
        .flatten()
}

fn load_sector_map(
    store: &impl Blockstore,
    state: &miner::State,
) -> anyhow::Result<BTreeMap<u64, miner::SectorOnChainInfo>> {
    Ok(state
        .load_sectors(store, None)?
        .into_iter()
        .map(|sector| (sector.sector_number, sector))
        .collect())
}

fn load_precommit_map(
    store: &(impl Blockstore + crate::prelude::ShallowClone),
    state: &miner::State,
) -> anyhow::Result<BTreeMap<u64, SectorPreCommitOnChainInfo>> {
    let mut precommits = BTreeMap::new();
    match state {
        miner::State::V8(state) => {
            let map = fil_actors_shared::v8::make_map_with_root::<
                _,
                fil_actor_miner_state::v8::SectorPreCommitOnChainInfo,
            >(&state.pre_committed_sectors, store)?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V9(state) => {
            let map = fil_actors_shared::v9::make_map_with_root::<
                _,
                fil_actor_miner_state::v9::SectorPreCommitOnChainInfo,
            >(&state.pre_committed_sectors, store)?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V10(state) => {
            let map = fil_actors_shared::v10::make_map_with_root::<
                _,
                fil_actor_miner_state::v10::SectorPreCommitOnChainInfo,
            >(&state.pre_committed_sectors, store)?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V11(state) => {
            let map = fil_actors_shared::v11::make_map_with_root::<
                _,
                fil_actor_miner_state::v11::SectorPreCommitOnChainInfo,
            >(&state.pre_committed_sectors, store)?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V12(state) => {
            let map = fil_actors_shared::v12::make_map_with_root::<
                _,
                fil_actor_miner_state::v12::SectorPreCommitOnChainInfo,
            >(&state.pre_committed_sectors, store)?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V13(state) => {
            let map = fil_actors_shared::v13::make_map_with_root::<
                _,
                fil_actor_miner_state::v13::SectorPreCommitOnChainInfo,
            >(&state.pre_committed_sectors, store)?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V14(state) => {
            let map = fil_actor_miner_state::v14::PreCommitMap::load(
                store,
                &state.pre_committed_sectors,
                fil_actor_miner_state::v14::PRECOMMIT_CONFIG,
                "precommits",
            )?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V15(state) => {
            let map = fil_actor_miner_state::v15::PreCommitMap::load(
                store,
                &state.pre_committed_sectors,
                fil_actor_miner_state::v15::PRECOMMIT_CONFIG,
                "precommits",
            )?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V16(state) => {
            let map = fil_actor_miner_state::v16::PreCommitMap::load(
                store,
                &state.pre_committed_sectors,
                fil_actor_miner_state::v16::PRECOMMIT_CONFIG,
                "precommits",
            )?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V17(state) => {
            let map = fil_actor_miner_state::v17::PreCommitMap::load(
                store,
                &state.pre_committed_sectors,
                fil_actor_miner_state::v17::PRECOMMIT_CONFIG,
                "precommits",
            )?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
        miner::State::V18(state) => {
            let map = fil_actor_miner_state::v18::PreCommitMap::load(
                store,
                &state.pre_committed_sectors,
                fil_actor_miner_state::v18::PRECOMMIT_CONFIG,
                "precommits",
            )?;
            map.for_each(|_key, value| {
                precommits.insert(value.info.sector_number, value.clone().into());
                Ok(())
            })?;
        }
    }
    Ok(precommits)
}

fn bitfield_numbers(bitfield: &BitField) -> BTreeSet<u64> {
    bitfield.iter().collect()
}

fn export_chain_rewards(
    chain_store: &ChainStore,
    state_manager: &StateManager,
    from_epoch: ChainEpoch,
    to_epoch: ChainEpoch,
    writers: &mut Writers,
    counts: &mut ExportCounts,
) -> anyhow::Result<()> {
    let start_tipset = chain_store.chain_index().load_required_tipset_by_height(
        from_epoch,
        chain_store.heaviest_tipset(),
        ResolveNullTipset::TakeNewer,
    )?;

    let mut rows = Vec::new();
    let mut current_height = from_epoch;
    let mut current_source = None;

    for tipset in start_tipset.chain(state_manager.db()) {
        let source = load_chain_reward_source(state_manager, &tipset)?;

        if tipset.epoch() > current_height {
            current_source = Some(source);
            continue;
        }

        if let Some(source) = &current_source {
            while current_height > tipset.epoch() && current_height >= to_epoch {
                rows.push(source.to_row(current_height, true));
                current_height -= 1;
            }
        }

        if current_height < to_epoch {
            break;
        }

        if tipset.epoch() == current_height {
            rows.push(source.to_row(current_height, false));
            current_height -= 1;
            current_source = Some(source);
        }

        if current_height < to_epoch {
            break;
        }
    }

    if let Some(source) = &current_source {
        while current_height >= to_epoch {
            rows.push(source.to_row(current_height, true));
            current_height -= 1;
        }
    }

    add_chain_reward_deltas(&mut rows)?;

    for row in rows {
        write_json_line(&mut writers.chain_rewards, &row)?;
        counts.chain_rewards += 1;
    }

    Ok(())
}

fn load_chain_reward_source(
    state_manager: &StateManager,
    tipset: &Tipset,
) -> anyhow::Result<ChainRewardSource> {
    let state_tree = state_manager.get_state_tree(tipset.parent_state())?;
    let actor = state_tree
        .get_actor(&Address::REWARD_ACTOR)?
        .context("reward actor not found")?;
    let reward_state: reward::State = state_tree.get_actor_state()?;
    let reward_actor_version = reward_actor_version(&reward_state);
    let reward_state = reward_state.into_lotus_json_value()?;

    Ok(ChainRewardSource {
        source_epoch: tipset.epoch(),
        state_root: tipset.parent_state().to_string(),
        tipset_key: format_tipset_key(tipset.key()),
        parent_tipset_key: format_tipset_key(tipset.parents()),
        actor_state_cid: actor.state.to_string(),
        actor_code_cid: actor.code.to_string(),
        actor_balance: TokenAmount::from(&actor.balance).atto().to_string(),
        actor_sequence: actor.sequence,
        network_version: state_manager
            .get_network_version(tipset.epoch())
            .to_string(),
        reward_actor_version,
        reward_state_epoch: json_i64(&reward_state, "Epoch")?,
        total_mined_reward: json_string(&reward_state, "TotalStoragePowerReward")?,
        new_reward: json_string(&reward_state, "ThisEpochReward")?,
        new_reward_smoothed_position_estimate: json_nested_string(
            &reward_state,
            "ThisEpochRewardSmoothed",
            "PositionEstimate",
        )?,
        new_reward_smoothed_velocity_estimate: json_nested_string(
            &reward_state,
            "ThisEpochRewardSmoothed",
            "VelocityEstimate",
        )?,
        new_baseline_power: json_string(&reward_state, "ThisEpochBaselinePower")?,
        effective_baseline_power: json_string(&reward_state, "EffectiveBaselinePower")?,
        effective_network_time: json_i64(&reward_state, "EffectiveNetworkTime")?,
        cum_sum_baseline: json_string(&reward_state, "CumsumBaseline")?,
        cum_sum_realized: json_string(&reward_state, "CumsumRealized")?,
        simple_total: json_string(&reward_state, "SimpleTotal")?,
        baseline_total: json_string(&reward_state, "BaselineTotal")?,
    })
}

impl ChainRewardSource {
    fn to_row(&self, height: ChainEpoch, is_null_epoch: bool) -> ChainRewardRow {
        ChainRewardRow {
            height,
            state_root: self.state_root.clone(),
            tipset_key: self.tipset_key.clone(),
            parent_tipset_key: self.parent_tipset_key.clone(),
            source_epoch: self.source_epoch,
            source_state_root: self.state_root.clone(),
            is_null_epoch,
            actor_state_cid: self.actor_state_cid.clone(),
            actor_code_cid: self.actor_code_cid.clone(),
            actor_balance: self.actor_balance.clone(),
            actor_sequence: self.actor_sequence,
            network_version: self.network_version.clone(),
            reward_actor_version: self.reward_actor_version,
            reward_state_epoch: self.reward_state_epoch,
            total_mined_reward: self.total_mined_reward.clone(),
            new_reward: self.new_reward.clone(),
            new_reward_smoothed_position_estimate: self
                .new_reward_smoothed_position_estimate
                .clone(),
            new_reward_smoothed_velocity_estimate: self
                .new_reward_smoothed_velocity_estimate
                .clone(),
            new_baseline_power: self.new_baseline_power.clone(),
            effective_baseline_power: self.effective_baseline_power.clone(),
            effective_network_time: self.effective_network_time,
            cum_sum_baseline: self.cum_sum_baseline.clone(),
            cum_sum_realized: self.cum_sum_realized.clone(),
            simple_total: self.simple_total.clone(),
            baseline_total: self.baseline_total.clone(),
            per_epoch_mined_reward: None,
            per_epoch_effective_network_time_delta: None,
            per_epoch_cum_sum_baseline_delta: None,
            per_epoch_cum_sum_realized_delta: None,
        }
    }
}

fn add_chain_reward_deltas(rows: &mut [ChainRewardRow]) -> anyhow::Result<()> {
    for index in 0..rows.len().saturating_sub(1) {
        let Some(slice) = rows.get_mut(index..) else {
            anyhow::bail!("failed to read chain reward rows from index {index}");
        };
        let Some((current, rest)) = slice.split_first_mut() else {
            anyhow::bail!("failed to read current chain reward row at index {index}");
        };
        let Some(previous_height) = rest.first() else {
            anyhow::bail!("failed to read previous chain reward row after index {index}");
        };
        current.per_epoch_mined_reward = Some(decimal_delta(
            &current.total_mined_reward,
            &previous_height.total_mined_reward,
        )?);
        current.per_epoch_effective_network_time_delta =
            Some(current.effective_network_time - previous_height.effective_network_time);
        current.per_epoch_cum_sum_baseline_delta = Some(decimal_delta(
            &current.cum_sum_baseline,
            &previous_height.cum_sum_baseline,
        )?);
        current.per_epoch_cum_sum_realized_delta = Some(decimal_delta(
            &current.cum_sum_realized,
            &previous_height.cum_sum_realized,
        )?);
    }
    Ok(())
}

fn reward_actor_version(state: &reward::State) -> &'static str {
    match state {
        reward::State::V8(_) => "V8",
        reward::State::V9(_) => "V9",
        reward::State::V10(_) => "V10",
        reward::State::V11(_) => "V11",
        reward::State::V12(_) => "V12",
        reward::State::V13(_) => "V13",
        reward::State::V14(_) => "V14",
        reward::State::V15(_) => "V15",
        reward::State::V16(_) => "V16",
        reward::State::V17(_) => "V17",
        reward::State::V18(_) => "V18",
    }
}

fn json_string(value: &serde_json::Value, field: &str) -> anyhow::Result<String> {
    match value
        .get(field)
        .with_context(|| format!("missing {field}"))?
    {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        other => bail!("{field} must be a string or number, got {other}"),
    }
}

fn json_nested_string(
    value: &serde_json::Value,
    parent: &str,
    field: &str,
) -> anyhow::Result<String> {
    json_string(
        value
            .get(parent)
            .with_context(|| format!("missing {parent}"))?,
        field,
    )
}

fn json_i64(value: &serde_json::Value, field: &str) -> anyhow::Result<i64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .with_context(|| format!("{field} must be an i64"))
}

fn decimal_delta(current: &str, previous: &str) -> anyhow::Result<String> {
    Ok((BigInt::from_str(current)? - BigInt::from_str(previous)?).to_string())
}

fn create_jsonl(out: &Path, name: &str) -> anyhow::Result<BufWriter<File>> {
    let path = out.join(format!("{name}.jsonl"));
    Ok(BufWriter::new(
        File::create(&path).with_context(|| format!("creating {}", path.display()))?,
    ))
}

fn write_json_line<T: Serialize>(writer: &mut BufWriter<File>, row: &T) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *writer, row)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_manifest(out: &Path, manifest: Manifest) -> anyhow::Result<()> {
    let path = out.join("manifest.json");
    let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
    serde_json::to_writer_pretty(BufWriter::new(file), &manifest)?;
    Ok(())
}

fn format_tipset_key(key: &TipsetKey) -> String {
    key.to_cids()
        .into_iter()
        .map(|cid| cid.to_string())
        .collect::<Vec<_>>()
        .join(",")
}
