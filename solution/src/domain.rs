// Important! Do not modify any declarations in this file.
// However, you may add extra #[derive] attributes and private methods / impl blocks.
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

// You do not have to provide any implementation of this trait.
#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;
    async fn get(&self, key: &str) -> Option<Vec<u8>>;
}

// You do not have to provide any implementation of this trait.
#[async_trait::async_trait]
pub trait RaftSender: Send + Sync {
    async fn send(&self, target: &Uuid, msg: RaftMessage);
}

// You do not have to provide any implementation of this trait.
#[async_trait::async_trait]
pub trait StateMachine: Send + Sync {
    /// Initializes the state machine from a serialized state.
    async fn initialize(&mut self, state: &[u8]);
    /// Applies a command to the state machine.
    async fn apply(&mut self, command: &[u8]) -> Vec<u8>;
    /// Serializes the state machine so that it can be snapshotted.
    async fn serialize(&self) -> Vec<u8>;
}

pub struct ServerConfig {
    pub self_id: Uuid,
    /// The range from which election timeout should be randomly chosen.
    pub election_timeout_range: RangeInclusive<Duration>,
    /// Periodic heartbeat interval.
    pub heartbeat_timeout: Duration,
    /// Initial cluster configuration.
    pub servers: HashSet<Uuid>,
    /// Maximum number of log entries that can be sent in one AppendEntries message.
    pub append_entries_batch_size: usize,
    /// Maximum number of snapshot bytes that can be sent in one InstallSnapshot message.
    pub snapshot_chunk_size: usize,
    /// Number of catch up round when adding a server to the cluster.
    pub catch_up_rounds: u64,
    /// The duration since last activity after which a client session should be expired.
    pub session_expiration: Duration,
}

pub struct ClientRequest {
    pub reply_to: UnboundedSender<ClientRequestResponse>,
    pub content: ClientRequestContent,
}

pub enum ClientRequestContent {
    /// Apply a command to the state machine.
    Command {
        command: Vec<u8>,
        client_id: Uuid, // client session USE
        sequence_num: u64, // client session USE
        lowest_sequence_num_without_response: u64, // client session
    },
    /// Create a snapshot of the current state of the state machine.
    Snapshot,
    /// Add a server to the cluster.
    AddServer { new_server: Uuid }, // cluster change
    /// Remove a server from the cluster.
    RemoveServer { old_server: Uuid }, // cluster change
    /// Open a new client session.
    RegisterClient, // client session
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientRequestResponse {
    CommandResponse(CommandResponseArgs),
    SnapshotResponse(SnapshotResponseArgs),
    AddServerResponse(AddServerResponseArgs),
    RemoveServerResponse(RemoveServerResponseArgs),
    RegisterClientResponse(RegisterClientResponseArgs),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandResponseArgs {
    pub client_id: Uuid,
    pub sequence_num: u64,
    pub content: CommandResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandResponseContent {
    /// The command has been applied to the state machine.
    CommandApplied { output: Vec<u8> },
    /// The server that received the request is not a leader.
    NotLeader { leader_hint: Option<Uuid> },
    /// The client session for the client specified in the request has expired.
    SessionExpired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotResponseArgs {
    pub content: SnapshotResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotResponseContent {
    /// A new snapshot has been created.
    SnapshotCreated { last_included_index: usize },
    /// No new entries were committed since the last snapshot.
    NothingToSnapshot { last_included_index: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddServerResponseArgs {
    pub new_server: Uuid,
    pub content: AddServerResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddServerResponseContent {
    /// The server has been added to the cluster.
    ServerAdded,
    /// The server that received the request is not a leader.
    NotLeader { leader_hint: Option<Uuid> },
    /// Another cluster change is in progress, that is, the leader has received
    /// a change request in the current term, but has not yet committed the corresponding
    /// log entry.
    ChangeInProgress,
    /// The server specified in the request is already in the cluster.
    AlreadyPresent,
    /// The added server timed out in the catch up phase.
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoveServerResponseArgs {
    pub old_server: Uuid,
    pub content: RemoveServerResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoveServerResponseContent {
    /// The server has been removed from the cluster.
    ServerRemoved,
    /// The server that received the request is not a leader.
    NotLeader { leader_hint: Option<Uuid> },
    /// Another cluster change is in progress, that is, the leader has received
    /// a change request in the current term, but has not yet committed the corresponding
    /// log entry.
    ChangeInProgress,
    /// The server specified in the request is already not in the cluster.
    NotPresent,
    /// There is only one server left in the cluster, so it cannot be removed.
    OneServerLeft,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterClientResponseArgs {
    pub content: RegisterClientResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterClientResponseContent {
    /// The client has been registered.
    ClientRegistered { client_id: Uuid },
    /// The server that received the request is not a leader.
    NotLeader { leader_hint: Option<Uuid> },
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RaftMessage {
    pub header: RaftMessageHeader,
    pub content: RaftMessageContent,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RaftMessageHeader {
    pub source: Uuid,
    pub term: u64,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum RaftMessageContent {
    AppendEntries(AppendEntriesArgs),
    AppendEntriesResponse(AppendEntriesResponseArgs),
    RequestVote(RequestVoteArgs),
    RequestVoteResponse(RequestVoteResponseArgs),
    InstallSnapshot(InstallSnapshotArgs),
    InstallSnapshotResponse(InstallSnapshotResponseArgs),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct AppendEntriesArgs {
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: usize,
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub content: LogEntryContent,
    pub term: u64,
    pub timestamp: SystemTime,
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum LogEntryContent {
    /// A no-op entry committed by a leader after it is elected.
    NoOp,
    Command {
        data: Vec<u8>,
        client_id: Uuid,
        sequence_num: u64,
        /// The lowest sequence number for which the client has not yet received
        /// a response (see Chapter 6.3 in the paper).
        lowest_sequence_num_without_response: u64,
    },
    Configuration {
        servers: HashSet<Uuid>,
    },
    RegisterClient,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct AppendEntriesResponseArgs {
    pub success: bool,
    /// The last log index that appears in the corresponding `AppendEntries` message, used
    /// for updating nextIndex and matchIndex in case of success. Equal to
    /// `prev_log_index + entries.len()` from the `AppendEntries` message.
    /// 
    
    /*
    If term is not correct - success: false, send your header, header includes term and leader knows it was term issue; leader updates its term
    If there is not such a matching entry - success: false, we know from the header that the term is correct, it must be matching issue (the term might be smaller, then we have to add new operations)

    we decrement nextIndex till we find the last matching
    we find matching - there is success
    we know from last_verified log index the last applied, log, therefore we know how many batches or logs in a single batch we have to send (if we have to send logs within a batch or multiple batches), since we wouldn't know otherwise, whether there is a match on the entry find from decremental iteration or normal operation

    If we manage to find matching index through decrementing nextIdx, we receive matchIdx + 1 = nextIdx, then we can send logs as task description intended
     */
    pub last_verified_log_index: usize,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RequestVoteArgs {
    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RequestVoteResponseArgs {
    pub vote_granted: bool,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct InstallSnapshotArgs {
    pub last_included_index: usize,
    pub last_included_term: u64,
    pub last_config: Option<HashSet<Uuid>>,
    pub client_sessions: Option<HashMap<Uuid, ClientSession>>,
    pub offset: usize,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientSession {
    pub last_activity: SystemTime,
    pub responses: HashMap<u64, Vec<u8>>,
    pub lowest_sequence_num_without_response: u64,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct InstallSnapshotResponseArgs {
    /// `last_included_index` from the corresponding `InstallSnapshot` message.
    pub last_included_index: usize,
    /// `offset` from the corresponding `InstallSnapshot` message.
    pub offset: usize,
}

// State of a Raft process with a corresponding (volatile) information.
#[derive(Default, PartialEq, Debug)]
pub enum ProcessType {
    #[default]
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader,
}

/// State of a Raft process.
/// It shall be kept in stable storage, and updated before replying to messages.
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct PersistentState {
    pub(crate) current_term: u64,
    /// Identifier of a process which has received this process' vote.
    /// `None if this process has not voted in this term.
    pub(crate) voted_for: Option<Uuid>,
    pub(crate) log: Vec<LogEntry>,
}

#[derive (Clone)]
pub struct ElectionTimeout;

#[derive (Clone)]
pub struct HeartbeatTick;

#[derive (Clone)]
pub struct Init;