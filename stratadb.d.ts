/**
 * TypeScript definitions for StrataDB Node.js SDK
 *
 * All data methods are async and return Promises. Factory methods
 * (Strata.open, Strata.cache) remain synchronous.
 */

// =========================================================================
// Error classes
// =========================================================================

/** Base error for all StrataDB errors. */
export class StrataError extends Error {
  /** Machine-readable error category. */
  code: string;
}
export class NotFoundError extends StrataError {}
export class ValidationError extends StrataError {}
export class ConflictError extends StrataError {}
export class StateError extends StrataError {}
export class ConstraintError extends StrataError {}
export class AccessDeniedError extends StrataError {}
export class IoError extends StrataError {}

// =========================================================================
// Value types
// =========================================================================

/** JSON-compatible value type */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

/** Versioned value returned by history operations */
export interface VersionedValue {
  value: JsonValue;
  version: number;
  timestamp: number;
}

/** JSON list result with pagination cursor */
export interface JsonListResult {
  keys: string[];
  cursor?: string;
}

/** Vector collection information */
export interface CollectionInfo {
  name: string;
  dimension: number;
  metric: string;
  count: number;
  indexType: string;
  memoryBytes: number;
}

/** Vector data with metadata */
export interface VectorData {
  key: string;
  embedding: number[];
  metadata?: JsonValue;
  version: number;
  timestamp: number;
}

/** Vector search result */
export interface SearchMatch {
  key: string;
  score: number;
  metadata?: JsonValue;
}

/** Fork operation result */
export interface ForkResult {
  source: string;
  destination: string;
  keysCopied: number;
}

/** Branch diff summary */
export interface DiffSummary {
  totalAdded: number;
  totalRemoved: number;
  totalModified: number;
}

/** Branch diff result */
export interface DiffResult {
  branchA: string;
  branchB: string;
  summary: DiffSummary;
}

/** Merge conflict */
export interface MergeConflict {
  key: string;
  space: string;
}

/** Merge operation result */
export interface MergeResult {
  keysApplied: number;
  spacesMerged: number;
  conflicts: MergeConflict[];
}

/** Database information */
export interface DatabaseInfo {
  version: string;
  uptimeSecs: number;
  branchCount: number;
  totalKeys: number;
}

/** Branch metadata with version info */
export interface BranchInfo {
  id: string;
  status: string;
  createdAt: number;
  updatedAt: number;
  parentId?: string;
  version: number;
  timestamp: number;
}

/** Branch export result */
export interface BranchExportResult {
  branchId: string;
  path: string;
  entryCount: number;
  bundleSize: number;
}

/** Branch import result */
export interface BranchImportResult {
  branchId: string;
  transactionsApplied: number;
  keysWritten: number;
}

/** Bundle validation result */
export interface BundleValidateResult {
  branchId: string;
  formatVersion: number;
  entryCount: number;
  checksumsValid: boolean;
}

/** Vector entry for batch upsert */
export interface BatchVectorEntry {
  key: string;
  vector: number[];
  metadata?: JsonValue;
}

/** Transaction info */
export interface TransactionInfo {
  id: string;
  status: string;
  startedAt: number;
}

/** Metadata filter for vector search */
export interface MetadataFilter {
  field: string;
  op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'contains';
  value: JsonValue;
}

/** KV list result with pagination */
export interface KvListResult {
  keys: string[];
}

/** Cross-primitive search result */
export interface SearchHit {
  entity: string;
  primitive: string;
  score: number;
  rank: number;
  snippet?: string;
}

/** Time range filter for search (ISO 8601 datetime strings) */
export interface SearchTimeRange {
  /** Range start (inclusive), e.g. "2026-02-07T00:00:00Z" */
  start: string;
  /** Range end (inclusive), e.g. "2026-02-09T23:59:59Z" */
  end: string;
}

/** Options for cross-primitive search */
export interface SearchOptions {
  /** Number of results to return (default: 10). */
  k?: number;
  /** Restrict to specific primitives (e.g. ["kv", "json", "event"]). */
  primitives?: string[];
  /** Time range filter (ISO 8601 datetime strings). */
  timeRange?: SearchTimeRange;
  /** Search mode: "keyword" or "hybrid" (default: "hybrid"). */
  mode?: string;
  /** Enable/disable query expansion. Absent = auto (use if model configured). */
  expand?: boolean;
  /** Enable/disable reranking. Absent = auto (use if model configured). */
  rerank?: boolean;
}

/** Time range for a branch */
export interface TimeRange {
  oldestTs: number | null;
  latestTs: number | null;
}

/** Options for opening a database */
export interface OpenOptions {
  /** Enable automatic text embedding for semantic search. */
  autoEmbed?: boolean;
  /** Open in read-only mode. */
  readOnly?: boolean;
  /**
   * Open as a read-only follower of an existing primary instance.
   * Followers do not acquire any file lock and can open a database
   * that is already exclusively locked by another process.
   * Call `refresh()` to see new commits from the primary.
   */
  follower?: boolean;
}

/** Database configuration snapshot */
export interface StrataConfig {
  durability: string;
  autoEmbed: boolean;
  model: ModelConfig | null;
}

/** Model configuration for query expansion and reranking */
export interface ModelConfig {
  endpoint: string;
  model: string;
  apiKey: string | null;
  timeoutMs: number;
}

// =========================================================================
// Options types for the new namespace API
// =========================================================================

/** Options for KV get */
export interface KvGetOptions {
  asOf?: number;
}

/** Options for KV keys listing */
export interface KvKeysOptions {
  prefix?: string;
  limit?: number;
  asOf?: number;
}

/** Options for state get */
export interface StateGetOptions {
  asOf?: number;
}

/** Options for state CAS */
export interface StateCasOptions {
  expectedVersion?: number;
}

/** Options for state keys listing */
export interface StateKeysOptions {
  prefix?: string;
  asOf?: number;
}

/** Options for event get */
export interface EventGetOptions {
  asOf?: number;
}

/** Options for event listing */
export interface EventListOptions {
  limit?: number;
  after?: number;
  asOf?: number;
}

/** Options for JSON get */
export interface JsonGetOptions {
  asOf?: number;
}

/** Options for JSON keys listing */
export interface JsonKeysOptions {
  limit?: number;
  prefix?: string;
  cursor?: string;
  asOf?: number;
}

/** Options for vector collection creation */
export interface VectorCreateCollectionOptions {
  dimension: number;
  metric?: string;
}

/** Options for vector upsert */
export interface VectorUpsertOptions {
  metadata?: JsonValue;
}

/** Options for vector get */
export interface VectorGetOptions {
  asOf?: number;
}

/** Options for vector search */
export interface VectorSearchOptions {
  limit?: number;
  metric?: string;
  filter?: MetadataFilter[];
  asOf?: number;
}

/** Options for branch merge */
export interface BranchMergeOptions {
  strategy?: string;
}

/** Options for space delete */
export interface SpaceDeleteOptions {
  force?: boolean;
}

/** Options for transaction callback */
export interface TransactionOptions {
  readOnly?: boolean;
}

// =========================================================================
// Batch result types
// =========================================================================

/** Result for a single item in a batch write operation */
export interface BatchResult {
  version: number | null;
  error: string | null;
}

/** Result for a single item in a batch get operation */
export interface BatchGetResult {
  value: JsonValue;
  version: number | null;
  timestamp: number | null;
  error: string | null;
}

// =========================================================================
// Inference types
// =========================================================================

/** Options for text generation */
export interface GenerateOptions {
  maxTokens?: number;
  temperature?: number;
  topK?: number;
  topP?: number;
  seed?: number;
  stopTokens?: number[];
  stopSequences?: string[];
}

/** Options for tokenization */
export interface TokenizeOptions {
  addSpecialTokens?: boolean;
}

/** Text generation result */
export interface GenerateResult {
  text: string;
  stopReason: string;
  promptTokens: number;
  completionTokens: number;
  model: string;
}

/** Tokenization result */
export interface TokenizeResult {
  ids: number[];
  count: number;
  model: string;
}

// =========================================================================
// Model types
// =========================================================================

/** Model information */
export interface ModelInfo {
  name: string;
  task: string;
  architecture: string;
  defaultQuant: string;
  embeddingDim: number;
  isLocal: boolean;
  sizeBytes: number;
}

/** Model pull/download result */
export interface ModelPullResult {
  name: string;
  path: string;
}

// =========================================================================
// Embedding status
// =========================================================================

/** Embedding pipeline status */
export interface EmbedStatus {
  autoEmbed: boolean;
  batchSize: number;
  pending: number;
  totalQueued: number;
  totalEmbedded: number;
  totalFailed: number;
  schedulerQueueDepth: number;
  schedulerActiveTasks: number;
}

// =========================================================================
// Durability types
// =========================================================================

/** WAL durability counters */
export interface DurabilityCounters {
  walAppends: number;
  syncCalls: number;
  bytesWritten: number;
  syncNanos: number;
}

// =========================================================================
// Batch entry types
// =========================================================================

/** Entry for batch KV put */
export interface BatchKvEntry {
  key: string;
  value: JsonValue;
}

/** Entry for batch state set */
export interface BatchStateEntry {
  cell: string;
  value: JsonValue;
}

/** Entry for batch event append */
export interface BatchEventEntry {
  /** Accepts both `eventType` (preferred) and `event_type`. */
  eventType: string;
  payload: JsonValue;
}

/** Entry for batch JSON set */
export interface BatchJsonEntry {
  key: string;
  path: string;
  value: JsonValue;
}

/** Entry for batch JSON get */
export interface BatchJsonGetEntry {
  key: string;
  path: string;
}

/** Entry for batch JSON delete */
export interface BatchJsonDeleteEntry {
  key: string;
  path: string;
}

// =========================================================================
// Graph types
// =========================================================================

/** Graph neighbor query result */
export interface GraphNeighborHit {
  nodeId: string;
  edgeType: string;
  weight: number;
}

/** BFS traversal result */
export interface GraphBfsResult {
  visited: string[];
  depths: Record<string, number>;
  edges: Array<{ src: string; dst: string; edgeType: string }>;
}

/** Graph bulk insert result */
export interface GraphBulkInsertResult {
  nodesInserted: number;
  edgesInserted: number;
}

/** Graph analytics result with integer values (WCC, CDLP) */
export interface GraphAnalyticsU64Result {
  algorithm: string;
  result: Record<string, number>;
}

/** Graph analytics result with float values (PageRank, LCC, SSSP) */
export interface GraphAnalyticsF64Result {
  algorithm: string;
  result: Record<string, number>;
  iterations: number | null;
}

/** Graph paginated node list result */
export interface GraphPage {
  items: string[];
  nextCursor: string | null;
}

/** Node for bulk graph insert */
export interface BulkGraphNode {
  nodeId: string;
  entityRef?: string;
  properties?: JsonValue;
  objectType?: string;
}

/** Edge for bulk graph insert */
export interface BulkGraphEdge {
  src: string;
  dst: string;
  edgeType: string;
  weight?: number;
  properties?: JsonValue;
}

/** Options for graph creation */
export interface GraphCreateOptions {
  cascadePolicy?: string;
}

/** Options for adding a node */
export interface GraphAddNodeOptions {
  entityRef?: string;
  properties?: JsonValue;
  objectType?: string;
}

/** Options for listing nodes with pagination */
export interface GraphListNodesOptions {
  limit?: number;
  cursor?: string;
}

/** Options for adding an edge */
export interface GraphAddEdgeOptions {
  weight?: number;
  properties?: JsonValue;
}

/** Options for neighbor queries */
export interface GraphNeighborOptions {
  direction?: string;
  edgeType?: string;
}

/** Data for bulk insert */
export interface GraphBulkInsertData {
  nodes?: BulkGraphNode[];
  edges?: BulkGraphEdge[];
}

/** Options for bulk insert */
export interface GraphBulkInsertOptions {
  chunkSize?: number;
}

/** Options for BFS traversal */
export interface GraphBfsOptions {
  maxNodes?: number;
  edgeTypes?: string[];
  direction?: string;
}

/** Options for CDLP */
export interface GraphCdlpOptions {
  direction?: string;
}

/** Options for PageRank */
export interface GraphPagerankOptions {
  damping?: number;
  maxIterations?: number;
  tolerance?: number;
}

/** Options for SSSP */
export interface GraphSsspOptions {
  direction?: string;
}

/** Options for branch creation */
export interface BranchCreateOptions {
  metadata?: JsonValue;
}

/** Options for branch listing */
export interface BranchListOptions {
  limit?: number;
  offset?: number;
}

// =========================================================================
// Namespace interfaces
// =========================================================================

/** KV Store namespace — accessed via `db.kv` */
export interface KvNamespace {
  set(key: string, value: JsonValue): Promise<number>;
  get(key: string, opts?: KvGetOptions): Promise<JsonValue>;
  delete(key: string): Promise<boolean>;
  keys(opts?: KvKeysOptions): Promise<string[]>;
  history(key: string): Promise<VersionedValue[] | null>;
  getVersioned(key: string): Promise<VersionedValue | null>;
  batchPut(entries: BatchKvEntry[]): Promise<BatchResult[]>;
}

/** State Cell namespace — accessed via `db.state` */
export interface StateNamespace {
  set(cell: string, value: JsonValue): Promise<number>;
  get(cell: string, opts?: StateGetOptions): Promise<JsonValue>;
  init(cell: string, value: JsonValue): Promise<number>;
  cas(cell: string, newValue: JsonValue, opts?: StateCasOptions): Promise<number | null>;
  delete(cell: string): Promise<boolean>;
  keys(opts?: StateKeysOptions): Promise<string[]>;
  history(cell: string): Promise<VersionedValue[] | null>;
  getVersioned(cell: string): Promise<VersionedValue | null>;
  batchSet(entries: BatchStateEntry[]): Promise<BatchResult[]>;
}

/** Event Log namespace — accessed via `db.events` */
export interface EventsNamespace {
  append(eventType: string, payload: JsonValue): Promise<number>;
  get(sequence: number, opts?: EventGetOptions): Promise<VersionedValue | null>;
  list(eventType: string, opts?: EventListOptions): Promise<VersionedValue[]>;
  count(): Promise<number>;
  batchAppend(entries: BatchEventEntry[]): Promise<BatchResult[]>;
}

/** JSON Document namespace — accessed via `db.json` */
export interface JsonNamespace {
  set(key: string, path: string, value: JsonValue): Promise<number>;
  get(key: string, path: string, opts?: JsonGetOptions): Promise<JsonValue>;
  delete(key: string, path: string): Promise<number>;
  keys(opts?: JsonKeysOptions): Promise<JsonListResult>;
  history(key: string): Promise<VersionedValue[] | null>;
  getVersioned(key: string): Promise<VersionedValue | null>;
  batchSet(entries: BatchJsonEntry[]): Promise<BatchResult[]>;
  batchGet(entries: BatchJsonGetEntry[]): Promise<BatchGetResult[]>;
  batchDelete(entries: BatchJsonDeleteEntry[]): Promise<BatchResult[]>;
}

/** Vector Store namespace — accessed via `db.vector` */
export interface VectorNamespace {
  createCollection(name: string, opts: VectorCreateCollectionOptions): Promise<number>;
  deleteCollection(name: string): Promise<boolean>;
  listCollections(): Promise<CollectionInfo[]>;
  stats(collection: string): Promise<CollectionInfo>;
  upsert(collection: string, key: string, vector: number[], opts?: VectorUpsertOptions): Promise<number>;
  get(collection: string, key: string, opts?: VectorGetOptions): Promise<VectorData | null>;
  delete(collection: string, key: string): Promise<boolean>;
  batchUpsert(collection: string, entries: BatchVectorEntry[]): Promise<number[]>;
  search(collection: string, query: number[], opts?: VectorSearchOptions): Promise<SearchMatch[]>;
}

/** Branch Management namespace — accessed via `db.branch` */
export interface BranchNamespace {
  current(): Promise<string>;
  switch(name: string): Promise<void>;
  create(name: string, opts?: BranchCreateOptions): Promise<void>;
  fork(destination: string): Promise<ForkResult>;
  list(opts?: BranchListOptions): Promise<string[]>;
  delete(name: string): Promise<void>;
  exists(name: string): Promise<boolean>;
  get(name: string): Promise<BranchInfo | null>;
  diff(branchA: string, branchB: string): Promise<DiffResult>;
  merge(source: string, opts?: BranchMergeOptions): Promise<MergeResult>;
  export(branch: string, path: string): Promise<BranchExportResult>;
  import(path: string): Promise<BranchImportResult>;
  validateBundle(path: string): Promise<BundleValidateResult>;
}

/** Space Management namespace — accessed via `db.space` */
export interface SpaceNamespace {
  current(): Promise<string>;
  switch(name: string): Promise<void>;
  create(name: string): Promise<void>;
  list(): Promise<string[]>;
  delete(name: string, opts?: SpaceDeleteOptions): Promise<void>;
  exists(name: string): Promise<boolean>;
}

/** Graph namespace — accessed via `db.graph` */
export interface GraphNamespace {
  // Lifecycle
  create(name: string, opts?: GraphCreateOptions): Promise<void>;
  delete(name: string): Promise<void>;
  list(): Promise<string[]>;
  info(name: string): Promise<JsonValue>;

  // Nodes
  addNode(graph: string, nodeId: string, opts?: GraphAddNodeOptions): Promise<void>;
  getNode(graph: string, nodeId: string): Promise<JsonValue>;
  removeNode(graph: string, nodeId: string): Promise<void>;
  listNodes(graph: string, opts?: GraphListNodesOptions): Promise<string[] | GraphPage>;

  // Edges
  addEdge(graph: string, src: string, dst: string, edgeType: string, opts?: GraphAddEdgeOptions): Promise<void>;
  removeEdge(graph: string, src: string, dst: string, edgeType: string): Promise<void>;
  neighbors(graph: string, nodeId: string, opts?: GraphNeighborOptions): Promise<GraphNeighborHit[]>;

  // Bulk & Traversal
  bulkInsert(graph: string, data: GraphBulkInsertData, opts?: GraphBulkInsertOptions): Promise<GraphBulkInsertResult>;
  bfs(graph: string, start: string, maxDepth: number, opts?: GraphBfsOptions): Promise<GraphBfsResult>;

  // Ontology
  defineObjectType(graph: string, definition: JsonValue): Promise<void>;
  getObjectType(graph: string, name: string): Promise<JsonValue>;
  listObjectTypes(graph: string): Promise<string[]>;
  deleteObjectType(graph: string, name: string): Promise<void>;
  defineLinkType(graph: string, definition: JsonValue): Promise<void>;
  getLinkType(graph: string, name: string): Promise<JsonValue>;
  listLinkTypes(graph: string): Promise<string[]>;
  deleteLinkType(graph: string, name: string): Promise<void>;
  freezeOntology(graph: string): Promise<void>;
  ontologyStatus(graph: string): Promise<JsonValue>;
  ontologySummary(graph: string): Promise<JsonValue>;
  listOntologyTypes(graph: string): Promise<string[]>;
  nodesByType(graph: string, objectType: string): Promise<string[]>;

  // Analytics
  wcc(graph: string): Promise<GraphAnalyticsU64Result>;
  cdlp(graph: string, maxIterations: number, opts?: GraphCdlpOptions): Promise<GraphAnalyticsU64Result>;
  pagerank(graph: string, opts?: GraphPagerankOptions): Promise<GraphAnalyticsF64Result>;
  lcc(graph: string): Promise<GraphAnalyticsF64Result>;
  sssp(graph: string, source: string, opts?: GraphSsspOptions): Promise<GraphAnalyticsF64Result>;
}

// =========================================================================
// Read-only snapshot namespace interfaces (returned by db.at())
// =========================================================================

/** Read-only KV namespace for snapshots */
export interface KvSnapshotNamespace {
  get(key: string): Promise<JsonValue>;
  keys(opts?: Omit<KvKeysOptions, 'asOf'>): Promise<string[]>;
  history(key: string): Promise<VersionedValue[] | null>;
  getVersioned(key: string): Promise<VersionedValue | null>;
}

/** Read-only State namespace for snapshots */
export interface StateSnapshotNamespace {
  get(cell: string): Promise<JsonValue>;
  keys(opts?: Omit<StateKeysOptions, 'asOf'>): Promise<string[]>;
  history(cell: string): Promise<VersionedValue[] | null>;
  getVersioned(cell: string): Promise<VersionedValue | null>;
}

/** Read-only Events namespace for snapshots */
export interface EventsSnapshotNamespace {
  get(sequence: number): Promise<VersionedValue | null>;
  list(eventType: string, opts?: Omit<EventListOptions, 'asOf'>): Promise<VersionedValue[]>;
  count(): Promise<number>;
}

/** Read-only JSON namespace for snapshots */
export interface JsonSnapshotNamespace {
  get(key: string, path: string): Promise<JsonValue>;
  keys(opts?: Omit<JsonKeysOptions, 'asOf'>): Promise<JsonListResult>;
  history(key: string): Promise<VersionedValue[] | null>;
  getVersioned(key: string): Promise<VersionedValue | null>;
}

/** Read-only Vector namespace for snapshots */
export interface VectorSnapshotNamespace {
  listCollections(): Promise<CollectionInfo[]>;
  stats(collection: string): Promise<CollectionInfo>;
  get(collection: string, key: string): Promise<VectorData | null>;
  search(collection: string, query: number[], opts?: Omit<VectorSearchOptions, 'asOf'>): Promise<SearchMatch[]>;
}

/** Read-only Graph namespace for snapshots */
export interface GraphSnapshotNamespace {
  list(): Promise<string[]>;
  info(name: string): Promise<JsonValue>;
  getNode(graph: string, nodeId: string): Promise<JsonValue>;
  listNodes(graph: string, opts?: GraphListNodesOptions): Promise<string[] | GraphPage>;
  neighbors(graph: string, nodeId: string, opts?: GraphNeighborOptions): Promise<GraphNeighborHit[]>;
  bfs(graph: string, start: string, maxDepth: number, opts?: GraphBfsOptions): Promise<GraphBfsResult>;
  getObjectType(graph: string, name: string): Promise<JsonValue>;
  listObjectTypes(graph: string): Promise<string[]>;
  getLinkType(graph: string, name: string): Promise<JsonValue>;
  listLinkTypes(graph: string): Promise<string[]>;
  ontologyStatus(graph: string): Promise<JsonValue>;
  ontologySummary(graph: string): Promise<JsonValue>;
  listOntologyTypes(graph: string): Promise<string[]>;
  nodesByType(graph: string, objectType: string): Promise<string[]>;
  wcc(graph: string): Promise<GraphAnalyticsU64Result>;
  cdlp(graph: string, maxIterations: number, opts?: GraphCdlpOptions): Promise<GraphAnalyticsU64Result>;
  pagerank(graph: string, opts?: GraphPagerankOptions): Promise<GraphAnalyticsF64Result>;
  lcc(graph: string): Promise<GraphAnalyticsF64Result>;
  sssp(graph: string, source: string, opts?: GraphSsspOptions): Promise<GraphAnalyticsF64Result>;
}

/**
 * Immutable time-travel snapshot returned by `db.at(timestamp)`.
 * Only read operations are available; writes throw StateError.
 */
export class StrataSnapshot {
  readonly kv: KvSnapshotNamespace;
  readonly state: StateSnapshotNamespace;
  readonly events: EventsSnapshotNamespace;
  readonly json: JsonSnapshotNamespace;
  readonly vector: VectorSnapshotNamespace;
  readonly graph: GraphSnapshotNamespace;
}

// =========================================================================
// Main Strata class
// =========================================================================

/**
 * StrataDB database handle.
 *
 * All data methods are async and return Promises.
 * Factory methods (`open`, `cache`) are synchronous.
 */
export class Strata {
  // Factory methods (synchronous)
  static open(path: string, options?: OpenOptions): Strata;
  static cache(): Strata;

  // -----------------------------------------------------------------------
  // Namespace accessors (NEW — preferred API)
  // -----------------------------------------------------------------------

  /** KV Store operations */
  readonly kv: KvNamespace;
  /** State Cell operations */
  readonly state: StateNamespace;
  /** Event Log operations */
  readonly events: EventsNamespace;
  /** JSON Document operations */
  readonly json: JsonNamespace;
  /** Vector Store operations */
  readonly vector: VectorNamespace;
  /** Branch Management operations */
  readonly branch: BranchNamespace;
  /** Space Management operations */
  readonly space: SpaceNamespace;
  /** Graph operations */
  readonly graph: GraphNamespace;

  // -----------------------------------------------------------------------
  // Time travel
  // -----------------------------------------------------------------------

  /** Create an immutable snapshot at the given timestamp. */
  at(timestamp: number): StrataSnapshot;

  // -----------------------------------------------------------------------
  // Transaction callback
  // -----------------------------------------------------------------------

  /**
   * Execute a function inside a transaction with auto-commit on success
   * and auto-rollback on error.
   */
  transaction<T>(fn: (tx: Strata) => Promise<T>, opts?: TransactionOptions): Promise<T>;

  // -----------------------------------------------------------------------
  // Configuration
  // -----------------------------------------------------------------------

  /** Get the current database configuration. */
  config(): Promise<StrataConfig>;

  /** Configure an inference model endpoint for query expansion and reranking. Persisted to strata.toml. */
  configureModel(endpoint: string, model: string, apiKey?: string | null, timeoutMs?: number | null): Promise<void>;

  /** Check whether auto-embedding is enabled. */
  autoEmbedEnabled(): Promise<boolean>;

  /** Enable or disable auto-embedding. Persisted to strata.toml. */
  setAutoEmbed(enabled: boolean): Promise<void>;

  // -----------------------------------------------------------------------
  // Database Operations
  // -----------------------------------------------------------------------

  ping(): Promise<string>;
  info(): Promise<DatabaseInfo>;
  flush(): Promise<void>;
  compact(): Promise<void>;
  close(): Promise<void>;

  // Search
  search(query: string, opts?: SearchOptions): Promise<SearchHit[]>;

  // Retention
  retentionApply(): Promise<void>;

  // Follower mode
  /** Returns `true` if this database was opened in read-only follower mode. */
  isFollower(): Promise<boolean>;
  /**
   * Replay new WAL records from the primary.
   * Only meaningful for follower instances (opened with `{ follower: true }`).
   * Returns the number of new records applied.
   */
  refresh(): Promise<number>;

  // Time Travel
  timeRange(): Promise<TimeRange>;

  // Transaction Operations (manual — prefer `transaction()` callback)
  begin(readOnly?: boolean): Promise<void>;
  commit(): Promise<number>;
  rollback(): Promise<void>;
  txnInfo(): Promise<TransactionInfo | null>;
  txnIsActive(): Promise<boolean>;

  // -----------------------------------------------------------------------
  // Configuration (key-value)
  // -----------------------------------------------------------------------

  /** Set a configuration key-value pair. */
  configureSet(key: string, value: string): Promise<void>;
  /** Get a configuration value by key. */
  configureGet(key: string): Promise<string | null>;

  // -----------------------------------------------------------------------
  // Embedding
  // -----------------------------------------------------------------------

  /** Embed a single text string. Returns a float vector. */
  embed(text: string): Promise<number[]>;
  /** Embed multiple texts in a batch. Returns an array of float vectors. */
  embedBatch(texts: string[]): Promise<number[][]>;
  /** Get the embedding pipeline status. */
  embedStatus(): Promise<EmbedStatus>;

  // -----------------------------------------------------------------------
  // Inference
  // -----------------------------------------------------------------------

  /** Generate text from a model. */
  generate(model: string, prompt: string, options?: GenerateOptions): Promise<GenerateResult>;
  /** Tokenize text using a model's tokenizer. */
  tokenize(model: string, text: string, options?: TokenizeOptions): Promise<TokenizeResult>;
  /** Detokenize token IDs back to text. */
  detokenize(model: string, ids: number[]): Promise<string>;
  /** Unload a model from memory. */
  generateUnload(model: string): Promise<boolean>;

  // -----------------------------------------------------------------------
  // Model Management
  // -----------------------------------------------------------------------

  /** List all available models. */
  modelsList(): Promise<ModelInfo[]>;
  /** Pull/download a model by name. */
  modelsPull(name: string): Promise<ModelPullResult>;
  /** List locally downloaded models. */
  modelsLocal(): Promise<ModelInfo[]>;

  // -----------------------------------------------------------------------
  // Durability
  // -----------------------------------------------------------------------

  /** Get WAL durability counters. */
  durabilityCounters(): Promise<DurabilityCounters>;

}

/**
 * Download model files for auto-embedding.
 */
export function setup(): string;
