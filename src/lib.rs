//! Node.js bindings for StrataDB.
//!
//! This module exposes the StrataDB API to Node.js via NAPI-RS.
//! All data methods are async (backed by `spawn_blocking`) so they never
//! block the Node.js event loop.

#![deny(clippy::all)]

use napi_derive::napi;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use stratadb::{
    AccessMode, BatchEventEntry, BatchGetItemResult, BatchItemResult, BatchJsonDeleteEntry,
    BatchJsonEntry, BatchJsonGetEntry, BatchKvEntry, BatchStateEntry, BatchVectorEntry,
    BranchExportResult, BranchId, BranchImportResult, BulkGraphEdge, BulkGraphNode,
    BundleValidateResult, CollectionInfo, Command, DescribeResult, DistanceMetric,
    Error as StrataError, FilterOp, GraphAnalyticsF64Result, GraphAnalyticsU64Result,
    GraphBfsResult, MergeStrategy, MetadataFilter, OpenOptions, Output, SearchQuery, Session,
    Strata as RustStrata, TimeRangeInput, TxnOptions, Value, VersionedBranchInfo, VersionedValue,
};

/// Maximum nesting depth for JSON → Value conversion.
const MAX_JSON_DEPTH: usize = 64;

/// Options for opening a database.
#[napi(object)]
pub struct JsOpenOptions {
    /// Enable automatic text embedding for semantic search.
    pub auto_embed: Option<bool>,
    /// Open in read-only mode.
    pub read_only: Option<bool>,
    /// Open as a read-only follower of an existing primary instance.
    ///
    /// Followers do not acquire any file lock and can open a database
    /// that is already exclusively locked by another process. All write
    /// operations are rejected. Call `refresh()` to see new commits
    /// from the primary.
    pub follower: Option<bool>,
}

/// Time range filter for search (ISO 8601 datetime strings).
#[napi(object)]
pub struct JsTimeRange {
    /// Range start (inclusive), e.g. "2026-02-07T00:00:00Z".
    pub start: String,
    /// Range end (inclusive), e.g. "2026-02-09T23:59:59Z".
    pub end: String,
}

/// Options for cross-primitive search.
#[napi(object)]
pub struct JsSearchOptions {
    /// Number of results to return (default: 10).
    pub k: Option<u32>,
    /// Restrict to specific primitives (e.g. ["kv", "json", "event"]).
    pub primitives: Option<Vec<String>>,
    /// Time range filter (ISO 8601 datetime strings).
    pub time_range: Option<JsTimeRange>,
    /// Search mode: "keyword" or "hybrid" (default: "hybrid").
    pub mode: Option<String>,
    /// Enable/disable query expansion. Absent = auto.
    pub expand: Option<bool>,
    /// Enable/disable reranking. Absent = auto.
    pub rerank: Option<bool>,
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

/// Convert a JavaScript value to a stratadb Value with depth checking.
fn js_to_value_checked(val: serde_json::Value, depth: usize) -> napi::Result<Value> {
    if depth > MAX_JSON_DEPTH {
        return Err(napi::Error::from_reason(
            "[VALIDATION] JSON nesting depth exceeds maximum of 64",
        ));
    }
    match val {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Ok(Value::Null)
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s)),
        serde_json::Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(js_to_value_checked(item, depth + 1)?);
            }
            Ok(Value::Array(Box::new(out)))
        }
        serde_json::Value::Object(map) => {
            let mut obj = HashMap::new();
            for (k, v) in map {
                obj.insert(k, js_to_value_checked(v, depth + 1)?);
            }
            Ok(Value::Object(Box::new(obj)))
        }
    }
}

/// Validate a vector, rejecting NaN/Infinity, and convert f64 → f32.
fn validate_vector(vec: &[f64]) -> napi::Result<Vec<f32>> {
    let mut out = Vec::with_capacity(vec.len());
    for (i, &f) in vec.iter().enumerate() {
        if f.is_nan() || f.is_infinite() {
            return Err(napi::Error::from_reason(format!(
                "[VALIDATION] Vector element at index {} is not a finite number",
                i
            )));
        }
        out.push(f as f32);
    }
    Ok(out)
}

/// Convert a stratadb Value to a serde_json Value.
fn value_to_js(val: Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Int(i) => serde_json::Value::Number(i.into()),
        Value::Float(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s),
        Value::Bytes(b) => serde_json::Value::String(base64_encode(&b)),
        Value::Array(arr) => {
            serde_json::Value::Array((*arr).into_iter().map(value_to_js).collect())
        }
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = (*map)
                .into_iter()
                .map(|(k, v)| (k, value_to_js(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

/// Simple base64 encoding for bytes.
fn base64_encode(data: &[u8]) -> String {
    use std::io::Write;
    let mut buf = Vec::new();
    let mut encoder = base64_encoder(&mut buf);
    encoder.write_all(data).unwrap();
    drop(encoder);
    String::from_utf8(buf).unwrap()
}

fn base64_encoder(writer: &mut Vec<u8>) -> impl std::io::Write + '_ {
    struct Base64Writer<'a>(&'a mut Vec<u8>);
    impl<'a> std::io::Write for Base64Writer<'a> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            const ALPHABET: &[u8] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            for chunk in buf.chunks(3) {
                let b0 = chunk[0] as usize;
                let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
                let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
                self.0.push(ALPHABET[b0 >> 2]);
                self.0.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)]);
                if chunk.len() > 1 {
                    self.0.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)]);
                } else {
                    self.0.push(b'=');
                }
                if chunk.len() > 2 {
                    self.0.push(ALPHABET[b2 & 0x3f]);
                } else {
                    self.0.push(b'=');
                }
            }
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    Base64Writer(writer)
}

/// Convert a VersionedValue to a JSON object.
fn versioned_to_js(vv: VersionedValue) -> serde_json::Value {
    serde_json::json!({
        "value": value_to_js(vv.value),
        "version": vv.version,
        "timestamp": vv.timestamp,
    })
}

/// Convert a DescribeResult to camelCase JSON for JS consumers.
fn describe_to_js(desc: DescribeResult) -> serde_json::Value {
    serde_json::json!({
        "version": desc.version,
        "path": desc.path,
        "branch": desc.branch,
        "branches": desc.branches,
        "spaces": desc.spaces,
        "follower": desc.follower,
        "primitives": {
            "kv": { "count": desc.primitives.kv.count },
            "json": { "count": desc.primitives.json.count },
            "events": { "count": desc.primitives.events.count },
            "state": {
                "count": desc.primitives.state.count,
                "cells": desc.primitives.state.cells,
            },
            "vector": {
                "collections": desc.primitives.vector.collections.iter().map(|c| {
                    serde_json::json!({
                        "name": c.name,
                        "dimension": c.dimension,
                        "metric": serde_json::to_value(c.metric).unwrap_or_default(),
                        "count": c.count,
                    })
                }).collect::<Vec<_>>(),
            },
            "graph": {
                "graphs": desc.primitives.graph.graphs.iter().map(|g| {
                    serde_json::json!({
                        "name": g.name,
                        "nodes": g.nodes,
                        "edges": g.edges,
                        "objectTypes": g.object_types,
                        "linkTypes": g.link_types,
                    })
                }).collect::<Vec<_>>(),
            },
        },
        "config": {
            "provider": desc.config.provider,
            "defaultModel": desc.config.default_model,
            "autoEmbed": desc.config.auto_embed,
            "embedModel": desc.config.embed_model,
            "durability": desc.config.durability,
        },
        "capabilities": {
            "search": desc.capabilities.search,
            "vectorSearch": desc.capabilities.vector_search,
            "generation": desc.capabilities.generation,
            "autoEmbed": desc.capabilities.auto_embed,
        },
    })
}

/// Convert stratadb error to napi Error with category prefix.
fn to_napi_err(e: StrataError) -> napi::Error {
    let code = match &e {
        StrataError::KeyNotFound { .. }
        | StrataError::BranchNotFound { .. }
        | StrataError::CollectionNotFound { .. }
        | StrataError::StreamNotFound { .. }
        | StrataError::CellNotFound { .. }
        | StrataError::DocumentNotFound { .. }
        | StrataError::GraphNotFound { .. } => "[NOT_FOUND]",

        StrataError::InvalidKey { .. }
        | StrataError::InvalidPath { .. }
        | StrataError::InvalidInput { .. }
        | StrataError::WrongType { .. } => "[VALIDATION]",

        StrataError::VersionConflict { .. }
        | StrataError::TransitionFailed { .. }
        | StrataError::Conflict { .. }
        | StrataError::TransactionConflict { .. } => "[CONFLICT]",

        StrataError::BranchClosed { .. }
        | StrataError::BranchExists { .. }
        | StrataError::CollectionExists { .. }
        | StrataError::TransactionNotActive
        | StrataError::TransactionAlreadyActive => "[STATE]",

        StrataError::DimensionMismatch { .. }
        | StrataError::ConstraintViolation { .. }
        | StrataError::HistoryTrimmed { .. }
        | StrataError::HistoryUnavailable { .. }
        | StrataError::Overflow { .. } => "[CONSTRAINT]",

        StrataError::AccessDenied { .. } => "[ACCESS_DENIED]",

        StrataError::Io { .. }
        | StrataError::Serialization { .. }
        | StrataError::Internal { .. }
        | StrataError::NotImplemented { .. } => "[IO]",
    };
    napi::Error::from_reason(format!("{} {}", code, e))
}

/// Helper to acquire the mutex lock, mapping poison errors.
fn lock_inner(
    inner: &Mutex<RustStrata>,
) -> napi::Result<std::sync::MutexGuard<'_, RustStrata>> {
    inner
        .lock()
        .map_err(|_| napi::Error::from_reason("Lock poisoned"))
}

fn lock_session(
    session: &Mutex<Option<Session>>,
) -> napi::Result<std::sync::MutexGuard<'_, Option<Session>>> {
    session
        .lock()
        .map_err(|_| napi::Error::from_reason("Lock poisoned"))
}

// ---------------------------------------------------------------------------
// Generic execute helpers
// ---------------------------------------------------------------------------

/// Convert a snake_case or dot-notation command name to PascalCase.
///
/// Examples: `kv_put` → `KvPut`, `kv.put` → `KvPut`, `graph_add_node` → `GraphAddNode`
fn to_pascal_case(s: &str) -> String {
    s.replace('.', "_")
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(f) => {
                    let mut s = f.to_uppercase().to_string();
                    s.push_str(chars.as_str());
                    s
                }
            }
        })
        .collect()
}

/// Convert a plain JSON value to the tagged Value format used by serde.
///
/// `"hello"` → `{"String": "hello"}`
/// `42` → `{"Int": 42}`
/// `null` → `"Null"`
fn json_to_tagged_value(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Null => serde_json::json!("Null"),
        serde_json::Value::Bool(b) => serde_json::json!({"Bool": b}),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::json!({"Int": i})
            } else {
                serde_json::json!({"Float": n.as_f64().unwrap_or(0.0)})
            }
        }
        serde_json::Value::String(s) => serde_json::json!({"String": s}),
        serde_json::Value::Array(arr) => {
            serde_json::json!({"Array": arr.into_iter().map(json_to_tagged_value).collect::<Vec<_>>()})
        }
        serde_json::Value::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .into_iter()
                .map(|(k, v)| (k, json_to_tagged_value(v)))
                .collect();
            serde_json::json!({"Object": map})
        }
    }
}

/// All field names in the Command/types hierarchy that carry `Value` type.
/// These need conversion from plain JSON to the tagged serde format.
const VALUE_TYPED_FIELDS: &[&str] = &["value", "payload", "metadata", "properties", "definition"];

/// Convert Value-typed fields in a JSON map from plain JSON to tagged format.
fn tag_value_fields(obj: &mut serde_json::Map<String, serde_json::Value>) {
    for field in VALUE_TYPED_FIELDS {
        if let Some(v) = obj.remove(*field) {
            obj.insert((*field).to_string(), json_to_tagged_value(v));
        }
    }
}

/// Pre-process args: convert Value-typed fields from plain JSON
/// to the tagged Value format expected by serde deserialization.
///
/// Handles:
/// - Top-level fields: value, payload, metadata, properties, definition
/// - Array fields (entries, nodes, edges, filter) whose elements may
///   contain Value-typed fields
fn preprocess_value_fields(args: &mut serde_json::Map<String, serde_json::Value>) {
    // Top-level Value-typed fields
    tag_value_fields(args);

    // Array fields whose elements may contain Value-typed fields:
    // - entries: KvBatchPut, EventBatchAppend, StateBatchSet, JsonBatchSet, VectorBatchUpsert
    // - nodes/edges: GraphBulkInsert
    // - filter: VectorSearch (MetadataFilter has value: Value)
    for array_field in &["entries", "nodes", "edges", "filter"] {
        if let Some(serde_json::Value::Array(items)) = args.get_mut(*array_field) {
            for item in items.iter_mut() {
                if let serde_json::Value::Object(obj) = item {
                    tag_value_fields(obj);
                }
            }
        }
    }
}

/// Convert an Output enum to plain JSON suitable for JavaScript consumers.
fn output_to_json(output: Output) -> serde_json::Value {
    match output {
        Output::Unit => serde_json::Value::Null,
        Output::Bool(b) => serde_json::json!(b),
        Output::Uint(n) => serde_json::json!(n),
        Output::Version(n) => serde_json::json!(n),
        Output::MaybeVersion(v) => match v {
            Some(n) => serde_json::json!(n),
            None => serde_json::Value::Null,
        },
        Output::Maybe(None) => serde_json::Value::Null,
        Output::Maybe(Some(v)) => value_to_js(v),
        Output::MaybeVersioned(None) => serde_json::Value::Null,
        Output::MaybeVersioned(Some(vv)) => versioned_to_js(vv),
        Output::VersionedValues(vvs) => {
            serde_json::json!(vvs.into_iter().map(versioned_to_js).collect::<Vec<_>>())
        }
        Output::VersionHistory(None) => serde_json::Value::Null,
        Output::VersionHistory(Some(vvs)) => {
            serde_json::json!(vvs.into_iter().map(versioned_to_js).collect::<Vec<_>>())
        }
        Output::Keys(keys) => serde_json::json!(keys),
        Output::SpaceList(names) => serde_json::json!(names),
        Output::Versions(vs) => serde_json::json!(vs),
        Output::Text(s) => serde_json::json!(s),
        Output::Embedding(v) => serde_json::json!(v),
        Output::Embeddings(v) => serde_json::json!(v),
        Output::ConfigValue(v) => match v {
            Some(s) => serde_json::json!(s),
            None => serde_json::Value::Null,
        },
        // Vector/batch results contain Value fields that need un-tagging
        Output::VectorMatches(matches) => {
            serde_json::json!(matches.into_iter().map(|m| {
                serde_json::json!({
                    "key": m.key,
                    "score": m.score,
                    "metadata": m.metadata.map(value_to_js),
                })
            }).collect::<Vec<_>>())
        }
        Output::VectorData(None) => serde_json::Value::Null,
        Output::VectorData(Some(vd)) => {
            serde_json::json!({
                "key": vd.key,
                "data": {
                    "embedding": vd.data.embedding,
                    "metadata": vd.data.metadata.map(value_to_js),
                },
                "version": vd.version,
                "timestamp": vd.timestamp,
            })
        }
        Output::BatchGetResults(results) => {
            serde_json::json!(results.into_iter().map(|r| {
                let mut obj = serde_json::Map::new();
                if let Some(v) = r.value {
                    obj.insert("value".to_string(), value_to_js(v));
                }
                if let Some(v) = r.version {
                    obj.insert("version".to_string(), serde_json::json!(v));
                }
                if let Some(t) = r.timestamp {
                    obj.insert("timestamp".to_string(), serde_json::json!(t));
                }
                if let Some(e) = r.error {
                    obj.insert("error".to_string(), serde_json::json!(e));
                }
                serde_json::Value::Object(obj)
            }).collect::<Vec<_>>())
        }
        // Agent-first introspection (#1274)
        Output::Described(desc) => describe_to_js(desc),
        // Agent-first write metadata (#1443)
        Output::WriteResult { key, version } => serde_json::json!({
            "key": key,
            "version": version,
        }),
        Output::DeleteResult { key, deleted } => serde_json::json!({
            "key": key,
            "deleted": deleted,
        }),
        Output::EventAppendResult {
            sequence,
            event_type,
        } => serde_json::json!({
            "sequence": sequence,
            "eventType": event_type,
        }),
        Output::VectorWriteResult {
            collection,
            key,
            version,
        } => serde_json::json!({
            "collection": collection,
            "key": key,
            "version": version,
        }),
        Output::VectorDeleteResult {
            collection,
            key,
            deleted,
        } => serde_json::json!({
            "collection": collection,
            "key": key,
            "deleted": deleted,
        }),
        Output::StateCasResult {
            cell,
            success,
            version,
            current_value,
            current_version,
        } => serde_json::json!({
            "cell": cell,
            "success": success,
            "version": version,
            "currentValue": current_value.map(value_to_js),
            "currentVersion": current_version,
        }),
        // Pagination metadata (#1444)
        Output::KeysPage {
            keys,
            has_more,
            cursor,
        } => serde_json::json!({
            "keys": keys,
            "hasMore": has_more,
            "cursor": cursor,
        }),
        // For all remaining complex types, use serde serialization
        // and strip the outer variant wrapper.
        other => {
            if let Ok(raw) = serde_json::to_value(&other) {
                // Output serializes as {"VariantName": inner} — unwrap the variant.
                if let serde_json::Value::Object(obj) = raw {
                    if obj.len() == 1 {
                        return obj.into_iter().next().unwrap().1;
                    }
                    // Unit variants like TxnBegun serialize as strings
                    return serde_json::Value::Object(obj);
                }
                // Unit variants serialize as strings like "TxnBegun"
                raw
            } else {
                serde_json::Value::Null
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Main struct
// ---------------------------------------------------------------------------

/// StrataDB database handle.
///
/// This is the main entry point for interacting with StrataDB from Node.js.
/// All data methods are async — they run on a blocking thread pool so the
/// Node.js event loop is never blocked.
#[napi]
pub struct Strata {
    inner: Arc<Mutex<RustStrata>>,
    session: Arc<Mutex<Option<Session>>>,
}

#[napi]
impl Strata {
    // =========================================================================
    // Factory methods (sync — lightweight, no I/O worth spawning for)
    // =========================================================================

    /// Open a database at the given path.
    #[napi(factory)]
    pub fn open(path: String, options: Option<JsOpenOptions>) -> napi::Result<Self> {
        let auto_embed = options.as_ref().and_then(|o| o.auto_embed).unwrap_or(false);
        let read_only = options.as_ref().and_then(|o| o.read_only).unwrap_or(false);
        let follower = options.as_ref().and_then(|o| o.follower).unwrap_or(false);

        #[cfg(feature = "embed")]
        if auto_embed {
            if let Err(e) = strata_intelligence::embed::download::ensure_model() {
                eprintln!("Warning: failed to download model files: {}", e);
            }
        }

        let mut opts = OpenOptions::new();
        if read_only || follower {
            opts = opts.access_mode(AccessMode::ReadOnly);
        }
        if follower {
            opts = opts.follower(true);
        }

        let raw = RustStrata::open_with(&path, opts).map_err(to_napi_err)?;
        if auto_embed {
            raw.set_auto_embed(true).map_err(to_napi_err)?;
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(raw)),
            session: Arc::new(Mutex::new(None)),
        })
    }

    /// Create an in-memory database (no persistence).
    #[napi(factory)]
    pub fn cache() -> napi::Result<Self> {
        let raw = RustStrata::cache().map_err(to_napi_err)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(raw)),
            session: Arc::new(Mutex::new(None)),
        })
    }

    // =========================================================================
    // KV Store
    // =========================================================================

    /// Store a key-value pair.
    #[napi(js_name = "kvPut")]
    pub async fn kv_put(&self, key: String, value: serde_json::Value) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(value, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.kv_put(&key, v).map(|n| n as i64).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a value by key. Optionally pass `asOf` (microseconds since epoch)
    /// to read as of a past timestamp.
    #[napi(js_name = "kvGet")]
    pub async fn kv_get(&self, key: String, as_of: Option<i64>) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::KvGet {
                    branch,
                    space,
                    key,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::MaybeVersioned(Some(vv)) => Ok(value_to_js(vv.value)),
                Output::MaybeVersioned(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for KvGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a key.
    #[napi(js_name = "kvDelete")]
    pub async fn kv_delete(&self, key: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.kv_delete(&key).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List keys with optional prefix filter. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "kvList")]
    pub async fn kv_list(
        &self,
        prefix: Option<String>,
        as_of: Option<i64>,
    ) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::KvList {
                    branch,
                    space,
                    prefix,
                    cursor: None,
                    limit: None,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason("Unexpected output for KvList")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get version history for a key.
    #[napi(js_name = "kvHistory")]
    pub async fn kv_history(&self, key: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.kv_getv(&key).map_err(to_napi_err)? {
                Some(versions) => {
                    let arr: Vec<serde_json::Value> =
                        versions.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                None => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // State Cell
    // =========================================================================

    /// Set a state cell value.
    #[napi(js_name = "stateSet")]
    pub async fn state_set(&self, cell: String, value: serde_json::Value) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(value, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .state_set(&cell, v)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a state cell value. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "stateGet")]
    pub async fn state_get(
        &self,
        cell: String,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::StateGet {
                    branch,
                    space,
                    cell,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::MaybeVersioned(Some(vv)) => Ok(value_to_js(vv.value)),
                Output::MaybeVersioned(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for StateGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Initialize a state cell if it doesn't exist.
    #[napi(js_name = "stateInit")]
    pub async fn state_init(&self, cell: String, value: serde_json::Value) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(value, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .state_init(&cell, v)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Compare-and-swap update based on version.
    #[napi(js_name = "stateCas")]
    pub async fn state_cas(
        &self,
        cell: String,
        new_value: serde_json::Value,
        expected_version: Option<i64>,
    ) -> napi::Result<Option<i64>> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(new_value, 0)?;
        let exp = expected_version.map(|n| n as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .state_cas(&cell, exp, v)
                .map(|opt| opt.map(|n| n as i64))
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get version history for a state cell.
    #[napi(js_name = "stateHistory")]
    pub async fn state_history(&self, cell: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.state_getv(&cell).map_err(to_napi_err)? {
                Some(versions) => {
                    let arr: Vec<serde_json::Value> =
                        versions.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                None => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Event Log
    // =========================================================================

    /// Append an event to the log.
    #[napi(js_name = "eventAppend")]
    pub async fn event_append(
        &self,
        event_type: String,
        payload: serde_json::Value,
    ) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(payload, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .event_append(&event_type, v)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get an event by sequence number. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "eventGet")]
    pub async fn event_get(
        &self,
        sequence: i64,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::EventGet {
                    branch,
                    space,
                    sequence: sequence as u64,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::MaybeVersioned(Some(vv)) => Ok(versioned_to_js(vv)),
                Output::MaybeVersioned(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(serde_json::json!({ "value": value_to_js(v) })),
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for EventGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List events by type. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "eventList")]
    pub async fn event_list(
        &self,
        event_type: String,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::EventGetByType {
                    branch,
                    space,
                    event_type,
                    limit: None,
                    after_sequence: None,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VersionedValues(events) => {
                    let arr: Vec<serde_json::Value> =
                        events.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for EventGetByType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get total event count.
    #[napi(js_name = "eventLen")]
    pub async fn event_len(&self) -> napi::Result<i64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.event_len().map(|n| n as i64).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // JSON Store
    // =========================================================================

    /// Set a value at a JSONPath.
    #[napi(js_name = "jsonSet")]
    pub async fn json_set(
        &self,
        key: String,
        path: String,
        value: serde_json::Value,
    ) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(value, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .json_set(&key, &path, v)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a value at a JSONPath. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "jsonGet")]
    pub async fn json_get(
        &self,
        key: String,
        path: String,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::JsonGet {
                    branch,
                    space,
                    key,
                    path,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::MaybeVersioned(Some(vv)) => Ok(value_to_js(vv.value)),
                Output::MaybeVersioned(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for JsonGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a JSON document.
    #[napi(js_name = "jsonDelete")]
    pub async fn json_delete(&self, key: String, path: String) -> napi::Result<i64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .json_delete(&key, &path)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get version history for a JSON document.
    #[napi(js_name = "jsonHistory")]
    pub async fn json_history(&self, key: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.json_getv(&key).map_err(to_napi_err)? {
                Some(versions) => {
                    let arr: Vec<serde_json::Value> =
                        versions.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                None => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List JSON document keys. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "jsonList")]
    pub async fn json_list(
        &self,
        limit: u32,
        prefix: Option<String>,
        cursor: Option<String>,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::JsonList {
                    branch,
                    space,
                    prefix,
                    cursor,
                    limit: limit as u64,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::JsonListResult { keys, cursor, has_more } => Ok(serde_json::json!({
                    "keys": keys,
                    "cursor": cursor,
                    "hasMore": has_more,
                })),
                Output::Keys(keys) => Ok(serde_json::json!({
                    "keys": keys,
                    "cursor": serde_json::Value::Null,
                    "hasMore": false,
                })),
                _ => Err(napi::Error::from_reason("Unexpected output for JsonList")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Vector Store
    // =========================================================================

    /// Create a vector collection.
    #[napi(js_name = "vectorCreateCollection")]
    pub async fn vector_create_collection(
        &self,
        collection: String,
        dimension: u32,
        metric: Option<String>,
    ) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let m = match metric.as_deref().unwrap_or("cosine") {
            "cosine" => DistanceMetric::Cosine,
            "euclidean" => DistanceMetric::Euclidean,
            "dot_product" | "dotproduct" => DistanceMetric::DotProduct,
            _ => return Err(napi::Error::from_reason("[VALIDATION] Invalid metric")),
        };
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .vector_create_collection(&collection, dimension as u64, m)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a vector collection.
    #[napi(js_name = "vectorDeleteCollection")]
    pub async fn vector_delete_collection(&self, collection: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .vector_delete_collection(&collection)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List vector collections.
    #[napi(js_name = "vectorListCollections")]
    pub async fn vector_list_collections(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let collections = guard.vector_list_collections().map_err(to_napi_err)?;
            let arr: Vec<serde_json::Value> =
                collections.into_iter().map(collection_info_to_js).collect();
            Ok(serde_json::Value::Array(arr))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Insert or update a vector.
    #[napi(js_name = "vectorUpsert")]
    pub async fn vector_upsert(
        &self,
        collection: String,
        key: String,
        vector: Vec<f64>,
        metadata: Option<serde_json::Value>,
    ) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let vec = validate_vector(&vector)?;
        let meta = match metadata {
            Some(m) => Some(js_to_value_checked(m, 0)?),
            None => None,
        };
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .vector_upsert(&collection, &key, vec, meta)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a vector by key. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "vectorGet")]
    pub async fn vector_get(
        &self,
        collection: String,
        key: String,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::VectorGet {
                    branch,
                    space,
                    collection,
                    key,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VectorData(Some(vd)) => {
                    let embedding: Vec<f64> =
                        vd.data.embedding.iter().map(|&f| f as f64).collect();
                    Ok(serde_json::json!({
                        "key": vd.key,
                        "embedding": embedding,
                        "metadata": vd.data.metadata.map(value_to_js),
                        "version": vd.version,
                        "timestamp": vd.timestamp,
                    }))
                }
                Output::VectorData(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for VectorGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a vector.
    #[napi(js_name = "vectorDelete")]
    pub async fn vector_delete(&self, collection: String, key: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.vector_delete(&collection, &key).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Search for similar vectors. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "vectorSearch")]
    pub async fn vector_search(
        &self,
        collection: String,
        query: Vec<f64>,
        k: u32,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let vec = validate_vector(&query)?;
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::VectorSearch {
                    branch,
                    space,
                    collection,
                    query: vec,
                    k: k as u64,
                    filter: None,
                    metric: None,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VectorMatches(matches) => {
                    let arr: Vec<serde_json::Value> = matches
                        .into_iter()
                        .map(|m| {
                            serde_json::json!({
                                "key": m.key,
                                "score": m.score,
                                "metadata": m.metadata.map(value_to_js),
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for VectorSearch",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get statistics for a single collection.
    #[napi(js_name = "vectorCollectionStats")]
    pub async fn vector_collection_stats(
        &self,
        collection: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let info = guard
                .vector_collection_stats(&collection)
                .map_err(to_napi_err)?;
            Ok(collection_info_to_js(info))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Batch insert/update multiple vectors.
    #[napi(js_name = "vectorBatchUpsert")]
    pub async fn vector_batch_upsert(
        &self,
        collection: String,
        vectors: Vec<serde_json::Value>,
    ) -> napi::Result<Vec<i64>> {
        let inner = self.inner.clone();
        // Parse and validate all entries on the JS thread before spawning.
        let batch: Vec<BatchVectorEntry> = vectors
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let key = obj
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'key'"))?
                    .to_string();
                let raw_vec: Vec<f64> = obj
                    .get("vector")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'vector'"))?
                    .iter()
                    .map(|n| {
                        n.as_f64().ok_or_else(|| {
                            napi::Error::from_reason(
                                "[VALIDATION] Vector element is not a number",
                            )
                        })
                    })
                    .collect::<napi::Result<_>>()?;
                let vec = validate_vector(&raw_vec)?;
                let meta = match obj.get("metadata") {
                    Some(m) => Some(js_to_value_checked(m.clone(), 0)?),
                    None => None,
                };
                Ok(BatchVectorEntry {
                    key,
                    vector: vec,
                    metadata: meta,
                })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .vector_batch_upsert(&collection, batch)
                .map(|versions| versions.into_iter().map(|v| v as i64).collect())
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Branch Management
    // =========================================================================

    /// Get the current branch name.
    #[napi(js_name = "currentBranch")]
    pub async fn current_branch(&self) -> napi::Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            Ok(guard.current_branch().to_string())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Switch to a different branch.
    #[napi(js_name = "setBranch")]
    pub async fn set_branch(&self, branch: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner
                .lock()
                .map_err(|_| napi::Error::from_reason("Lock poisoned"))?;
            guard.set_branch(&branch).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Create a new empty branch.
    #[napi(js_name = "createBranch")]
    pub async fn create_branch(
        &self,
        branch: String,
        metadata: Option<serde_json::Value>,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        let meta_val = metadata
            .map(|m| js_to_value_checked(m, 0))
            .transpose()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::BranchCreate {
                    branch_id: Some(branch),
                    metadata: meta_val,
                })
                .map_err(to_napi_err)?
            {
                Output::BranchWithVersion { .. } => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for BranchCreate",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Fork the current branch to a new branch, copying all data.
    #[napi(js_name = "forkBranch")]
    pub async fn fork_branch(&self, destination: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let info = guard.fork_branch(&destination).map_err(to_napi_err)?;
            Ok(serde_json::json!({
                "source": info.source,
                "destination": info.destination,
                "keysCopied": info.keys_copied,
            }))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all branches.
    #[napi(js_name = "listBranches")]
    pub async fn list_branches(
        &self,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::BranchList {
                    state: None,
                    limit: limit.map(|l| l as u64),
                    offset: offset.map(|o| o as u64),
                })
                .map_err(to_napi_err)?
            {
                Output::BranchInfoList(branches) => {
                    let names: Vec<serde_json::Value> = branches
                        .into_iter()
                        .map(|b| serde_json::Value::String(b.info.id.as_str().to_string()))
                        .collect();
                    Ok(serde_json::Value::Array(names))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for BranchList",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a branch.
    #[napi(js_name = "deleteBranch")]
    pub async fn delete_branch(&self, branch: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.delete_branch(&branch).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Check if a branch exists.
    #[napi(js_name = "branchExists")]
    pub async fn branch_exists(&self, name: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.branches().exists(&name).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get branch metadata with version info.
    #[napi(js_name = "branchGet")]
    pub async fn branch_get(&self, name: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.branch_get(&name).map_err(to_napi_err)? {
                Some(info) => Ok(versioned_branch_info_to_js(info)),
                None => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Compare two branches.
    #[napi(js_name = "diffBranches")]
    pub async fn diff_branches(
        &self,
        branch_a: String,
        branch_b: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let diff = guard
                .diff_branches(&branch_a, &branch_b)
                .map_err(to_napi_err)?;
            Ok(serde_json::json!({
                "branchA": diff.branch_a,
                "branchB": diff.branch_b,
                "summary": {
                    "totalAdded": diff.summary.total_added,
                    "totalRemoved": diff.summary.total_removed,
                    "totalModified": diff.summary.total_modified,
                },
            }))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Merge a branch into the current branch.
    #[napi(js_name = "mergeBranches")]
    pub async fn merge_branches(
        &self,
        source: String,
        strategy: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let strat = match strategy.as_deref().unwrap_or("last_writer_wins") {
            "last_writer_wins" => MergeStrategy::LastWriterWins,
            "strict" => MergeStrategy::Strict,
            _ => return Err(napi::Error::from_reason("[VALIDATION] Invalid merge strategy")),
        };
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let target = guard.current_branch().to_string();
            let info = guard
                .merge_branches(&source, &target, strat)
                .map_err(to_napi_err)?;
            let conflicts: Vec<serde_json::Value> = info
                .conflicts
                .into_iter()
                .map(|c| {
                    serde_json::json!({
                        "key": c.key,
                        "space": c.space,
                    })
                })
                .collect();
            Ok(serde_json::json!({
                "keysApplied": info.keys_applied,
                "spacesMerged": info.spaces_merged,
                "conflicts": conflicts,
            }))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Space Management
    // =========================================================================

    /// Get the current space name.
    #[napi(js_name = "currentSpace")]
    pub async fn current_space(&self) -> napi::Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            Ok(guard.current_space().to_string())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Switch to a different space.
    #[napi(js_name = "setSpace")]
    pub async fn set_space(&self, space: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner
                .lock()
                .map_err(|_| napi::Error::from_reason("Lock poisoned"))?;
            guard.set_space(&space).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all spaces in the current branch.
    #[napi(js_name = "listSpaces")]
    pub async fn list_spaces(&self) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.list_spaces().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a space and all its data.
    #[napi(js_name = "deleteSpace")]
    pub async fn delete_space(&self, space: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.delete_space(&space).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Force delete a space even if non-empty.
    #[napi(js_name = "deleteSpaceForce")]
    pub async fn delete_space_force(&self, space: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.delete_space_force(&space).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Database Operations
    // =========================================================================

    /// Check database connectivity.
    #[napi]
    pub async fn ping(&self) -> napi::Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.ping().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get database info.
    #[napi]
    pub async fn info(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let info = guard.info().map_err(to_napi_err)?;
            Ok(serde_json::json!({
                "version": info.version,
                "uptimeSecs": info.uptime_secs,
                "branchCount": info.branch_count,
                "totalKeys": info.total_keys,
            }))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a structured snapshot of the database for agent introspection.
    ///
    /// Returns version, branch, spaces, follower status, per-primitive
    /// summaries (counts, collections, graphs), configuration, and
    /// capability flags — everything an agent needs to plan its actions.
    #[napi]
    pub async fn describe(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let output = guard
                .executor()
                .execute(Command::Describe { branch })
                .map_err(to_napi_err)?;
            Ok(output_to_json(output))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Flush writes to disk.
    #[napi]
    pub async fn flush(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.flush().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Trigger compaction.
    #[napi]
    pub async fn compact(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.compact().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Bundle Operations
    // =========================================================================

    /// Export a branch to a bundle file.
    #[napi(js_name = "branchExport")]
    pub async fn branch_export(
        &self,
        branch: String,
        path: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let result = guard.branch_export(&branch, &path).map_err(to_napi_err)?;
            Ok(branch_export_result_to_js(result))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Import a branch from a bundle file.
    #[napi(js_name = "branchImport")]
    pub async fn branch_import(&self, path: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let result = guard.branch_import(&path).map_err(to_napi_err)?;
            Ok(branch_import_result_to_js(result))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Validate a bundle file without importing.
    #[napi(js_name = "branchValidateBundle")]
    pub async fn branch_validate_bundle(&self, path: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let result = guard.branch_validate_bundle(&path).map_err(to_napi_err)?;
            Ok(bundle_validate_result_to_js(result))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Transaction Operations
    // =========================================================================

    /// Begin a new transaction.
    #[napi(js_name = "begin")]
    pub async fn begin(&self, read_only: Option<bool>) -> napi::Result<()> {
        let inner = self.inner.clone();
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            if session_ref.is_none() {
                let guard = lock_inner(&inner)?;
                *session_ref = Some(guard.session());
            }
            let session = session_ref.as_mut().unwrap();
            let cmd = Command::TxnBegin {
                branch: None,
                options: Some(TxnOptions {
                    read_only: read_only.unwrap_or(false),
                }),
            };
            session.execute(cmd).map_err(to_napi_err)?;
            Ok(())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Commit the current transaction.
    #[napi]
    pub async fn commit(&self) -> napi::Result<i64> {
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            let session = session_ref
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("[STATE] No transaction active"))?;
            match session.execute(Command::TxnCommit).map_err(to_napi_err)? {
                Output::TxnCommitted { version } => Ok(version as i64),
                _ => Err(napi::Error::from_reason("Unexpected output for TxnCommit")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Rollback the current transaction.
    #[napi]
    pub async fn rollback(&self) -> napi::Result<()> {
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            let session = session_ref
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("[STATE] No transaction active"))?;
            session.execute(Command::TxnRollback).map_err(to_napi_err)?;
            Ok(())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get current transaction info.
    #[napi(js_name = "txnInfo")]
    pub async fn txn_info(&self) -> napi::Result<serde_json::Value> {
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            if session_ref.is_none() {
                return Ok(serde_json::Value::Null);
            }
            let session = session_ref.as_mut().unwrap();
            match session.execute(Command::TxnInfo).map_err(to_napi_err)? {
                Output::TxnInfo(Some(info)) => Ok(serde_json::json!({
                    "id": info.id,
                    "status": format!("{:?}", info.status).to_lowercase(),
                    "startedAt": info.started_at,
                })),
                Output::TxnInfo(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for TxnInfo")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Check if a transaction is currently active.
    #[napi(js_name = "txnIsActive")]
    pub async fn txn_is_active(&self) -> napi::Result<bool> {
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            if session_ref.is_none() {
                return Ok(false);
            }
            let session = session_ref.as_mut().unwrap();
            match session.execute(Command::TxnIsActive).map_err(to_napi_err)? {
                Output::Bool(active) => Ok(active),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for TxnIsActive",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // State Operations
    // =========================================================================

    /// Delete a state cell.
    #[napi(js_name = "stateDelete")]
    pub async fn state_delete(&self, cell: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::StateDelete {
                    branch: None,
                    space: None,
                    cell,
                })
                .map_err(to_napi_err)?
            {
                Output::DeleteResult { deleted, .. } => Ok(deleted),
                _ => Err(napi::Error::from_reason("Unexpected output for StateDelete")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List state cell names with optional prefix filter. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "stateList")]
    pub async fn state_list(
        &self,
        prefix: Option<String>,
        as_of: Option<i64>,
    ) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::StateList {
                    branch,
                    space,
                    prefix,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason("Unexpected output for StateList")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Versioned Getters
    // =========================================================================

    /// Get a value by key with version info.
    #[napi(js_name = "kvGetVersioned")]
    pub async fn kv_get_versioned(&self, key: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.kv_getv(&key).map_err(to_napi_err)? {
                Some(versions) if !versions.is_empty() => {
                    Ok(versioned_to_js(versions.into_iter().next().unwrap()))
                }
                _ => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a state cell value with version info.
    #[napi(js_name = "stateGetVersioned")]
    pub async fn state_get_versioned(&self, cell: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.state_getv(&cell).map_err(to_napi_err)? {
                Some(versions) if !versions.is_empty() => {
                    Ok(versioned_to_js(versions.into_iter().next().unwrap()))
                }
                _ => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a JSON document value with version info.
    #[napi(js_name = "jsonGetVersioned")]
    pub async fn json_get_versioned(&self, key: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.json_getv(&key).map_err(to_napi_err)? {
                Some(versions) if !versions.is_empty() => {
                    Ok(versioned_to_js(versions.into_iter().next().unwrap()))
                }
                _ => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Pagination
    // =========================================================================

    /// List keys with pagination support. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "kvListPaginated")]
    pub async fn kv_list_paginated(
        &self,
        prefix: Option<String>,
        limit: Option<u32>,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::KvList {
                    branch,
                    space,
                    prefix,
                    cursor: None,
                    limit: limit.map(|l| l as u64),
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::KeysPage { keys, has_more, cursor } => Ok(serde_json::json!({
                    "keys": keys,
                    "hasMore": has_more,
                    "cursor": cursor,
                })),
                Output::Keys(keys) => Ok(serde_json::json!({
                    "keys": keys,
                    "hasMore": false,
                    "cursor": serde_json::Value::Null,
                })),
                _ => Err(napi::Error::from_reason("Unexpected output for KvList")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List events by type with pagination support. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "eventListPaginated")]
    pub async fn event_list_paginated(
        &self,
        event_type: String,
        limit: Option<u32>,
        after: Option<i64>,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::EventGetByType {
                    branch,
                    space,
                    event_type,
                    limit: limit.map(|l| l as u64),
                    after_sequence: after.map(|a| a as u64),
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VersionedValues(events) => {
                    let arr: Vec<serde_json::Value> =
                        events.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for EventGetByType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Enhanced Vector Search
    // =========================================================================

    /// Search for similar vectors with optional filter and metric override.
    /// Optionally pass `asOf` for time-travel.
    #[napi(js_name = "vectorSearchFiltered")]
    pub async fn vector_search_filtered(
        &self,
        collection: String,
        query: Vec<f64>,
        k: u32,
        metric: Option<String>,
        filter: Option<Vec<serde_json::Value>>,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let vec = validate_vector(&query)?;

        let metric_enum = match metric.as_deref() {
            Some("cosine") => Some(DistanceMetric::Cosine),
            Some("euclidean") => Some(DistanceMetric::Euclidean),
            Some("dot_product") | Some("dotproduct") => Some(DistanceMetric::DotProduct),
            Some(m) => {
                return Err(napi::Error::from_reason(format!(
                    "[VALIDATION] Invalid metric: {}",
                    m
                )))
            }
            None => None,
        };

        let as_of_u64 = as_of.map(|t| t as u64);

        let filter_vec = match filter {
            Some(arr) => {
                let mut filters = Vec::new();
                for item in arr {
                    let obj = item.as_object().ok_or_else(|| {
                        napi::Error::from_reason("[VALIDATION] Filter must be an object")
                    })?;
                    let field = obj
                        .get("field")
                        .and_then(|f| f.as_str())
                        .ok_or_else(|| {
                            napi::Error::from_reason("[VALIDATION] Filter missing 'field'")
                        })?
                        .to_string();
                    let op_str =
                        obj.get("op").and_then(|o| o.as_str()).ok_or_else(|| {
                            napi::Error::from_reason("[VALIDATION] Filter missing 'op'")
                        })?;
                    let op = match op_str {
                        "eq" => FilterOp::Eq,
                        "ne" => FilterOp::Ne,
                        "gt" => FilterOp::Gt,
                        "gte" => FilterOp::Gte,
                        "lt" => FilterOp::Lt,
                        "lte" => FilterOp::Lte,
                        "in" => FilterOp::In,
                        "contains" => FilterOp::Contains,
                        _ => {
                            return Err(napi::Error::from_reason(format!(
                                "[VALIDATION] Invalid filter op: {}",
                                op_str
                            )))
                        }
                    };
                    let value_json = obj.get("value").ok_or_else(|| {
                        napi::Error::from_reason("[VALIDATION] Filter missing 'value'")
                    })?.clone();
                    let value = js_to_value_checked(value_json, 0)?;
                    filters.push(MetadataFilter { field, op, value });
                }
                Some(filters)
            }
            None => None,
        };

        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::VectorSearch {
                    branch,
                    space,
                    collection,
                    query: vec,
                    k: k as u64,
                    filter: filter_vec,
                    metric: metric_enum,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VectorMatches(matches) => {
                    let arr: Vec<serde_json::Value> = matches
                        .into_iter()
                        .map(|m| {
                            serde_json::json!({
                                "key": m.key,
                                "score": m.score,
                                "metadata": m.metadata.map(value_to_js),
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for VectorSearch",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Space Operations
    // =========================================================================

    /// Create a new space explicitly.
    #[napi(js_name = "spaceCreate")]
    pub async fn space_create(&self, space: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::SpaceCreate {
                    branch: None,
                    space,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason("Unexpected output for SpaceCreate")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Check if a space exists in the current branch.
    #[napi(js_name = "spaceExists")]
    pub async fn space_exists(&self, space: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::SpaceExists {
                    branch: None,
                    space,
                })
                .map_err(to_napi_err)?
            {
                Output::Bool(exists) => Ok(exists),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for SpaceExists",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Configuration
    // =========================================================================

    /// Get the current database configuration.
    ///
    /// Returns an object with `durability`, `autoEmbed`, and optional `model`.
    #[napi]
    pub async fn config(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let cfg = guard.config().map_err(to_napi_err)?;
            let mut obj = serde_json::Map::new();
            obj.insert("durability".into(), serde_json::Value::String(cfg.durability));
            obj.insert("autoEmbed".into(), serde_json::Value::Bool(cfg.auto_embed));
            if let Some(model) = cfg.model {
                let mut m = serde_json::Map::new();
                m.insert("endpoint".into(), serde_json::Value::String(model.endpoint));
                m.insert("model".into(), serde_json::Value::String(model.model));
                m.insert(
                    "apiKey".into(),
                    model
                        .api_key
                        .map(|s| serde_json::Value::String(s.to_string()))
                        .unwrap_or(serde_json::Value::Null),
                );
                m.insert("timeoutMs".into(), serde_json::Value::Number(model.timeout_ms.into()));
                obj.insert("model".into(), serde_json::Value::Object(m));
            } else {
                obj.insert("model".into(), serde_json::Value::Null);
            }
            Ok(serde_json::Value::Object(obj))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Check whether auto-embedding is enabled.
    #[napi(js_name = "autoEmbedEnabled")]
    pub async fn auto_embed_enabled(&self) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.auto_embed_enabled().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Enable or disable auto-embedding of text values.
    ///
    /// Persisted to strata.toml for disk-backed databases.
    #[napi(js_name = "setAutoEmbed")]
    pub async fn set_auto_embed(&self, enabled: bool) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.set_auto_embed(enabled).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Configure an inference model endpoint for intelligent search.
    ///
    /// When a model is configured, `search()` transparently expands queries
    /// using the model for better recall. Search works identically without a model.
    /// Persisted to strata.toml.
    #[napi(js_name = "configureModel")]
    pub async fn configure_model(
        &self,
        endpoint: String,
        model: String,
        api_key: Option<String>,
        timeout_ms: Option<u32>,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .executor()
                .execute(Command::ConfigureModel {
                    endpoint,
                    model,
                    api_key,
                    timeout_ms: timeout_ms.map(|ms| ms as u64),
                })
                .map_err(to_napi_err)?;
            Ok(())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Search
    // =========================================================================

    /// Search across multiple primitives for matching content.
    #[napi]
    pub async fn search(
        &self,
        query: String,
        options: Option<JsSearchOptions>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;

            let (k, primitives, time_range, mode, expand, rerank) = match options {
                Some(opts) => (
                    opts.k,
                    opts.primitives,
                    opts.time_range.map(|tr| TimeRangeInput {
                        start: tr.start,
                        end: tr.end,
                    }),
                    opts.mode,
                    opts.expand,
                    opts.rerank,
                ),
                None => (None, None, None, None, None, None),
            };

            let sq = SearchQuery {
                query,
                k: k.map(|n| n as u64),
                primitives,
                time_range,
                mode,
                expand,
                rerank,
                precomputed_embedding: None,
            };

            match guard
                .executor()
                .execute(Command::Search {
                    branch: None,
                    space: None,
                    search: sq,
                })
                .map_err(to_napi_err)?
            {
                Output::SearchResults(results) => {
                    let arr: Vec<serde_json::Value> = results
                        .into_iter()
                        .map(|hit| {
                            serde_json::json!({
                                "entity": hit.entity,
                                "primitive": hit.primitive,
                                "score": hit.score,
                                "rank": hit.rank,
                                "snippet": hit.snippet,
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason("Unexpected output for Search")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Retention
    // =========================================================================

    /// Apply retention policy to trigger garbage collection.
    #[napi(js_name = "retentionApply")]
    pub async fn retention_apply(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::RetentionApply { branch: None })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for RetentionApply",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Generic command dispatch
    // =========================================================================

    /// Execute any command by name with JSON arguments.
    ///
    /// This provides a generic dispatch interface: pass a command name (snake_case
    /// or dot-notation) and a JSON args object, and get a JSON result back.
    ///
    /// ```js
    /// const version = await db.execute("kv_put", { key: "foo", value: "bar" });
    /// const val = await db.execute("kv_get", { key: "foo" });
    /// const keys = await db.execute("kv.list", { prefix: "f" });
    /// ```
    ///
    /// Command names map to executor Command variants: `kv_put` → `KvPut`,
    /// `graph_add_node` → `GraphAddNode`, etc.  Branch and space default to
    /// the current context if not specified in args.
    #[napi]
    pub async fn execute(
        &self,
        command: String,
        args: Option<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            // Normalize command name: kv.put → kv_put → KvPut
            let pascal = to_pascal_case(&command);

            // Get args as a mutable map (empty if null/absent)
            let mut args_map = match args.unwrap_or(serde_json::Value::Null) {
                serde_json::Value::Object(m) => m,
                serde_json::Value::Null => serde_json::Map::new(),
                _ => {
                    return Err(napi::Error::from_reason(
                        "[VALIDATION] args must be an object or null",
                    ))
                }
            };

            // Convert plain JSON values to tagged Value format for value/payload fields
            preprocess_value_fields(&mut args_map);

            // Build the Command JSON.
            // Unit variants (Ping, Info, etc.) serialize as just "Ping",
            // while struct variants serialize as {"KvPut": {key: ..., value: ...}}.
            // Try struct form first, fall back to unit variant if args are empty.
            let cmd: Command = if args_map.is_empty() {
                // Try unit variant first (e.g., "Ping")
                serde_json::from_value::<Command>(serde_json::Value::String(pascal.clone()))
                    .or_else(|_| {
                        // Fall back to struct variant with empty fields
                        let mut m = serde_json::Map::new();
                        m.insert(pascal.clone(), serde_json::Value::Object(args_map.clone()));
                        serde_json::from_value::<Command>(serde_json::Value::Object(m))
                    })
            } else {
                let mut m = serde_json::Map::new();
                m.insert(pascal.clone(), serde_json::Value::Object(args_map));
                serde_json::from_value::<Command>(serde_json::Value::Object(m))
            }
            .map_err(|e| {
                napi::Error::from_reason(format!(
                    "[VALIDATION] Invalid command '{}': {}",
                    command, e
                ))
            })?;

            // Execute through session (supports transactions) or executor
            let mut session_guard = lock_session(&session_arc)?;
            let output = if let Some(session) = session_guard.as_mut() {
                session.execute(cmd).map_err(to_napi_err)?
            } else {
                let guard = lock_inner(&inner)?;
                guard.executor().execute(cmd).map_err(to_napi_err)?
            };

            // Convert Output to plain JSON
            Ok(output_to_json(output))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Follower mode
    // =========================================================================

    /// Returns `true` if this database was opened in read-only follower mode.
    #[napi(js_name = "isFollower")]
    pub fn is_follower(&self) -> napi::Result<bool> {
        let guard = lock_inner(&self.inner)?;
        Ok(guard.database().is_follower())
    }

    /// Replay new WAL records from the primary.
    ///
    /// Only meaningful for follower instances (opened with `{ follower: true }`).
    /// Returns the number of new records applied. Returns 0 for non-follower
    /// instances or when there are no new records.
    #[napi]
    pub async fn refresh(&self) -> napi::Result<i64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let applied = guard
                .database()
                .refresh()
                .map_err(|e| napi::Error::from_reason(format!("{}", e)))?;
            Ok(applied as i64)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /// Close the database, releasing all resources.
    ///
    /// After calling `close()`, any further method call on this instance will
    /// fail with a "Lock poisoned" or similar error.  This mirrors the
    /// `client.close()` pattern used by every major Node.js database driver.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            // Drop session first (it borrows the inner DB).
            {
                let mut s = lock_session(&session_arc)?;
                *s = None;
            }
            // Replace the inner Strata with a freshly-opened cache that will
            // be immediately dropped, effectively releasing the original DB.
            let mut guard = inner
                .lock()
                .map_err(|_| napi::Error::from_reason("Lock poisoned"))?;
            let placeholder = RustStrata::cache().map_err(to_napi_err)?;
            *guard = placeholder;
            Ok(())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Time Travel
    // =========================================================================

    /// Get the time range (oldest and latest timestamps) for the current branch.
    #[napi(js_name = "timeRange")]
    pub async fn time_range(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            match guard
                .executor()
                .execute(Command::TimeRange { branch })
                .map_err(to_napi_err)?
            {
                Output::TimeRange {
                    oldest_ts,
                    latest_ts,
                } => Ok(serde_json::json!({
                    "oldestTs": oldest_ts.map(|t| t as i64),
                    "latestTs": latest_ts.map(|t| t as i64),
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for TimeRange",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Batch Operations
    // =========================================================================

    /// Batch put multiple KV entries.
    #[napi(js_name = "kvBatchPut")]
    pub async fn kv_batch_put(
        &self,
        entries: Vec<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let batch: Vec<BatchKvEntry> = entries
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let key = obj
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'key'"))?
                    .to_string();
                let value = obj
                    .get("value")
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'value'"))?
                    .clone();
                let value = js_to_value_checked(value, 0)?;
                Ok(BatchKvEntry { key, value })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::KvBatchPut {
                    branch,
                    space,
                    entries: batch,
                })
                .map_err(to_napi_err)?
            {
                Output::BatchResults(results) => Ok(batch_results_to_js(results)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for KvBatchPut",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Batch set multiple state cells.
    #[napi(js_name = "stateBatchSet")]
    pub async fn state_batch_set(
        &self,
        entries: Vec<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let batch: Vec<BatchStateEntry> = entries
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let cell = obj
                    .get("cell")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'cell'"))?
                    .to_string();
                let value = obj
                    .get("value")
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'value'"))?
                    .clone();
                let value = js_to_value_checked(value, 0)?;
                Ok(BatchStateEntry { cell, value })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::StateBatchSet {
                    branch,
                    space,
                    entries: batch,
                })
                .map_err(to_napi_err)?
            {
                Output::BatchResults(results) => Ok(batch_results_to_js(results)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for StateBatchSet",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Batch append multiple events.
    #[napi(js_name = "eventBatchAppend")]
    pub async fn event_batch_append(
        &self,
        entries: Vec<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let batch: Vec<BatchEventEntry> = entries
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let event_type = obj
                    .get("event_type")
                    .or_else(|| obj.get("eventType"))
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| {
                        napi::Error::from_reason("[VALIDATION] Missing 'event_type'")
                    })?
                    .to_string();
                let payload = obj
                    .get("payload")
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'payload'"))?
                    .clone();
                let payload = js_to_value_checked(payload, 0)?;
                Ok(BatchEventEntry {
                    event_type,
                    payload,
                })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::EventBatchAppend {
                    branch,
                    space,
                    entries: batch,
                })
                .map_err(to_napi_err)?
            {
                Output::BatchResults(results) => Ok(batch_results_to_js(results)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for EventBatchAppend",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Batch set multiple JSON documents.
    #[napi(js_name = "jsonBatchSet")]
    pub async fn json_batch_set(
        &self,
        entries: Vec<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let batch: Vec<BatchJsonEntry> = entries
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let key = obj
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'key'"))?
                    .to_string();
                let path = obj
                    .get("path")
                    .and_then(|p| p.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'path'"))?
                    .to_string();
                let value = obj
                    .get("value")
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'value'"))?
                    .clone();
                let value = js_to_value_checked(value, 0)?;
                Ok(BatchJsonEntry { key, path, value })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::JsonBatchSet {
                    branch,
                    space,
                    entries: batch,
                })
                .map_err(to_napi_err)?
            {
                Output::BatchResults(results) => Ok(batch_results_to_js(results)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for JsonBatchSet",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Batch get multiple JSON documents.
    #[napi(js_name = "jsonBatchGet")]
    pub async fn json_batch_get(
        &self,
        entries: Vec<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let batch: Vec<BatchJsonGetEntry> = entries
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let key = obj
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'key'"))?
                    .to_string();
                let path = obj
                    .get("path")
                    .and_then(|p| p.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'path'"))?
                    .to_string();
                Ok(BatchJsonGetEntry { key, path })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::JsonBatchGet {
                    branch,
                    space,
                    entries: batch,
                })
                .map_err(to_napi_err)?
            {
                Output::BatchGetResults(results) => Ok(batch_get_results_to_js(results)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for JsonBatchGet",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Batch delete multiple JSON documents.
    #[napi(js_name = "jsonBatchDelete")]
    pub async fn json_batch_delete(
        &self,
        entries: Vec<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let batch: Vec<BatchJsonDeleteEntry> = entries
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let key = obj
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'key'"))?
                    .to_string();
                let path = obj
                    .get("path")
                    .and_then(|p| p.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'path'"))?
                    .to_string();
                Ok(BatchJsonDeleteEntry { key, path })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::JsonBatchDelete {
                    branch,
                    space,
                    entries: batch,
                })
                .map_err(to_napi_err)?
            {
                Output::BatchResults(results) => Ok(batch_results_to_js(results)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for JsonBatchDelete",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Configuration (key-value)
    // =========================================================================

    /// Set a configuration key-value pair.
    #[napi(js_name = "configureSet")]
    pub async fn configure_set(&self, key: String, value: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::ConfigureSet { key, value })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for ConfigureSet",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a configuration value by key.
    #[napi(js_name = "configureGet")]
    pub async fn configure_get(&self, key: String) -> napi::Result<Option<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::ConfigureGetKey { key })
                .map_err(to_napi_err)?
            {
                Output::ConfigValue(v) => Ok(v),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for ConfigureGetKey",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Embedding
    // =========================================================================

    /// Embed a single text string.
    #[napi]
    pub async fn embed(&self, text: String) -> napi::Result<Vec<f64>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::Embed { text })
                .map_err(to_napi_err)?
            {
                Output::Embedding(vec) => Ok(vec.into_iter().map(|f| f as f64).collect()),
                _ => Err(napi::Error::from_reason("Unexpected output for Embed")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Embed multiple texts in a batch.
    #[napi(js_name = "embedBatch")]
    pub async fn embed_batch(&self, texts: Vec<String>) -> napi::Result<Vec<Vec<f64>>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::EmbedBatch { texts })
                .map_err(to_napi_err)?
            {
                Output::Embeddings(vecs) => Ok(vecs
                    .into_iter()
                    .map(|v| v.into_iter().map(|f| f as f64).collect())
                    .collect()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for EmbedBatch",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get the embedding pipeline status.
    #[napi(js_name = "embedStatus")]
    pub async fn embed_status(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::EmbedStatus)
                .map_err(to_napi_err)?
            {
                Output::EmbedStatus(info) => Ok(serde_json::json!({
                    "autoEmbed": info.auto_embed,
                    "batchSize": info.batch_size,
                    "pending": info.pending,
                    "totalQueued": info.total_queued,
                    "totalEmbedded": info.total_embedded,
                    "totalFailed": info.total_failed,
                    "schedulerQueueDepth": info.scheduler_queue_depth,
                    "schedulerActiveTasks": info.scheduler_active_tasks,
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for EmbedStatus",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Inference
    // =========================================================================

    /// Generate text from a model.
    #[napi]
    pub async fn generate(
        &self,
        model: String,
        prompt: String,
        options: Option<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let (max_tokens, temperature, top_k, top_p, seed, stop_tokens, stop_sequences) =
            match options {
                Some(opts) => {
                    let obj = opts.as_object();
                    (
                        obj.and_then(|o| o.get("maxTokens"))
                            .and_then(|v| v.as_u64())
                            .map(|n| n as usize),
                        obj.and_then(|o| o.get("temperature"))
                            .and_then(|v| v.as_f64())
                            .map(|f| f as f32),
                        obj.and_then(|o| o.get("topK"))
                            .and_then(|v| v.as_u64())
                            .map(|n| n as usize),
                        obj.and_then(|o| o.get("topP"))
                            .and_then(|v| v.as_f64())
                            .map(|f| f as f32),
                        obj.and_then(|o| o.get("seed")).and_then(|v| v.as_u64()),
                        obj.and_then(|o| o.get("stopTokens"))
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|n| n.as_u64().map(|n| n as u32))
                                    .collect()
                            }),
                        obj.and_then(|o| o.get("stopSequences"))
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                                    .collect()
                            }),
                    )
                }
                None => (None, None, None, None, None, None, None),
            };
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::Generate {
                    model,
                    prompt,
                    max_tokens,
                    temperature,
                    top_k,
                    top_p,
                    seed,
                    stop_tokens,
                    stop_sequences,
                })
                .map_err(to_napi_err)?
            {
                Output::Generated(result) => Ok(serde_json::json!({
                    "text": result.text,
                    "stopReason": result.stop_reason,
                    "promptTokens": result.prompt_tokens,
                    "completionTokens": result.completion_tokens,
                    "model": result.model,
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for Generate",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Tokenize text using a model's tokenizer.
    #[napi]
    pub async fn tokenize(
        &self,
        model: String,
        text: String,
        options: Option<serde_json::Value>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let add_special_tokens = options
            .and_then(|o| o.as_object().and_then(|obj| obj.get("addSpecialTokens")?.as_bool()));
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::Tokenize {
                    model,
                    text,
                    add_special_tokens,
                })
                .map_err(to_napi_err)?
            {
                Output::TokenIds(result) => Ok(serde_json::json!({
                    "ids": result.ids,
                    "count": result.count,
                    "model": result.model,
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for Tokenize",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Detokenize token IDs back to text.
    #[napi]
    pub async fn detokenize(
        &self,
        model: String,
        ids: Vec<u32>,
    ) -> napi::Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::Detokenize { model, ids })
                .map_err(to_napi_err)?
            {
                Output::Text(text) => Ok(text),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for Detokenize",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Unload a model from memory.
    #[napi(js_name = "generateUnload")]
    pub async fn generate_unload(&self, model: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GenerateUnload { model })
                .map_err(to_napi_err)?
            {
                Output::Bool(b) => Ok(b),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GenerateUnload",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Model Management
    // =========================================================================

    /// List all available models.
    #[napi(js_name = "modelsList")]
    pub async fn models_list(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::ModelsList)
                .map_err(to_napi_err)?
            {
                Output::ModelsList(models) => {
                    let arr: Vec<serde_json::Value> = models
                        .into_iter()
                        .map(|m| {
                            serde_json::json!({
                                "name": m.name,
                                "task": m.task,
                                "architecture": m.architecture,
                                "defaultQuant": m.default_quant,
                                "embeddingDim": m.embedding_dim,
                                "isLocal": m.is_local,
                                "sizeBytes": m.size_bytes,
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for ModelsList",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Pull/download a model by name.
    #[napi(js_name = "modelsPull")]
    pub async fn models_pull(&self, name: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::ModelsPull { name })
                .map_err(to_napi_err)?
            {
                Output::ModelsPulled { name, path } => Ok(serde_json::json!({
                    "name": name,
                    "path": path,
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for ModelsPull",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List locally downloaded models.
    #[napi(js_name = "modelsLocal")]
    pub async fn models_local(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::ModelsLocal)
                .map_err(to_napi_err)?
            {
                Output::ModelsList(models) => {
                    let arr: Vec<serde_json::Value> = models
                        .into_iter()
                        .map(|m| {
                            serde_json::json!({
                                "name": m.name,
                                "task": m.task,
                                "architecture": m.architecture,
                                "defaultQuant": m.default_quant,
                                "embeddingDim": m.embedding_dim,
                                "isLocal": m.is_local,
                                "sizeBytes": m.size_bytes,
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for ModelsLocal",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Durability
    // =========================================================================

    /// Get WAL durability counters.
    #[napi(js_name = "durabilityCounters")]
    pub async fn durability_counters(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::DurabilityCounters)
                .map_err(to_napi_err)?
            {
                Output::DurabilityCounters(counters) => Ok(serde_json::json!({
                    "walAppends": counters.wal_appends,
                    "syncCalls": counters.sync_calls,
                    "bytesWritten": counters.bytes_written,
                    "syncNanos": counters.sync_nanos,
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for DurabilityCounters",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Graph — Lifecycle
    // =========================================================================

    /// Create a new graph.
    #[napi(js_name = "graphCreate")]
    pub async fn graph_create(
        &self,
        graph: String,
        cascade_policy: Option<String>,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphCreate {
                    branch: None,
                    graph,
                    cascade_policy,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphCreate",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a graph.
    #[napi(js_name = "graphDelete")]
    pub async fn graph_delete(&self, graph: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphDelete {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphDelete",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all graph names.
    #[napi(js_name = "graphList")]
    pub async fn graph_list(&self) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphList { branch: None })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphList",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get graph metadata.
    #[napi(js_name = "graphGetMeta")]
    pub async fn graph_get_meta(&self, graph: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphGetMeta {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphGetMeta",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Graph — Nodes
    // =========================================================================

    /// Add or update a node.
    #[napi(js_name = "graphAddNode")]
    pub async fn graph_add_node(
        &self,
        graph: String,
        node_id: String,
        entity_ref: Option<String>,
        properties: Option<serde_json::Value>,
        object_type: Option<String>,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        let props = properties
            .map(|p| js_to_value_checked(p, 0))
            .transpose()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphAddNode {
                    branch: None,
                    graph,
                    node_id,
                    entity_ref,
                    properties: props,
                    object_type,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphAddNode",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a node.
    #[napi(js_name = "graphGetNode")]
    pub async fn graph_get_node(
        &self,
        graph: String,
        node_id: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphGetNode {
                    branch: None,
                    graph,
                    node_id,
                })
                .map_err(to_napi_err)?
            {
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphGetNode",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Remove a node and its incident edges.
    #[napi(js_name = "graphRemoveNode")]
    pub async fn graph_remove_node(
        &self,
        graph: String,
        node_id: String,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphRemoveNode {
                    branch: None,
                    graph,
                    node_id,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphRemoveNode",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all node IDs in a graph.
    #[napi(js_name = "graphListNodes")]
    pub async fn graph_list_nodes(&self, graph: String) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphListNodes {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphListNodes",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List node IDs with cursor-based pagination.
    #[napi(js_name = "graphListNodesPaginated")]
    pub async fn graph_list_nodes_paginated(
        &self,
        graph: String,
        limit: u32,
        cursor: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphListNodesPaginated {
                    branch: None,
                    graph,
                    limit: limit as usize,
                    cursor,
                })
                .map_err(to_napi_err)?
            {
                Output::GraphPage { items, next_cursor } => Ok(serde_json::json!({
                    "items": items,
                    "nextCursor": next_cursor,
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphListNodesPaginated",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Graph — Edges
    // =========================================================================

    /// Add or update an edge.
    #[napi(js_name = "graphAddEdge")]
    pub async fn graph_add_edge(
        &self,
        graph: String,
        src: String,
        dst: String,
        edge_type: String,
        weight: Option<f64>,
        properties: Option<serde_json::Value>,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        let props = properties
            .map(|p| js_to_value_checked(p, 0))
            .transpose()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphAddEdge {
                    branch: None,
                    graph,
                    src,
                    dst,
                    edge_type,
                    weight,
                    properties: props,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphAddEdge",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Remove an edge.
    #[napi(js_name = "graphRemoveEdge")]
    pub async fn graph_remove_edge(
        &self,
        graph: String,
        src: String,
        dst: String,
        edge_type: String,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphRemoveEdge {
                    branch: None,
                    graph,
                    src,
                    dst,
                    edge_type,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphRemoveEdge",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get neighbors of a node.
    #[napi(js_name = "graphNeighbors")]
    pub async fn graph_neighbors(
        &self,
        graph: String,
        node_id: String,
        direction: Option<String>,
        edge_type: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphNeighbors {
                    branch: None,
                    graph,
                    node_id,
                    direction,
                    edge_type,
                })
                .map_err(to_napi_err)?
            {
                Output::GraphNeighbors(neighbors) => {
                    let arr: Vec<serde_json::Value> = neighbors
                        .into_iter()
                        .map(|n| {
                            serde_json::json!({
                                "nodeId": n.node_id,
                                "edgeType": n.edge_type,
                                "weight": n.weight,
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphNeighbors",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Graph — Bulk & Traversal
    // =========================================================================

    /// Bulk insert nodes and edges into a graph.
    #[napi(js_name = "graphBulkInsert")]
    pub async fn graph_bulk_insert(
        &self,
        graph: String,
        nodes: Vec<serde_json::Value>,
        edges: Vec<serde_json::Value>,
        chunk_size: Option<u32>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let bulk_nodes: Vec<BulkGraphNode> = nodes
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let node_id = obj
                    .get("nodeId")
                    .or_else(|| obj.get("node_id"))
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'nodeId'"))?
                    .to_string();
                let entity_ref = obj
                    .get("entityRef")
                    .or_else(|| obj.get("entity_ref"))
                    .and_then(|k| k.as_str())
                    .map(|s| s.to_string());
                let properties = obj
                    .get("properties")
                    .filter(|v| !v.is_null())
                    .map(|p| js_to_value_checked(p.clone(), 0))
                    .transpose()?;
                let object_type = obj
                    .get("objectType")
                    .or_else(|| obj.get("object_type"))
                    .and_then(|k| k.as_str())
                    .map(|s| s.to_string());
                Ok(BulkGraphNode {
                    node_id,
                    entity_ref,
                    properties,
                    object_type,
                })
            })
            .collect::<napi::Result<_>>()?;
        let bulk_edges: Vec<BulkGraphEdge> = edges
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let src = obj
                    .get("src")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'src'"))?
                    .to_string();
                let dst = obj
                    .get("dst")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'dst'"))?
                    .to_string();
                let edge_type = obj
                    .get("edgeType")
                    .or_else(|| obj.get("edge_type"))
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'edgeType'"))?
                    .to_string();
                let weight = obj.get("weight").and_then(|w| w.as_f64());
                let properties = obj
                    .get("properties")
                    .filter(|v| !v.is_null())
                    .map(|p| js_to_value_checked(p.clone(), 0))
                    .transpose()?;
                Ok(BulkGraphEdge {
                    src,
                    dst,
                    edge_type,
                    weight,
                    properties,
                })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphBulkInsert {
                    branch: None,
                    graph,
                    nodes: bulk_nodes,
                    edges: bulk_edges,
                    chunk_size: chunk_size.map(|c| c as usize),
                })
                .map_err(to_napi_err)?
            {
                Output::GraphBulkInsertResult {
                    nodes_inserted,
                    edges_inserted,
                } => Ok(serde_json::json!({
                    "nodesInserted": nodes_inserted,
                    "edgesInserted": edges_inserted,
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphBulkInsert",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// BFS traversal from a start node.
    #[napi(js_name = "graphBfs")]
    pub async fn graph_bfs(
        &self,
        graph: String,
        start: String,
        max_depth: u32,
        max_nodes: Option<u32>,
        edge_types: Option<Vec<String>>,
        direction: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphBfs {
                    branch: None,
                    graph,
                    start,
                    max_depth: max_depth as usize,
                    max_nodes: max_nodes.map(|n| n as usize),
                    edge_types,
                    direction,
                })
                .map_err(to_napi_err)?
            {
                Output::GraphBfs(result) => Ok(graph_bfs_result_to_js(result)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphBfs",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Graph — Ontology
    // =========================================================================

    /// Define an object type in the graph ontology.
    #[napi(js_name = "graphDefineObjectType")]
    pub async fn graph_define_object_type(
        &self,
        graph: String,
        definition: serde_json::Value,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        let def = js_to_value_checked(definition, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphDefineObjectType {
                    branch: None,
                    graph,
                    definition: def,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphDefineObjectType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get an object type definition.
    #[napi(js_name = "graphGetObjectType")]
    pub async fn graph_get_object_type(
        &self,
        graph: String,
        name: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphGetObjectType {
                    branch: None,
                    graph,
                    name,
                })
                .map_err(to_napi_err)?
            {
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphGetObjectType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all object type names.
    #[napi(js_name = "graphListObjectTypes")]
    pub async fn graph_list_object_types(
        &self,
        graph: String,
    ) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphListObjectTypes {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphListObjectTypes",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete an object type definition.
    #[napi(js_name = "graphDeleteObjectType")]
    pub async fn graph_delete_object_type(
        &self,
        graph: String,
        name: String,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphDeleteObjectType {
                    branch: None,
                    graph,
                    name,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphDeleteObjectType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Define a link type in the graph ontology.
    #[napi(js_name = "graphDefineLinkType")]
    pub async fn graph_define_link_type(
        &self,
        graph: String,
        definition: serde_json::Value,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        let def = js_to_value_checked(definition, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphDefineLinkType {
                    branch: None,
                    graph,
                    definition: def,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphDefineLinkType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a link type definition.
    #[napi(js_name = "graphGetLinkType")]
    pub async fn graph_get_link_type(
        &self,
        graph: String,
        name: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphGetLinkType {
                    branch: None,
                    graph,
                    name,
                })
                .map_err(to_napi_err)?
            {
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphGetLinkType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all link type names.
    #[napi(js_name = "graphListLinkTypes")]
    pub async fn graph_list_link_types(
        &self,
        graph: String,
    ) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphListLinkTypes {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphListLinkTypes",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a link type definition.
    #[napi(js_name = "graphDeleteLinkType")]
    pub async fn graph_delete_link_type(
        &self,
        graph: String,
        name: String,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphDeleteLinkType {
                    branch: None,
                    graph,
                    name,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphDeleteLinkType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Freeze the graph ontology (no more type changes).
    #[napi(js_name = "graphFreezeOntology")]
    pub async fn graph_freeze_ontology(&self, graph: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphFreezeOntology {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphFreezeOntology",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get the ontology status of a graph.
    #[napi(js_name = "graphOntologyStatus")]
    pub async fn graph_ontology_status(
        &self,
        graph: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphOntologyStatus {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphOntologyStatus",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a complete ontology summary.
    #[napi(js_name = "graphOntologySummary")]
    pub async fn graph_ontology_summary(
        &self,
        graph: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphOntologySummary {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphOntologySummary",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all ontology types (both object and link types).
    #[napi(js_name = "graphListOntologyTypes")]
    pub async fn graph_list_ontology_types(
        &self,
        graph: String,
    ) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphListOntologyTypes {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphListOntologyTypes",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get all node IDs of a given object type.
    #[napi(js_name = "graphNodesByType")]
    pub async fn graph_nodes_by_type(
        &self,
        graph: String,
        object_type: String,
    ) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphNodesByType {
                    branch: None,
                    graph,
                    object_type,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphNodesByType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Graph — Analytics
    // =========================================================================

    /// Weakly Connected Components.
    #[napi(js_name = "graphWcc")]
    pub async fn graph_wcc(&self, graph: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphWcc {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::GraphAnalyticsU64(result) => {
                    Ok(graph_analytics_u64_to_js(result))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphWcc",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Community Detection via Label Propagation.
    #[napi(js_name = "graphCdlp")]
    pub async fn graph_cdlp(
        &self,
        graph: String,
        max_iterations: u32,
        direction: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphCdlp {
                    branch: None,
                    graph,
                    max_iterations: max_iterations as usize,
                    direction,
                })
                .map_err(to_napi_err)?
            {
                Output::GraphAnalyticsU64(result) => {
                    Ok(graph_analytics_u64_to_js(result))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphCdlp",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// PageRank importance scoring.
    #[napi(js_name = "graphPagerank")]
    pub async fn graph_pagerank(
        &self,
        graph: String,
        damping: Option<f64>,
        max_iterations: Option<u32>,
        tolerance: Option<f64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphPagerank {
                    branch: None,
                    graph,
                    damping,
                    max_iterations: max_iterations.map(|m| m as usize),
                    tolerance,
                })
                .map_err(to_napi_err)?
            {
                Output::GraphAnalyticsF64(result) => {
                    Ok(graph_analytics_f64_to_js(result))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphPagerank",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Local Clustering Coefficient.
    #[napi(js_name = "graphLcc")]
    pub async fn graph_lcc(&self, graph: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphLcc {
                    branch: None,
                    graph,
                })
                .map_err(to_napi_err)?
            {
                Output::GraphAnalyticsF64(result) => {
                    Ok(graph_analytics_f64_to_js(result))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphLcc",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Single-Source Shortest Path (Dijkstra).
    #[napi(js_name = "graphSssp")]
    pub async fn graph_sssp(
        &self,
        graph: String,
        source: String,
        direction: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::GraphSssp {
                    branch: None,
                    graph,
                    source,
                    direction,
                })
                .map_err(to_napi_err)?
            {
                Output::GraphAnalyticsF64(result) => {
                    Ok(graph_analytics_f64_to_js(result))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for GraphSssp",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }
}

// ---------------------------------------------------------------------------
// Batch result helpers
// ---------------------------------------------------------------------------

fn batch_results_to_js(results: Vec<BatchItemResult>) -> serde_json::Value {
    let arr: Vec<serde_json::Value> = results
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "version": r.version.map(|v| v as i64),
                "error": r.error,
            })
        })
        .collect();
    serde_json::Value::Array(arr)
}

fn batch_get_results_to_js(results: Vec<BatchGetItemResult>) -> serde_json::Value {
    let arr: Vec<serde_json::Value> = results
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "value": r.value.map(value_to_js),
                "version": r.version.map(|v| v as i64),
                "timestamp": r.timestamp.map(|t| t as i64),
                "error": r.error,
            })
        })
        .collect();
    serde_json::Value::Array(arr)
}

// ---------------------------------------------------------------------------
// Top-level functions
// ---------------------------------------------------------------------------

/// Download model files for auto-embedding.
#[napi]
pub fn setup() -> napi::Result<String> {
    #[cfg(feature = "embed")]
    {
        let path = strata_intelligence::embed::download::ensure_model()
            .map_err(napi::Error::from_reason)?;
        Ok(path.to_string_lossy().into_owned())
    }

    #[cfg(not(feature = "embed"))]
    {
        Err(napi::Error::from_reason(
            "The 'embed' feature is not enabled in this build",
        ))
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers (free functions)
// ---------------------------------------------------------------------------

fn collection_info_to_js(c: CollectionInfo) -> serde_json::Value {
    serde_json::json!({
        "name": c.name,
        "dimension": c.dimension,
        "metric": format!("{:?}", c.metric).to_lowercase(),
        "count": c.count,
        "indexType": c.index_type,
        "memoryBytes": c.memory_bytes,
    })
}

fn versioned_branch_info_to_js(info: VersionedBranchInfo) -> serde_json::Value {
    serde_json::json!({
        "id": info.info.id.as_str(),
        "status": format!("{:?}", info.info.status).to_lowercase(),
        "createdAt": info.info.created_at,
        "updatedAt": info.info.updated_at,
        "parentId": info.info.parent_id.map(|p| p.as_str().to_string()),
        "version": info.version,
        "timestamp": info.timestamp,
    })
}

fn branch_export_result_to_js(r: BranchExportResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "path": r.path,
        "entryCount": r.entry_count,
        "bundleSize": r.bundle_size,
    })
}

fn branch_import_result_to_js(r: BranchImportResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "transactionsApplied": r.transactions_applied,
        "keysWritten": r.keys_written,
    })
}

fn bundle_validate_result_to_js(r: BundleValidateResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "formatVersion": r.format_version,
        "entryCount": r.entry_count,
        "checksumsValid": r.checksums_valid,
    })
}

fn graph_bfs_result_to_js(r: GraphBfsResult) -> serde_json::Value {
    let edges: Vec<serde_json::Value> = r
        .edges
        .into_iter()
        .map(|(src, dst, edge_type)| {
            serde_json::json!({ "src": src, "dst": dst, "edgeType": edge_type })
        })
        .collect();
    let depths: serde_json::Map<String, serde_json::Value> = r
        .depths
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::Number((v as u64).into())))
        .collect();
    serde_json::json!({
        "visited": r.visited,
        "depths": depths,
        "edges": edges,
    })
}

fn graph_analytics_u64_to_js(r: GraphAnalyticsU64Result) -> serde_json::Value {
    let result: serde_json::Map<String, serde_json::Value> = r
        .result
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::Number(v.into())))
        .collect();
    serde_json::json!({
        "algorithm": r.algorithm,
        "result": result,
    })
}

fn graph_analytics_f64_to_js(r: GraphAnalyticsF64Result) -> serde_json::Value {
    let result: serde_json::Map<String, serde_json::Value> = r
        .result
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                serde_json::Number::from_f64(v)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    serde_json::json!({
        "algorithm": r.algorithm,
        "result": result,
        "iterations": r.iterations,
    })
}
