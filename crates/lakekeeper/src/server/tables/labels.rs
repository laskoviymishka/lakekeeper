//! Table-scoped label CRUD handlers.
//!
//! Labels are catalog-scoped key/value annotations on tables and columns. They do NOT
//! modify `metadata.json`, do NOT create snapshots, and are not part of Iceberg table
//! state — they are API-layer enrichment served by the catalog.
//!
//! Storage in this V1 PoC: a process-local `RwLock<HashMap<...>>`. A productionization
//! pass replaces this with a Postgres-backed labels table (one row per (table_id, key)
//! plus a column-labels join). The wire shape stays the same; only the persistence
//! layer changes.
//!
//! Spec reference: `projects/iceberg-governance/labels/labels-crud-followup.md`.

use std::{
    collections::HashMap,
    sync::{LazyLock, RwLock},
};

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::{ColumnLabels, ErrorModel, Labels, LabelEntry, UpdateLabelsRequest};

use crate::{
    api::{ApiContext, Result, iceberg::v1::TableParameters},
    request_metadata::RequestMetadata,
    service::{Authorizer, CatalogStore, SecretStore, State},
};

/// Catalog-managed (read-only) label keys.
///
/// Writes targeting any of these keys cause the entire `UpdateLabels` request to be
/// rejected with `403 LabelKeyNotWritable` before any mutation. A real catalog computes
/// this set from its own policy table; the PoC hardcodes the canonical examples from
/// the spec.
const READ_ONLY_KEYS: &[&str] = &[
    "last-accessed-at",
    "last-edit-by",
    "query-count-24h",
    "sla-latency-p99",
    "migration-timestamp",
];

/// PoC in-memory store keyed by `TableIdent`. Productionization swaps this for the
/// catalog's persistent storage layer.
static LABEL_STORE: LazyLock<RwLock<HashMap<TableIdent, Labels>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

fn snapshot_labels(table: &TableIdent) -> Labels {
    LABEL_STORE
        .read()
        .expect("label store poisoned")
        .get(table)
        .cloned()
        .unwrap_or_default()
}

/// Build the spec-conformant `403 LabelKeyNotWritable` error.
///
/// `ErrorModel` is Lakekeeper's existing IRC error wrapper; the surrounding response
/// layer maps `code = 403` to an HTTP 403 with the wrapped JSON body. Spec error
/// envelope is `{ error: { type, message, key } }` — the `key` is conveyed through
/// the message text plus a typed marker in `stack` so engines that need it can parse.
fn label_key_not_writable(key: &str) -> ErrorModel {
    ErrorModel::builder()
        .message(format!(
            "Label key '{key}' is catalog-managed and cannot be written via the API."
        ))
        .r#type("LabelKeyNotWritable")
        .code(403)
        .stack(vec![format!("key={key}")])
        .build()
}

fn assert_writable(entries: &[LabelEntry]) -> Result<()> {
    for entry in entries {
        if READ_ONLY_KEYS.contains(&entry.key.as_str()) {
            return Err(label_key_not_writable(&entry.key).into());
        }
    }
    Ok(())
}

fn upsert_column<'a>(
    column_labels: &'a mut Vec<ColumnLabels>,
    field_id: i32,
) -> &'a mut ColumnLabels {
    let pos = column_labels.iter().position(|c| c.field_id == field_id);
    match pos {
        Some(i) => &mut column_labels[i],
        None => {
            column_labels.push(ColumnLabels {
                field_id,
                labels: HashMap::new(),
            });
            column_labels.last_mut().expect("just pushed")
        }
    }
}

fn apply_request(table: &TableIdent, request: &UpdateLabelsRequest) -> Labels {
    let mut store = LABEL_STORE.write().expect("label store poisoned");
    let entry = store.entry(table.clone()).or_default();

    // Updates first, then removals. Within a request the catalog already validated
    // no overlap between updates and removals (engines build clean requests; if a
    // client mixes them we apply update-then-remove which removes wins — but that
    // is a malformed request the spec disallows).
    for upd in &request.updates {
        let value = upd
            .value
            .clone()
            .unwrap_or_default();
        match upd.field_id {
            None => {
                entry.labels.insert(upd.key.clone(), value);
            }
            Some(field_id) => {
                let col = upsert_column(&mut entry.column_labels, field_id);
                col.labels.insert(upd.key.clone(), value);
            }
        }
    }

    for rem in &request.removals {
        match rem.field_id {
            None => {
                entry.labels.remove(&rem.key);
            }
            Some(field_id) => {
                if let Some(pos) = entry
                    .column_labels
                    .iter()
                    .position(|c| c.field_id == field_id)
                {
                    entry.column_labels[pos].labels.remove(&rem.key);
                    if entry.column_labels[pos].labels.is_empty() {
                        entry.column_labels.remove(pos);
                    }
                }
            }
        }
    }

    entry.clone()
}

/// Read the current label set for a table. Read-only operation; no authorization
/// gate added in V1 — production-grade catalogs add the same `LoadTable`-style
/// authz check here.
pub(super) async fn load_labels<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: TableParameters,
    _state: ApiContext<State<A, C, S>>,
    _request_metadata: RequestMetadata,
) -> Result<Labels> {
    let TableParameters { prefix: _, table } = parameters;
    Ok(snapshot_labels(&table))
}

/// Apply label updates and removals atomically. Catalog-managed (read-only) keys
/// are rejected with `403 LabelKeyNotWritable` before any mutation.
pub(super) async fn update_labels<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: TableParameters,
    request: UpdateLabelsRequest,
    _state: ApiContext<State<A, C, S>>,
    _request_metadata: RequestMetadata,
) -> Result<Labels> {
    let TableParameters { prefix: _, table } = parameters;

    // Atomic-check-before-mutate: validate the full request against the read-only
    // policy first; only mutate if every entry passes.
    assert_writable(&request.updates)?;
    assert_writable(&request.removals)?;

    Ok(apply_request(&table, &request))
}

