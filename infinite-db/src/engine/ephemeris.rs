//! Ephemeris append and query helpers.

use crate::infinitedb_core::{
    address::RevisionId,
    ephemeris::{
        ephemeris_entry_to_hyperedge, hyperedge_to_ephemeris_entry, EphemerisEntry, WandererId,
        EPHEMERIS_SPACE,
    },
    hyperedge::HyperedgeId,
    void::VoidOr,
};

use super::hypergraph::{prepare_assertion_write, HypergraphWriteRow};

pub fn prepare_ephemeris_write(entry: &EphemerisEntry, edge_id: HyperedgeId) -> std::io::Result<HypergraphWriteRow> {
    let edge = ephemeris_entry_to_hyperedge(entry, edge_id);
    prepare_assertion_write(EPHEMERIS_SPACE, &edge)
}

pub fn decode_ephemeris_entries(edges: &[crate::infinitedb_core::hyperedge::Hyperedge]) -> Vec<EphemerisEntry> {
    edges
        .iter()
        .filter_map(hyperedge_to_ephemeris_entry)
        .collect()
}

pub fn wanderer_presence(
    entries: &[EphemerisEntry],
    wanderer: WandererId,
    as_of: RevisionId,
) -> VoidOr<EphemerisEntry> {
    let mut latest: Option<EphemerisEntry> = None;
    for e in entries {
        if e.wanderer != wanderer || e.stamp > as_of {
            continue;
        }
        if latest.as_ref().map(|l| e.stamp > l.stamp).unwrap_or(true) {
            latest = Some(e.clone());
        }
    }
    match latest {
        None => VoidOr::Void,
        Some(e) => VoidOr::Known(e),
    }
}
