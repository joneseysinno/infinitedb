//! SPACE_TOWER integration tests (Waves 1–3).

use infinite_db::infinitedb_core::{
    address::{DimensionVector, SpaceId},
    placement::{bbox_to_child, extent_in_parent, Placement},
    space::{CenterReservation, SpaceConfig},
};
use infinite_db::infinitedb_index::hilbert;
use infinite_db::infinitedb_server::api::{handle_request, Request, Response};
use infinite_db::infinitedb_server::session::{AccessLevel, Session, SpaceGrant};
use infinite_db::InfiniteDb;
use std::time::Instant;
use tempfile::TempDir;

fn admin_session(spaces: &[SpaceId]) -> Session {
    Session::new(
        infinite_db::infinitedb_core::branch::BranchId::MAIN,
        infinite_db::infinitedb_core::snapshot::SnapshotId(1),
        infinite_db::infinitedb_core::address::RevisionId::legacy(0),
        spaces
            .iter()
            .map(|&s| SpaceGrant {
                space: s,
                level: AccessLevel::Admin,
            })
            .collect(),
    )
}

#[test]
fn wave1_registry_tree_and_catalog() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "site", 3))
        .unwrap();
    let building_p = Placement::axis_aligned(vec![0, 0, 0], 1, 1, vec![128, 128, 128]);
    db.register_space(
        SpaceConfig::new(SpaceId(2), "building", 3)
            .with_parent(SpaceId(1))
            .with_placement(building_p),
    )
    .unwrap();
    let detail_p = Placement {
        offset: vec![10, 10],
        scale_num: vec![1, 1],
        scale_den: vec![1, 1],
        extent: vec![64, 64],
        fixed_axes: vec![(2, 0)],
    };
    db.register_space(
        SpaceConfig::new(SpaceId(3), "detail", 2)
            .with_parent(SpaceId(2))
            .with_placement(detail_p),
    )
    .unwrap();

    assert_eq!(db.list_children(SpaceId(1)), vec![SpaceId(2)]);
    let subtree = db.get_subtree(SpaceId(1));
    assert_eq!(subtree.len(), 3);
    assert_eq!(subtree[0].0, SpaceId(1));
    assert_eq!(subtree[1].0, SpaceId(2));

    let session = admin_session(&[SpaceId(1), SpaceId(2), SpaceId(3)]);
    match handle_request(
        &db,
        &session,
        Request::ListChildren { parent: SpaceId(1) },
    ) {
        Response::SpaceConfigs(configs) => assert_eq!(configs.len(), 1),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn wave2_precision_ceiling_rejects_33_bits() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let err = db.register_space(
        SpaceConfig::new(SpaceId(1), "big", 2).with_bits_per_dim(33),
    );
    assert!(err.is_err());
}

#[test]
fn wave2_coordinate_overflow_rejected_before_write() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "s", 1).with_bits_per_dim(4))
        .unwrap();
    let err = db.insert(SpaceId(1), DimensionVector::new(vec![20]), vec![1]);
    assert!(err.is_err());
}

#[test]
fn wave3_register_or_get_racing_id() {
    let dir = TempDir::new().unwrap();
    let db = std::sync::Arc::new(InfiniteDb::open(dir.path()).unwrap());
    let cfg = SpaceConfig::new(SpaceId(100), "race", 2);
    let mut handles = vec![];
    for _ in 0..4 {
        let db = std::sync::Arc::clone(&db);
        let cfg = cfg.clone();
        handles.push(std::thread::spawn(move || db.register_or_get_space(cfg).unwrap()));
    }
    let ids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(ids.iter().all(|&id| id == ids[0]));
}

#[test]
fn wave3_density_tracks_writes() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "s", 2).with_bits_per_dim(8))
        .unwrap();
    db.insert(
        SpaceId(1),
        DimensionVector::new(hilbert::decode(0x8000, 2, 8)),
        vec![1],
    )
    .unwrap();
    db.insert(
        SpaceId(1),
        DimensionVector::new(hilbert::decode(0xFFFF, 2, 8)),
        vec![2],
    )
    .unwrap();
    let d = db.space_density(SpaceId(1)).known().expect("observed");
    assert_eq!(d.record_count, 2);
    assert_eq!(d.max_occupied_depth, 1);
}

#[test]
fn wave3_density_durable_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let before;
    {
        let db = InfiniteDb::open(&path).unwrap();
        db.register_space(SpaceConfig::new(SpaceId(1), "s", 2).with_bits_per_dim(8))
            .unwrap();
        db.insert(SpaceId(1), DimensionVector::new(vec![0, 0]), vec![1])
            .unwrap();
        db.insert(SpaceId(1), DimensionVector::new(vec![200, 200]), vec![2])
            .unwrap();
        db.sync().unwrap();
        before = db.space_density(SpaceId(1));
    }
    let db = InfiniteDb::open(&path).unwrap();
    assert_eq!(db.space_density(SpaceId(1)), before);
}

#[test]
fn wave3_mirror_structural_only_interlock() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(1), "parent", 2)
            .with_bits_per_dim(8)
            .with_center_reservation(CenterReservation::StructuralOnly),
    )
    .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(2), "child", 2)
            .with_parent(SpaceId(1))
            .with_placement(Placement {
                offset: vec![10, 10],
                scale_num: vec![1, 1],
                scale_den: vec![1, 1],
                extent: vec![64, 64],
                fixed_axes: vec![],
            }),
    )
    .unwrap();
    db.sync().unwrap();
    let mirrors = db
        .query(SpaceId(1), None)
        .unwrap()
        .into_iter()
        .filter(|r| !r.tombstone)
        .count();
    assert_eq!(mirrors, 1);
}

fn bbox_intersects_extents(
    bbox_min: &[u32],
    bbox_max: &[u32],
    ext_min: &[i64],
    ext_max: &[i64],
) -> bool {
    for i in 0..bbox_min.len().min(ext_min.len()) {
        if (bbox_max[i] as i64) < ext_min[i] || (bbox_min[i] as i64) > ext_max[i] {
            return false;
        }
    }
    true
}

fn record_site_point(
    db: &InfiniteDb,
    site: SpaceId,
    space: SpaceId,
    pt: &[u32],
) -> Vec<u32> {
    match (site.0, space.0) {
        // Fixture-specific transforms (site=1, sheet=4, east=3).
        (1, 4) => vec![10 + pt[0], 10 + pt[1], 0],
        (1, 3) => vec![128 + pt[0], pt[1], pt[2]],
        _ => {
            let subtree: std::collections::HashMap<_, _> =
                db.get_subtree(site).into_iter().collect();
            let mut path = Vec::new();
            let mut current = space;
            while current != site {
                let cfg = subtree.get(&current).expect("space in subtree");
                path.push(cfg.placement.clone().expect("placement"));
                current = cfg.parent.expect("parent");
            }
            infinite_db::infinitedb_index::placement::to_ancestor(pt, &path)
                .expect("to_ancestor")
        }
    }
}

fn brute_force_site_query(
    db: &InfiniteDb,
    site: SpaceId,
    bbox_min: &[u32],
    bbox_max: &[u32],
) -> Vec<infinite_db::infinitedb_core::block::Record> {
    let mut out = Vec::new();
    for (space, _) in db.get_subtree(site) {
        if !db.list_children(space).is_empty() {
            continue;
        }
        let recs = db.query(space, None).unwrap();
        for rec in recs {
            let parent = record_site_point(db, site, space, &rec.address.point.coords);
            let inside = parent
                .iter()
                .zip(bbox_min.iter().zip(bbox_max.iter()))
                .all(|(&c, (&lo, &hi))| c >= lo && c <= hi);
            if inside {
                out.push(rec);
            }
        }
    }
    out
}

fn clip_bbox_to_extents(
    min: &[u32],
    max: &[u32],
    ext_min: &[i64],
    ext_max: &[i64],
) -> Option<(Vec<u32>, Vec<u32>)> {
    let n = min.len().min(ext_min.len());
    let mut out_min = Vec::with_capacity(n);
    let mut out_max = Vec::with_capacity(n);
    for i in 0..n {
        let lo = min[i].max(ext_min[i].max(0) as u32);
        let hi = max[i].min(ext_max[i].max(0) as u32);
        if lo > hi {
            return None;
        }
        out_min.push(lo);
        out_max.push(hi);
    }
    Some((out_min, out_max))
}

fn catalog_descent_query(
    db: &InfiniteDb,
    root: SpaceId,
    bbox_min: &[u32],
    bbox_max: &[u32],
) -> (Vec<infinite_db::infinitedb_core::block::Record>, u64) {
    let subtree: std::collections::HashMap<_, _> =
        db.get_subtree(root).into_iter().collect();
    let mut trips = 0u64;
    fn descend(
        db: &InfiniteDb,
        subtree: &std::collections::HashMap<SpaceId, SpaceConfig>,
        space: SpaceId,
        min: &[u32],
        max: &[u32],
        trips: &mut u64,
        out: &mut Vec<infinite_db::infinitedb_core::block::Record>,
    ) {
        *trips += 1;
        let children = db.list_children(space);
        *trips += 1;
        if children.is_empty() {
            let recs = db.query(space, None).unwrap();
            for rec in recs {
                if rec.address.point.coords.len() == min.len()
                    && rec
                        .address
                        .point
                        .coords
                        .iter()
                        .zip(min.iter().zip(max.iter()))
                        .all(|(&c, (&lo, &hi))| c >= lo && c <= hi)
                {
                    out.push(rec);
                }
            }
            return;
        }
        for child_id in children {
            let Some(child_cfg) = subtree.get(&child_id) else {
                continue;
            };
            let placement = child_cfg.placement.as_ref().unwrap();
            let (ext_min, ext_max) = extent_in_parent(placement);
            if !bbox_intersects_extents(min, max, &ext_min, &ext_max) {
                continue;
            }
            let Some((clipped_min, clipped_max)) = clip_bbox_to_extents(min, max, &ext_min, &ext_max)
            else {
                continue;
            };
            let Some((child_min, child_max)) =
                bbox_to_child(placement, &clipped_min, &clipped_max)
            else {
                continue;
            };
            descend(
                db,
                subtree,
                child_id,
                &child_min,
                &child_max,
                trips,
                out,
            );
        }
    }
    let mut out = Vec::new();
    descend(
        db,
        &subtree,
        root,
        bbox_min,
        bbox_max,
        &mut trips,
        &mut out,
    );
    (out, trips)
}

/// T10 descent conformance fixture — catalog BVH descent with baseline.
#[test]
fn wave3_descent_conformance_fixture() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "site", 3))
        .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(2), "west", 3)
            .with_parent(SpaceId(1))
            .with_placement(Placement::axis_aligned(
                vec![0, 0, 0],
                1,
                1,
                vec![128, 128, 128],
            )),
    )
    .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(3), "east", 3)
            .with_parent(SpaceId(1))
            .with_placement(Placement::axis_aligned(
                vec![128, 0, 0],
                1,
                1,
                vec![127, 128, 128],
            )),
    )
    .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(4), "sheet", 2)
            .with_parent(SpaceId(2))
            .with_placement(Placement {
                offset: vec![10, 10],
                scale_num: vec![1, 1],
                scale_den: vec![1, 1],
                extent: vec![64, 64],
                fixed_axes: vec![(2, 0)],
            }),
    )
    .unwrap();

    db.insert(SpaceId(4), DimensionVector::new(vec![5, 5]), b"a".to_vec())
        .unwrap();
    db.insert(SpaceId(3), DimensionVector::new(vec![64, 64, 64]), b"b".to_vec())
        .unwrap();
    db.sync().unwrap();

    let west_bbox_min = vec![0, 0, 0];
    let west_bbox_max = vec![127, 127, 127];
    let t0 = Instant::now();
    let (descent_hits, descent_trips) =
        catalog_descent_query(&db, SpaceId(1), &west_bbox_min, &west_bbox_max);
    let descent_elapsed = t0.elapsed();
    let t1 = Instant::now();
    let brute_hits = brute_force_site_query(&db, SpaceId(1), &west_bbox_min, &west_bbox_max);
    let brute_elapsed = t1.elapsed();

    assert_eq!(descent_hits.len(), 1);
    assert_eq!(brute_hits.len(), 1);
    assert_eq!(
        descent_hits[0].data,
        b"a",
        "west-only bbox must hit sheet child, not east"
    );

    let sheet_z0_min = vec![0, 0, 0];
    let sheet_z0_max = vec![127, 127, 0];
    let z0 = catalog_descent_query(&db, SpaceId(1), &sheet_z0_min, &sheet_z0_max).0;
    assert_eq!(z0.len(), 1);
    let z0_brute = brute_force_site_query(&db, SpaceId(1), &sheet_z0_min, &sheet_z0_max);
    assert_eq!(z0.len(), z0_brute.len());
    let z1 = catalog_descent_query(&db, SpaceId(1), &[0, 0, 1], &[255, 255, 1]).0;
    assert_eq!(z1.len(), 0);

    let idx1 = hilbert::encode(&[127, 0], 8);
    let idx2 = hilbert::encode(&[128, 0], 8);
    let d = 1u128 << 16;
    assert!(idx1.abs_diff(idx2) >= d / 4);
    db.register_space(SpaceConfig::new(SpaceId(10), "jump", 2).with_bits_per_dim(8))
        .unwrap();
    db.insert(SpaceId(10), DimensionVector::new(vec![127, 0]), b"p1".to_vec())
        .unwrap();
    db.insert(SpaceId(10), DimensionVector::new(vec![128, 0]), b"p2".to_vec())
        .unwrap();
    db.sync().unwrap();
    let jump = db
        .query_bbox(
            SpaceId(10),
            DimensionVector::new(vec![127, 0]),
            DimensionVector::new(vec![128, 0]),
            None,
        )
        .unwrap();
    assert_eq!(jump.len(), 2);

    eprintln!(
        "G-NATIVE-DESCENT baseline: descent_trips={descent_trips} descent_ms={} brute_ms={}",
        descent_elapsed.as_millis(),
        brute_elapsed.as_millis()
    );
}

/// T12 — cross-space flow vector composed at site ancestor.
#[test]
fn wave4_cross_space_flow_vector() {
    use infinite_db::infinitedb_core::hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    };
    use std::collections::BTreeMap;

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let edge_space = SpaceId(100);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "site", 2))
        .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(2), "west", 2)
            .with_parent(SpaceId(1))
            .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![128, 128])),
    )
    .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(3), "east", 2)
            .with_parent(SpaceId(1))
            .with_placement(Placement::axis_aligned(vec![64, 0], 1, 1, vec![64, 128])),
    )
    .unwrap();

    fn ep(space: SpaceId, x: u32, y: u32, pol: EndpointPolarity) -> EndpointRef {
        EndpointRef::new(
            EndpointRole::new("n"),
            space,
            DimensionVector::new(vec![x, y]),
        )
        .with_polarity(pol)
    }

    let edge = Hyperedge {
        id: HyperedgeId(1),
        kind: HyperedgeKind::new("flow"),
        endpoints: vec![
            ep(SpaceId(2), 10, 5, EndpointPolarity::Tail),
            ep(SpaceId(3), 5, 5, EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    };
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();

    let rec = db
        .query_flow_vector_for_edge(edge_space, HyperedgeId(1), None)
        .unwrap()
        .expect("cross-space vector indexed");
    assert_eq!(rec.vector.space, SpaceId(1));
    assert_eq!(rec.vector.delta, vec![59, 0]);
}

/// Same-space flow vectors unchanged (T12 regression).
#[test]
fn wave4_same_space_flow_vector_regression() {
    use infinite_db::infinitedb_core::hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    };
    use std::collections::BTreeMap;

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let edge_space = SpaceId(10);
    let entity = SpaceId(20);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(entity, "entity", 2))
        .unwrap();

    let edge = Hyperedge {
        id: HyperedgeId(7),
        kind: HyperedgeKind::new("flow"),
        endpoints: vec![
            EndpointRef::new(
                EndpointRole::new("n"),
                entity,
                DimensionVector::new(vec![0, 0]),
            )
            .with_polarity(EndpointPolarity::Tail),
            EndpointRef::new(
                EndpointRole::new("n"),
                entity,
                DimensionVector::new(vec![0, 8]),
            )
            .with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    };
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    let rec = db
        .query_flow_vector_for_edge(edge_space, HyperedgeId(7), None)
        .unwrap()
        .unwrap();
    assert_eq!(rec.vector.space, entity);
    assert_eq!(rec.vector.delta, vec![0, 8]);
}

/// Disjoint forests produce no flow vector (T12).
#[test]
fn wave4_no_common_ancestor_no_vector() {
    use infinite_db::infinitedb_core::hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    };
    use std::collections::BTreeMap;

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let edge_space = SpaceId(10);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "a", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(SpaceId(2), "b", 2))
        .unwrap();

    let edge = Hyperedge {
        id: HyperedgeId(99),
        kind: HyperedgeKind::new("flow"),
        endpoints: vec![
            EndpointRef::new(
                EndpointRole::new("n"),
                SpaceId(1),
                DimensionVector::new(vec![0, 0]),
            )
            .with_polarity(EndpointPolarity::Tail),
            EndpointRef::new(
                EndpointRole::new("n"),
                SpaceId(2),
                DimensionVector::new(vec![1, 0]),
            )
            .with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    };
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    assert!(
        db.query_flow_vector_for_edge(edge_space, HyperedgeId(99), None)
            .unwrap()
            .is_none()
    );
}
