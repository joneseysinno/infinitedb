//! Versioned hyperedge payload codec.
//!
//! V1 (legacy): untagged bincode of the pre-M1 struct (no polarity, no directionality).
//! V2: leading [`HYPEREDGE_PAYLOAD_V2_TAG`] byte followed by tagged body.
//! V3 (M5): leading [`HYPEREDGE_PAYLOAD_V3_TAG`] — same body as V2 with optional authoring provenance.
//! V4 (M7): leading [`HYPEREDGE_PAYLOAD_V4_TAG`] — optional computation provenance (may coexist with V3 fields).

use std::collections::BTreeMap;
use std::io;

use bincode::{config::standard, decode_from_slice, encode_to_vec, Decode, Encode};

use super::address::{DimensionVector, RevisionId, SpaceId};
use super::hyperedge::{
    Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
    HyperedgeKind,
};

/// Version tag prefix for V2 hyperedge payloads. Never written for V1.
pub const HYPEREDGE_PAYLOAD_V2_TAG: u8 = 0xE2;
/// Version tag prefix for V3 hyperedge payloads (authoring provenance).
pub const HYPEREDGE_PAYLOAD_V3_TAG: u8 = 0xE3;
/// Version tag prefix for V4 hyperedge payloads (computation provenance).
pub const HYPEREDGE_PAYLOAD_V4_TAG: u8 = 0xE4;

/// Encode a hyperedge payload (V4 when computation present, else V3/V2).
pub fn encode_hyperedge(edge: &Hyperedge) -> Result<Vec<u8>, io::Error> {
    let tag = if edge.computation.is_some() {
        HYPEREDGE_PAYLOAD_V4_TAG
    } else if edge.authoring_frame.is_some() {
        HYPEREDGE_PAYLOAD_V3_TAG
    } else {
        HYPEREDGE_PAYLOAD_V2_TAG
    };
    let body = encode_to_vec(edge, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(tag);
    out.extend(body);
    Ok(out)
}

/// Decode a hyperedge payload (V1–V4).
pub fn decode_hyperedge(data: &[u8]) -> Result<Hyperedge, io::Error> {
    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty hyperedge payload",
        ));
    }
    if data[0] == HYPEREDGE_PAYLOAD_V4_TAG
        || data[0] == HYPEREDGE_PAYLOAD_V3_TAG
        || data[0] == HYPEREDGE_PAYLOAD_V2_TAG
    {
        let (edge, consumed) = decode_from_slice::<Hyperedge, _>(&data[1..], standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if consumed != data.len() - 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing bytes in hyperedge payload",
            ));
        }
        return Ok(edge);
    }
    decode_v1(data)
}

fn decode_v1(data: &[u8]) -> Result<Hyperedge, io::Error> {
    let (legacy, consumed) = decode_from_slice::<HyperedgeV1, _>(data, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if consumed != data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes in V1 hyperedge payload",
        ));
    }
    Ok(legacy.into_v2())
}

/// Pre-M1 wire shape (decode-only).
#[derive(Debug, Clone, Encode, Decode)]
struct HyperedgeV1 {
    id: HyperedgeId,
    kind: HyperedgeKind,
    endpoints: Vec<EndpointRefV1>,
    weight_milli: Option<i64>,
    metadata: BTreeMap<String, String>,
    valid_from: RevisionId,
    valid_to: Option<RevisionId>,
}

#[derive(Debug, Clone, Encode, Decode)]
struct EndpointRefV1 {
    role: EndpointRole,
    space: SpaceId,
    node: DimensionVector,
}

impl HyperedgeV1 {
    fn into_v2(self) -> Hyperedge {
        Hyperedge {
            id: self.id,
            kind: self.kind,
            endpoints: self
                .endpoints
                .into_iter()
                .map(|ep| EndpointRef {
                    role: ep.role,
                    space: ep.space,
                    node: ep.node,
                    polarity: EndpointPolarity::Neutral,
                })
                .collect(),
            weight_milli: self.weight_milli,
            metadata: self.metadata,
            valid_from: self.valid_from,
            valid_to: self.valid_to,
            directionality: Directionality::Undirected,
            authoring_frame: None,
            computation: None,
        }
    }
}

/// Encode a V1 fixture (for tests and migration fixtures — production never writes V1).
pub fn encode_hyperedge_v1_fixture(edge: &HyperedgeV1Fixture) -> Vec<u8> {
    encode_to_vec(edge, standard()).expect("V1 fixture must encode")
}

/// Test/migration helper matching the V1 wire struct.
#[derive(Debug, Clone, Encode, Decode)]
pub struct HyperedgeV1Fixture {
    pub id: HyperedgeId,
    pub kind: HyperedgeKind,
    pub endpoints: Vec<EndpointRefV1Fixture>,
    pub weight_milli: Option<i64>,
    pub metadata: BTreeMap<String, String>,
    pub valid_from: RevisionId,
    pub valid_to: Option<RevisionId>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct EndpointRefV1Fixture {
    pub role: EndpointRole,
    pub space: SpaceId,
    pub node: DimensionVector,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::computation::ComputationProvenance;
    use crate::infinitedb_core::judgment::{SubjectIdentity, SubjectKind, SubjectPin};
    use crate::infinitedb_core::provenance::{AuthoringFrameProvenance, FrameId};

    fn sample_v2_edge() -> Hyperedge {
        Hyperedge {
            id: HyperedgeId(99),
            kind: HyperedgeKind::new("beam.bears_on"),
            endpoints: vec![
                EndpointRef::new(
                    EndpointRole::new("support"),
                    SpaceId(1),
                    DimensionVector::new(vec![1, 2]),
                )
                .with_polarity(EndpointPolarity::Tail),
                EndpointRef::new(
                    EndpointRole::new("beam"),
                    SpaceId(1),
                    DimensionVector::new(vec![3, 4]),
                )
                .with_polarity(EndpointPolarity::Head),
            ],
            weight_milli: Some(1000),
            metadata: BTreeMap::from([("src".into(), "ifc".into())]),
            valid_from: RevisionId(5),
            valid_to: Some(RevisionId(10)),
            directionality: Directionality::Directed,
            authoring_frame: None,
            computation: None,
        }
    }

    fn sample_v1_fixture() -> HyperedgeV1Fixture {
        HyperedgeV1Fixture {
            id: HyperedgeId(42),
            kind: HyperedgeKind::new("link"),
            endpoints: vec![
                EndpointRefV1Fixture {
                    role: EndpointRole::new("a"),
                    space: SpaceId(1),
                    node: DimensionVector::new(vec![0, 0]),
                },
                EndpointRefV1Fixture {
                    role: EndpointRole::new("b"),
                    space: SpaceId(1),
                    node: DimensionVector::new(vec![1, 0]),
                },
            ],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
        }
    }

    #[test]
    fn v2_roundtrip() {
        let edge = sample_v2_edge();
        let bytes = encode_hyperedge(&edge).unwrap();
        assert_eq!(bytes[0], HYPEREDGE_PAYLOAD_V2_TAG);
        let decoded = decode_hyperedge(&bytes).unwrap();
        assert_eq!(decoded.id, edge.id);
        assert_eq!(decoded.directionality, Directionality::Directed);
        assert_eq!(decoded.endpoints[0].polarity, EndpointPolarity::Tail);
    }

    #[test]
    fn v3_roundtrip_with_provenance() {
        let mut edge = sample_v2_edge();
        edge.authoring_frame = Some(AuthoringFrameProvenance {
            frame_id: FrameId(7),
            as_of: RevisionId(3),
        });
        let bytes = encode_hyperedge(&edge).unwrap();
        assert_eq!(bytes[0], HYPEREDGE_PAYLOAD_V3_TAG);
        let decoded = decode_hyperedge(&bytes).unwrap();
        assert_eq!(decoded.authoring_frame, edge.authoring_frame);
    }

    #[test]
    fn v4_roundtrip_with_computation() {
        let mut edge = sample_v2_edge();
        edge.computation = Some(ComputationProvenance {
            inputs: vec![SubjectPin {
                kind: SubjectKind::Hyperedge,
                space: SpaceId(1),
                identity: SubjectIdentity::Hyperedge(HyperedgeId(1)),
                subject_revision: RevisionId(2),
            }],
        });
        let bytes = encode_hyperedge(&edge).unwrap();
        assert_eq!(bytes[0], HYPEREDGE_PAYLOAD_V4_TAG);
        let decoded = decode_hyperedge(&bytes).unwrap();
        assert_eq!(decoded.computation, edge.computation);
    }

    #[test]
    fn v4_roundtrip_with_authoring_and_computation() {
        let mut edge = sample_v2_edge();
        edge.authoring_frame = Some(AuthoringFrameProvenance {
            frame_id: FrameId(7),
            as_of: RevisionId(3),
        });
        edge.computation = Some(ComputationProvenance {
            inputs: vec![SubjectPin {
                kind: SubjectKind::Hyperedge,
                space: SpaceId(1),
                identity: SubjectIdentity::Hyperedge(HyperedgeId(1)),
                subject_revision: RevisionId(2),
            }],
        });
        let bytes = encode_hyperedge(&edge).unwrap();
        assert_eq!(bytes[0], HYPEREDGE_PAYLOAD_V4_TAG);
        let decoded = decode_hyperedge(&bytes).unwrap();
        assert_eq!(decoded.authoring_frame, edge.authoring_frame);
        assert_eq!(decoded.computation, edge.computation);
    }

    #[test]
    fn v3_decode_yields_none_computation() {
        let mut edge = sample_v2_edge();
        edge.authoring_frame = Some(AuthoringFrameProvenance {
            frame_id: FrameId(1),
            as_of: RevisionId(1),
        });
        let bytes = encode_hyperedge(&edge).unwrap();
        assert_eq!(bytes[0], HYPEREDGE_PAYLOAD_V3_TAG);
        assert!(decode_hyperedge(&bytes).unwrap().computation.is_none());
    }

    #[test]
    fn v1_decodes_as_undirected_neutral() {
        let fixture = sample_v1_fixture();
        let bytes = encode_hyperedge_v1_fixture(&fixture);
        assert_ne!(bytes.first(), Some(&HYPEREDGE_PAYLOAD_V2_TAG));
        let decoded = decode_hyperedge(&bytes).unwrap();
        assert_eq!(decoded.directionality, Directionality::Undirected);
        assert!(decoded
            .endpoints
            .iter()
            .all(|ep| ep.polarity == EndpointPolarity::Neutral));
    }

    #[test]
    fn v1_reencode_produces_v2() {
        let bytes = encode_hyperedge_v1_fixture(&sample_v1_fixture());
        let decoded = decode_hyperedge(&bytes).unwrap();
        let v2 = encode_hyperedge(&decoded).unwrap();
        assert_eq!(v2[0], HYPEREDGE_PAYLOAD_V2_TAG);
        assert_eq!(decode_hyperedge(&v2).unwrap().id, HyperedgeId(42));
    }

    #[test]
    fn v2_tag_not_confused_with_small_id_v1() {
        for id in [0u64, 1, 42, 127] {
            let fixture = HyperedgeV1Fixture {
                id: HyperedgeId(id),
                kind: HyperedgeKind::new("k"),
                endpoints: vec![
                    EndpointRefV1Fixture {
                        role: EndpointRole::new("a"),
                        space: SpaceId(1),
                        node: DimensionVector::new(vec![0, 0]),
                    },
                    EndpointRefV1Fixture {
                        role: EndpointRole::new("b"),
                        space: SpaceId(1),
                        node: DimensionVector::new(vec![1, 0]),
                    },
                ],
                weight_milli: None,
                metadata: BTreeMap::new(),
                valid_from: RevisionId::ZERO,
                valid_to: None,
            };
            let bytes = encode_hyperedge_v1_fixture(&fixture);
            assert_ne!(bytes[0], HYPEREDGE_PAYLOAD_V2_TAG);
            assert_ne!(bytes[0], HYPEREDGE_PAYLOAD_V3_TAG);
            assert_ne!(bytes[0], HYPEREDGE_PAYLOAD_V4_TAG);
            decode_hyperedge(&bytes).unwrap();
        }
    }

    #[test]
    fn random_noise_never_panics() {
        for seed in 0u64..64 {
            let mut data = vec![0u8; 32];
            for (i, b) in data.iter_mut().enumerate() {
                *b = ((seed.wrapping_mul(17).wrapping_add(i as u64)) & 0xFF) as u8;
            }
            let _ = decode_hyperedge(&data);
        }
    }
}
