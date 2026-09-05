//! Versioned Nexus payload codec.

use std::io;

use bincode::{config::standard, decode_from_slice, encode_to_vec};

use super::nexus::NexusEdge;

pub const NEXUS_PAYLOAD_V1_TAG: u8 = 0xC1;

pub fn encode_nexus(edge: &NexusEdge) -> Result<Vec<u8>, io::Error> {
    let body = encode_to_vec(edge, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(NEXUS_PAYLOAD_V1_TAG);
    out.extend(body);
    Ok(out)
}

pub fn decode_nexus(data: &[u8]) -> Result<NexusEdge, io::Error> {
    if data.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty nexus payload"));
    }
    if data[0] != NEXUS_PAYLOAD_V1_TAG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown nexus payload version",
        ));
    }
    let (edge, consumed) = decode_from_slice::<NexusEdge, _>(&data[1..], standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if consumed != data.len() - 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes in nexus payload",
        ));
    }
    Ok(edge)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        address::{RevisionId, SpaceId},
        hyperedge::{Directionality, EndpointPolarity},
        nexus::{NexusEndpoint, NexusId, NexusKind},
        universe::ContainerRef,
    };
    use std::collections::BTreeMap;

    #[test]
    fn roundtrip() {
        let edge = NexusEdge {
            id: NexusId(42),
            kind: NexusKind::new("mirror"),
            endpoints: vec![
                NexusEndpoint {
                    container: ContainerRef::Space(SpaceId(1)),
                    region: None,
                    polarity: EndpointPolarity::Neutral,
                },
                NexusEndpoint {
                    container: ContainerRef::Space(SpaceId(2)),
                    region: None,
                    polarity: EndpointPolarity::Tail,
                },
            ],
            weight_milli: Some(500),
            metadata: BTreeMap::from([("k".into(), "v".into())]),
            valid_from: RevisionId::legacy(1),
            valid_to: None,
            directionality: Directionality::Directed,
        };
        let bytes = encode_nexus(&edge).unwrap();
        let decoded = decode_nexus(&bytes).unwrap();
        assert_eq!(decoded.id, edge.id);
        assert_eq!(decoded.kind.as_str(), "mirror");
    }
}
