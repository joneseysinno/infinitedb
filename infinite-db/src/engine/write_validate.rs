//! Write-path validation at the effect boundary (T5/T7).

use crate::engine::error::EngineError;
use crate::infinitedb_core::{
    address::DimensionVector,
    space::{CenterReservation, SpaceConfig},
};
use crate::infinitedb_index::center::dyadic_center_level;

/// Validate coordinates and center-reservation policy before enqueue.
pub fn validate_point_write(
    config: &SpaceConfig,
    point: &DimensionVector,
    structural: bool,
) -> Result<(), EngineError> {
    if point.coords.len() != config.dims {
        return Err(EngineError::InvalidSpaceConfig {
            message: format!(
                "expected {} dimensions, got {}",
                config.dims,
                point.coords.len()
            ),
        });
    }
    let limit = 1u64.checked_shl(config.bits_per_dim).unwrap_or(u64::MAX);
    for &c in &point.coords {
        if c as u64 >= limit {
            return Err(EngineError::InvalidSpaceConfig {
                message: format!(
                    "coordinate {c} exceeds bits_per_dim {} (max {})",
                    config.bits_per_dim,
                    limit - 1
                ),
            });
        }
    }
    match config.center_reservation {
        CenterReservation::Off => {}
        CenterReservation::StructuralOnly => {
            let is_center = dyadic_center_level(&point.coords, config.bits_per_dim).is_some();
            if is_center && !structural {
                return Err(EngineError::InvalidSpaceConfig {
                    message: "write at reserved dyadic center requires structural marker".into(),
                });
            }
            if structural && !is_center {
                return Err(EngineError::InvalidSpaceConfig {
                    message: "structural write requires a dyadic cell-center point".into(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{DimensionVector, SpaceId};
    use crate::infinitedb_core::space::{CenterReservation, SpaceConfig};

    #[test]
    fn rejects_oversized_coordinate() {
        let cfg = SpaceConfig::new(SpaceId(1), "s", 1).with_bits_per_dim(4);
        let pt = DimensionVector::new(vec![20]);
        assert!(validate_point_write(&cfg, &pt, false).is_err());
    }

    #[test]
    fn structural_only_validation_golden() {
        let cfg = SpaceConfig::new(SpaceId(1), "s", 2)
            .with_bits_per_dim(8)
            .with_center_reservation(CenterReservation::StructuralOnly);
        let off = SpaceConfig::new(SpaceId(2), "off", 2).with_bits_per_dim(8);

        let pts = [
            (vec![5, 5], false, true),
            (vec![5, 5], true, false),
            (vec![6, 4], false, false),
            (vec![6, 4], true, true),
        ];
        for (coords, structural, expect_err) in pts {
            let pt = DimensionVector::new(coords.clone());
            let err = validate_point_write(&cfg, &pt, structural).is_err();
            assert_eq!(
                err, expect_err,
                "StructuralOnly ({coords:?}, structural={structural})"
            );
            for off_coords in [&[0, 0], &[5, 5], &[6, 4], &[127, 0], &[128, 0]] {
                let off_pt = DimensionVector::new(off_coords.to_vec());
                assert!(
                    validate_point_write(&off, &off_pt, structural).is_ok(),
                    "Off regression at {off_coords:?}"
                );
            }
        }
    }

    #[test]
    fn inv_center_complement_measure_1d() {
        let cfg = SpaceConfig::new(SpaceId(1), "s", 1)
            .with_bits_per_dim(8)
            .with_center_reservation(CenterReservation::StructuralOnly);
        let mut rejected = 0u32;
        for c in 0..256u32 {
            let pt = DimensionVector::new(vec![c]);
            if validate_point_write(&cfg, &pt, false).is_err() {
                rejected += 1;
            }
        }
        assert_eq!(rejected, 127);
    }
}
