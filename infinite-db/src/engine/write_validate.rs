//! Write-path validation at the effect boundary (T5/T7).

use crate::engine::error::EngineError;
use crate::infinitedb_core::{
    address::DimensionVector,
    space::{CenterReservation, SpaceConfig},
};
use crate::infinitedb_index::center::is_dyadic_center;

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
            let touches_center = point
                .coords
                .iter()
                .any(|&c| is_dyadic_center(c, config.bits_per_dim).is_some());
            if touches_center && !structural {
                return Err(EngineError::InvalidSpaceConfig {
                    message: "write at reserved dyadic center requires structural marker".into(),
                });
            }
            if structural && !is_center_point(&point.coords, config.bits_per_dim) {
                return Err(EngineError::InvalidSpaceConfig {
                    message: "structural write requires a dyadic cell-center point".into(),
                });
            }
        }
    }
    Ok(())
}

fn is_center_point(coords: &[u32], bits_per_dim: u32) -> bool {
    if bits_per_dim == 0 {
        return false;
    }
    for k in 1..=bits_per_dim {
        let shift = bits_per_dim - k;
        let modulus = 1u32 << shift;
        let half = 1u32 << shift.saturating_sub(1);
        if coords
            .iter()
            .all(|&c| c % modulus == half)
        {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::SpaceId;

    #[test]
    fn rejects_oversized_coordinate() {
        let cfg = SpaceConfig::new(SpaceId(1), "s", 1).with_bits_per_dim(4);
        let pt = DimensionVector::new(vec![20]);
        assert!(validate_point_write(&cfg, &pt, false).is_err());
    }
}
