use geo::Geometry;

use super::to_pl::{Builder, GeomOpResult};
use crate::ops::to_geom::Geos;
use polars::prelude::*;

pub fn run_op_on_struct<'a, F, T>(
    inputs: &[Series],
    f: F,
) -> PolarsResult<Series>
where
    F: Fn(&Geometry) -> T,
    T: Into<GeomOpResult>,
{
    let s = &inputs[0];
    let rows = s.len();
    let geometries = Geos::new(s);
    let mut builder = Builder::new(rows);
    for i in 0..rows {
        match &geometries.get_row(i) {
            Some(geom) => builder.add(f(geom).into()),
            None => builder.add_null(),
        }
    }
    Ok(builder.finish())
}
