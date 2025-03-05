use geo::{
    Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Orient,
    Point, Polygon, orient::Direction,
};

use polars::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
enum ChunkedArrays {
    Point(ChunkedArray<FixedSizeListType>),
    MultiPoint(ChunkedArray<ListType>),
    LineString(ChunkedArray<ListType>),
    MultiLineString(ChunkedArray<ListType>),
    Polygon(ChunkedArray<ListType>),
    MultiPolygon(ChunkedArray<ListType>),
}

trait UncheckedAsSeries {
    unsafe fn unchecked_as_series(&self, idx: usize) -> Series;
}

impl UncheckedAsSeries for ChunkedArray<FixedSizeListType> {
    #[inline]
    unsafe fn unchecked_as_series(&self, idx: usize) -> Series {
        unsafe {
            let inner = self.value_unchecked(idx);
            Series::from_chunks_and_dtype_unchecked(
                self.name().clone(),
                vec![inner],
                &self.inner_dtype().to_physical(),
            )
        }
    }
}
impl UncheckedAsSeries for ChunkedArray<ListType> {
    #[inline]
    unsafe fn unchecked_as_series(&self, idx: usize) -> Series {
        unsafe {
            let inner = self.value_unchecked(idx);
            Series::from_chunks_and_dtype_unchecked(
                self.name().clone(),
                vec![inner],
                &self.inner_dtype().to_physical(),
            )
        }
    }
}
fn add_to(
    hashmap: &mut HashMap<PlSmallStr, (ChunkedArrays, HashSet<usize>)>,
    name: PlSmallStr,
    cas_null: (ChunkedArrays, HashSet<usize>),
) {
    if hashmap.contains_key(&name) {
        // TODO: reconcile multiple of same column type
        panic!("multiple {} found", name)
    }
    hashmap.insert(name, cas_null);
}
pub(crate) struct Geos {
    _hashmap: HashMap<PlSmallStr, (ChunkedArrays, HashSet<usize>)>,
}
impl Geos {
    pub(crate) fn new(struct_col: &Series) -> Geos {
        let ca_struct = struct_col.struct_().unwrap();
        let geometries_series = ca_struct.fields_as_series();
        let mut geometries: HashMap<PlSmallStr, (ChunkedArrays, HashSet<usize>)> = HashMap::new();
        geometries_series.into_iter().for_each(|s| {
            let nulls = s.is_null();
            let null_set: HashSet<usize> = nulls
                .iter()
                .enumerate()
                .filter_map(|(i, x)| match x {
                    Some(x) => match x {
                        true => Some(i),
                        false => None,
                    },
                    None => None,
                })
                .collect();
            if s.name().starts_with("POINT") {
                add_to(
                    &mut geometries,
                    "POINT".into(),
                    (ChunkedArrays::Point(s.array().unwrap().clone()), null_set),
                );
            } else if s.name().starts_with("MULTIPOINT") {
                add_to(
                    &mut geometries,
                    "MULTIPOINT".into(),
                    (
                        ChunkedArrays::MultiPoint(s.list().unwrap().clone()),
                        null_set,
                    ),
                );
            } else if s.name().starts_with("LINESTRING") {
                add_to(
                    &mut geometries,
                    "LINESTRING".into(),
                    (
                        ChunkedArrays::LineString(s.list().unwrap().clone()),
                        null_set,
                    ),
                );
            } else if s.name().starts_with("MULTILINESTRING") {
                add_to(
                    &mut geometries,
                    "MULTILINESTRING".into(),
                    (
                        ChunkedArrays::MultiLineString(s.list().unwrap().clone()),
                        null_set,
                    ),
                );
            } else if s.name().starts_with("POLYGON") {
                add_to(
                    &mut geometries,
                    "POLYGON".into(),
                    (ChunkedArrays::Polygon(s.list().unwrap().clone()), null_set),
                );
            } else if s.name().starts_with("MULTIPOLYGON") {
                add_to(
                    &mut geometries,
                    "MULTIPOLYGON".into(),
                    (
                        ChunkedArrays::MultiPolygon(s.list().unwrap().clone()),
                        null_set,
                    ),
                );
            } else {
                panic!("bad column {}", s.name())
            }
        });
        Geos {
            _hashmap: geometries,
        }
    }

    pub(crate) fn get_row(&self, row: usize) -> Option<Geometry> {
        let mut geoms: Vec<Geometry> = self
            ._hashmap
            .iter()
            .filter_map(|(_, (cas, nulls))| {
                if nulls.contains(&row) {
                    return None;
                }
                Some(match cas {
                    ChunkedArrays::Point(p) => {
                        let s = unsafe { p.unchecked_as_series(row) };

                        chunked_to_point(s.f64().unwrap()).into()
                    }
                    ChunkedArrays::MultiPoint(p) => {
                        let s = unsafe { p.unchecked_as_series(row) };

                        chunked_to_multipoint(s.array().unwrap()).into()
                    }
                    ChunkedArrays::LineString(p) => {
                        let s = unsafe { p.unchecked_as_series(row) };

                        chunked_to_linestring(s.array().unwrap()).into()
                    }
                    ChunkedArrays::MultiLineString(p) => {
                        let s = unsafe { p.unchecked_as_series(row) };

                        chunked_to_multilinestring(s.list().unwrap()).into()
                    }
                    ChunkedArrays::Polygon(p) => {
                        let s = unsafe { p.unchecked_as_series(row) };

                        chunked_to_polygon(s.list().unwrap()).into()
                    }
                    ChunkedArrays::MultiPolygon(p) => {
                        let s = unsafe { p.unchecked_as_series(row) };

                        chunked_to_multipolygon(s.list().unwrap()).into()
                    }
                })
            })
            .collect();
        match geoms.len() {
            0 => None,
            1 => Some(geoms.remove(0)),
            _ => Some(Geometry::GeometryCollection(GeometryCollection::new_from(
                geoms,
            ))),
        }
    }
}

pub(crate) fn chunked_to_point(ca: &ChunkedArray<Float64Type>) -> Point {
    let point: Point = (ca.get(0).unwrap(), ca.get(1).unwrap()).into();
    point
}
pub(crate) fn chunked_to_points(ca: &ChunkedArray<FixedSizeListType>) -> Vec<Point> {
    let points: Vec<Point> = ca
        .amortized_iter()
        .filter_map(|s3| match s3 {
            Some(s3) => {
                let s3 = s3.as_ref();
                let ca_coords = s3.f64().unwrap();
                Some(chunked_to_point(ca_coords))
            }
            None => None,
        })
        .collect();
    points
}
pub(crate) fn chunked_to_linestring(ca: &ChunkedArray<FixedSizeListType>) -> LineString {
    let points = chunked_to_points(ca);
    LineString::from(points)
}
pub(crate) fn chunked_to_multipoint(ca: &ChunkedArray<FixedSizeListType>) -> MultiPoint {
    let points = chunked_to_points(ca);
    MultiPoint(points)
}
pub(crate) fn chunked_to_linestrings(ca: &ChunkedArray<ListType>) -> Vec<LineString> {
    ca.amortized_iter()
        .map(|s2| match s2 {
            Some(s2) => {
                let s2 = s2.as_ref();
                let ca_points = s2.array().unwrap();
                chunked_to_linestring(ca_points)
            }
            None => {
                let empty: Vec<(f64, f64)> = vec![];
                LineString::from(empty)
            }
        })
        .collect()
}
pub(crate) fn chunked_to_multilinestring(ca: &ChunkedArray<ListType>) -> MultiLineString {
    MultiLineString(chunked_to_linestrings(ca))
}
pub(crate) fn chunked_to_polygon(ca: &ChunkedArray<ListType>) -> Polygon {
    let mut linestrings = chunked_to_linestrings(ca);
    let exterior = linestrings.remove(0);
    let geo_poly = Polygon::new(exterior, linestrings);
    geo_poly.orient(Direction::Default)
}
pub(crate) fn chunked_to_multipolygon(ca: &ChunkedArray<ListType>) -> MultiPolygon {
    let multis: Vec<Polygon> = ca
        .amortized_iter()
        .filter_map(|s3| match s3 {
            Some(s3) => {
                let s3 = s3.as_ref();
                let polygon = s3.list().unwrap();
                Some(chunked_to_polygon(polygon))
            }
            None => None,
        })
        .collect();
    MultiPolygon(multis)
}
