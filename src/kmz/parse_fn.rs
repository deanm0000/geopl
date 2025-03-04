use crate::kmz::builders::Builders;
use crate::kmz::enums::*;
use ::zip::read::ZipArchive;
use kml::Kml;
use kml::types::{Coord, Geometry, LineString, LinearRing, Placemark, Point, Polygon};
use polars::chunked_array::builder::AnonymousListBuilder;
use polars::prelude::*;
use std::fs::File;
use std::io::Read;

pub(crate) fn parse_point(builders: &mut Builders, point: Point) {
    builders.add_point(point, true);
}
pub(crate) fn parse_multigeometry(builders: &mut Builders, geoms: Vec<Geometry>, add_row: bool) {
    // separate the list of geometries into lists of each kind of geometry, first create empty vecs
    let mut geoms = geoms;
    let mut points: Vec<Point> = vec![];
    let mut line_strings: Vec<LineString> = vec![];
    let mut linear_rings: Vec<LinearRing> = vec![];
    let mut polygons: Vec<Polygon> = vec![];

    // if nested multi, need to flatten
    let mut i = 0;
    while i < geoms.len() {
        if matches!(geoms[i], Geometry::MultiGeometry(_)) {
            if let Geometry::MultiGeometry(multi) = geoms.remove(i) {
                geoms.splice(i..i, multi.geometries); // Insert elements in-place
            }
        } else {
            i += 1;
        }
    }
    if geoms.len() == 0 {
        builders.row += 1;
        return;
    } else if geoms.len() <= 1 {
        parse_geometry(builders, geoms.remove(0), add_row);
        return;
    }
    // move each geo type to its vec
    geoms.into_iter().for_each(|geom| match geom {
        Geometry::LineString(ls) => line_strings.push(ls),
        Geometry::LinearRing(lr) => linear_rings.push(lr),
        Geometry::Point(p) => points.push(p),
        Geometry::Polygon(poly) => polygons.push(poly),
        _ => {
            eprintln!("found multi with unsupported {:?}", geom);
        }
    });
    // for each type, if only one treat it as that type, if multiple, treat it as MULTI
    match points.len() {
        0 => {}
        1 => builders.add_point(points.remove(0), false),
        _ => builders.add_points(points, false),
    };
    match line_strings.len() {
        0 => {}
        1 => builders.add_line(line_strings.remove(0).coords, LineKind::LineString, false),
        _ => {
            let ls_coords: Vec<Vec<Coord>> = line_strings.into_iter().map(|ls| ls.coords).collect();
            builders.add_lines(ls_coords, MultiLineKind::MultiLineString, false);
        }
    }
    match linear_rings.len() {
        0 => {}
        1 => builders.add_line(linear_rings.remove(0).coords, LineKind::LineString, false),
        _ => {
            let lr_coords: Vec<Vec<Coord>> = linear_rings.into_iter().map(|lr| lr.coords).collect();
            builders.add_lines(lr_coords, MultiLineKind::MultiLinearRing, false);
        }
    };
    match polygons.len() {
        0 => {}
        1 => builders.add_polygon(polygons.remove(0), false),
        _ => builders.add_polygons(polygons, false),
    }
    // This is implicitly assuming that at least one type existed but it isn't checked.

    builders.row += 1;
}
pub(crate) fn parse_geometry(builders: &mut Builders, geometry: Geometry, add_row: bool) {
    match geometry {
        Geometry::Point(point) => builders.add_point(point, add_row),
        Geometry::Element(_) => {}
        Geometry::LineString(ls) => builders.add_line(ls.coords, LineKind::LineString, add_row),
        Geometry::LinearRing(ls) => builders.add_line(ls.coords, LineKind::LinearRing, add_row),
        Geometry::Polygon(poly) => builders.add_polygon(poly, add_row),
        Geometry::MultiGeometry(multi_geom) => {
            parse_multigeometry(builders, multi_geom.geometries, add_row)
        }
        _ => {}
    }
}
pub(crate) fn parse_placemark(builders: &mut Builders, placemark: Placemark) {
    match placemark.geometry {
        Some(geometry) => parse_geometry(builders, geometry, true),
        None => builders.row += 1,
    };
    builders.add_name(placemark.name.as_deref());
    builders.add_description(placemark.description.as_deref());
}
pub(crate) fn iter_elems(builders: &mut Builders, elems: Vec<Kml>) {
    elems
        .into_iter()
        .for_each(|kml| parse_kml_inner(builders, kml))
}
pub(crate) fn parse_kml_inner(builders: &mut Builders, kml: Kml) {
    match kml {
        Kml::KmlDocument(doc) => iter_elems(builders, doc.elements),
        Kml::Point(point) => parse_point(builders, point),
        Kml::Placemark(placemark) => parse_placemark(builders, placemark),
        Kml::Document { attrs: _, elements } => iter_elems(builders, elements),
        Kml::Folder { attrs: _, elements } => {
            iter_elems(builders, elements);
        }
        Kml::Style(_) => {}
        _ => {}
    }
}
pub(crate) fn parse_kml(kml: Kml) -> DataFrame {
    let mut builders = Builders::new();
    parse_kml_inner(&mut builders, kml);
    builders.finish_geometry()
}
pub fn read_kml(kml_path: String, sink_path: Option<String>) -> DataFrame {
    let file = File::open(kml_path).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();
    for i in 0..archive.len() {
        let mut inner_file = archive.by_index(i).unwrap();
        let file_name = inner_file.name().to_string();

        if file_name.ends_with(".kml") {
            let mut contents = String::new();
            inner_file.read_to_string(&mut contents).unwrap();
            let lines = contents.lines().collect::<Vec<_>>();
            let kml_string = if lines[0].starts_with("<?xml") {
                lines[1..lines.len()].join("\n")
            } else {
                lines.join("\n")
            };
            let kml_data: Kml = kml_string.parse().unwrap();
            let df = parse_kml(kml_data);
            if let Some(save_path) = sink_path {
                let file_save = File::create(save_path).unwrap();
                ParquetWriter::new(file_save)
                    .finish(&mut df.clone())
                    .unwrap();
            }
            return df;
        }
    }
    DataFrame::empty()
}

pub(crate) fn poly_to_series(polygon: Polygon) -> Series {
    let outer_size = polygon.outer.coords.len();
    let outer_coords_fs = coords_to_series(polygon.outer.coords);

    let inner_size: usize = polygon.inner.iter().map(|lr| lr.coords.len()).sum();
    let size = (outer_size + inner_size) * 2;
    let mut ls_builder = AnonymousListBuilder::new(
        "".into(),
        size,
        Some(DataType::Array(Box::new(DataType::Float64), 2)),
    );
    ls_builder.append_series(&outer_coords_fs).unwrap();
    let inner_ss: Vec<Series> = polygon
        .inner
        .into_iter()
        .map(|ls| coords_to_series(ls.coords))
        .collect();
    for s in inner_ss.iter() {
        ls_builder.append_series(s).unwrap();
    }
    ls_builder.finish().into_series()
}
pub(crate) fn coords_to_series(coords: Vec<Coord>) -> Series {
    let coords_av: Vec<AnyValue> = coords
        .iter()
        .map(|coord| AnyValue::List(Series::new("".into(), vec![coord.x, coord.y])))
        .collect();
    let coord_fs = Series::from_any_values_and_dtype(
        "".into(),
        &coords_av,
        &DataType::Array(Box::new(DataType::Float64), 2),
        true,
    )
    .unwrap();
    coord_fs
}
