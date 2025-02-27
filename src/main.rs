use ::zip::read::ZipArchive;
use kml::Kml;
use kml::types::{Coord, Geometry, LineString, LinearRing, Placemark, Point, Polygon};
use paste::paste;
use polars::chunked_array::builder::{AnonymousListBuilder, get_list_builder};
use polars::prelude::*;
use polars_arrow::array::{
    MutableArray, MutableFixedSizeListArray, MutablePrimitiveArray, TryPush,
};
use std::env;
use std::fs::File;
use std::io::Read;
const INIT_CAPACITY: usize = 1000;
const POINT: PlSmallStr = PlSmallStr::from_static("POINT");
const MULTIPOINT: PlSmallStr = PlSmallStr::from_static("MULTIPOINT");
const POLYGON: PlSmallStr = PlSmallStr::from_static("POLYGON");
const MULTIPOLYGON: PlSmallStr = PlSmallStr::from_static("MULTIPOLYGON");
const GEOMETRY: PlSmallStr = PlSmallStr::from_static("GEOMETRY");
macro_rules! process_finisher {
    ($self:ident, $($name:ident),*) => {
        $(
            paste! {
                let builder_take = $self.$name.take();
                if let Some(mut builder) = builder_take {
                    for _ in $self.[<$name _row>]..$self.row {
                        builder.append_null();
                    }
                    let column = builder.finish().into_series().into_column();
                    $self.finished_geom.push(column);
                }
            }
        )*
    };
}
#[derive(Clone)]
enum LineKind {
    LineString,
    LinearRing,
}
impl From<LineKind> for PlSmallStr {
    fn from(value: LineKind) -> Self {
        match value {
            LineKind::LineString => PlSmallStr::from("LINESTRING"),
            LineKind::LinearRing => PlSmallStr::from("LINEARRING"),
        }
    }
}
#[derive(Clone)]
enum MultiLineKind {
    MultiLineString,
    MultiLinearRing,
}
impl From<MultiLineKind> for PlSmallStr {
    fn from(value: MultiLineKind) -> Self {
        match value {
            MultiLineKind::MultiLineString => PlSmallStr::from("MULTILINESTRING"),
            MultiLineKind::MultiLinearRing => PlSmallStr::from("MULTILINEARRING"),
        }
    }
}

fn poly_to_series(polygon: Polygon) -> Series {
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
fn coords_to_series(coords: Vec<Coord>) -> Series {
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

struct Builders {
    point: Option<MutableFixedSizeListArray<MutablePrimitiveArray<f64>>>,
    point_row: usize,
    points: Option<Box<dyn ListBuilderTrait>>,
    points_row: usize,
    line_string: Option<Box<dyn ListBuilderTrait>>,
    line_string_row: usize,
    line_strings: Option<Box<dyn ListBuilderTrait>>,
    line_strings_row: usize,
    linear_ring: Option<Box<dyn ListBuilderTrait>>,
    linear_ring_row: usize,
    linear_rings: Option<Box<dyn ListBuilderTrait>>,
    linear_rings_row: usize,
    polygon: Option<Box<dyn ListBuilderTrait>>,
    polygon_row: usize,
    polygons: Option<Box<dyn ListBuilderTrait>>,
    polygons_row: usize,
    name: Option<StringChunkedBuilder>,
    description: Option<StringChunkedBuilder>,
    row: usize,
    finished_geom: Vec<Column>,
}

impl Builders {
    fn new() -> Builders {
        Builders {
            point: None,
            point_row: 0usize,
            points: None,
            points_row: 0usize,
            line_string: None,
            line_string_row: 0usize,
            line_strings: None,
            line_strings_row: 0usize,
            polygon: None,
            polygon_row: 0usize,
            polygons: None,
            polygons_row: 0usize,
            linear_ring: None,
            linear_ring_row: 0usize,
            linear_rings: None,
            linear_rings_row: 0usize,
            name: Some(StringChunkedBuilder::new("Name".into(), INIT_CAPACITY)),
            description: Some(StringChunkedBuilder::new(
                "Description".into(),
                INIT_CAPACITY,
            )),
            row: 0usize,
            finished_geom: vec![],
        }
    }

    fn add_point(self: &mut Builders, point: Point, add_row: bool) {
        let point_take = self.point.take();
        let mut point_builder = point_take
            .unwrap_or_else(|| MutableFixedSizeListArray::new(MutablePrimitiveArray::new(), 2));

        for _ in self.point_row..self.row {
            point_builder.push_null();
        }
        let res = point_builder.try_push(Some(vec![Some(point.coord.x), Some(point.coord.y)]));
        match res {
            Ok(_) => {}
            Err(e) => {
                eprintln!("{},point push {},{}", e, point.coord.x, point.coord.y);
                point_builder.push_null();
            }
        };
        if add_row {
            self.row += 1;
        }
        self.point_row = self.row;
        self.point = Some(point_builder);
    }
    fn add_points(self: &mut Builders, points: Vec<Point>, add_row: bool) {
        let point_take = self.points.take();
        let mut points_builder = point_take.unwrap_or_else(|| {
            get_list_builder(
                &DataType::Array(Box::new(DataType::Float64), 2),
                INIT_CAPACITY,
                INIT_CAPACITY / 2,
                MULTIPOINT,
            )
        });

        for _ in self.points_row..self.row {
            points_builder.append_null();
        }
        self.points_row = self.row;
        let coords: Vec<Coord> = points.into_iter().map(|p| p.coord).collect();
        let points_s = coords_to_series(coords);
        points_builder.append_series(&points_s).unwrap();
        if add_row {
            self.row += 1;
        }
        self.points_row = self.row;
        self.points = Some(points_builder);
    }
    fn add_line(self: &mut Builders, coords: Vec<Coord>, line_kind: LineKind, add_row: bool) {
        let (ls_take, own_row) = match line_kind {
            LineKind::LineString => (self.line_string.take(), &mut self.line_string_row),
            LineKind::LinearRing => (self.linear_ring.take(), &mut self.linear_ring_row),
        };
        let mut ls_builder = ls_take.unwrap_or_else(|| {
            get_list_builder(
                &DataType::Array(Box::new(DataType::Float64), 2),
                INIT_CAPACITY,
                INIT_CAPACITY / 2,
                line_kind.clone().into(),
            )
        });

        for _ in *own_row..self.row {
            ls_builder.append_null()
        }
        *own_row = self.row;
        let coord_s = coords_to_series(coords);

        ls_builder.append_series(&coord_s).unwrap();

        if add_row {
            self.row += 1;
        }
        *own_row += 1;
        match line_kind {
            LineKind::LineString => self.line_string = Some(ls_builder),
            LineKind::LinearRing => self.linear_ring = Some(ls_builder),
        };
    }
    fn add_lines(
        self: &mut Builders,
        coords: Vec<Vec<Coord>>,
        line_kind: MultiLineKind,
        add_row: bool,
    ) {
        let (ls_take, own_row) = match line_kind {
            MultiLineKind::MultiLineString => {
                (self.line_strings.take(), &mut self.line_strings_row)
            }
            MultiLineKind::MultiLinearRing => {
                (self.linear_rings.take(), &mut self.linear_rings_row)
            }
        };
        let mut ls_builder = ls_take.unwrap_or_else(|| {
            get_list_builder(
                &DataType::List(Box::new(DataType::Array(Box::new(DataType::Float64), 2))),
                INIT_CAPACITY,
                INIT_CAPACITY / 2,
                line_kind.clone().into(),
            )
        });

        for _ in *own_row..self.row {
            ls_builder.append_null()
        }
        *own_row = self.row;

        let mut outer_builder = get_list_builder(
            &DataType::Array(Box::new(DataType::Float64), 2),
            coords.len() * 2,
            coords.len(),
            "".into(),
        );
        for outer_coords in coords {
            let coord_s = coords_to_series(outer_coords);
            outer_builder.append_series(&coord_s).unwrap();
        }
        let outer_s = outer_builder.finish().into_series();

        ls_builder.append_series(&outer_s).unwrap();

        if add_row {
            self.row += 1;
        }
        *own_row += 1;
        match line_kind {
            MultiLineKind::MultiLineString => self.line_strings = Some(ls_builder),
            MultiLineKind::MultiLinearRing => self.linear_rings = Some(ls_builder),
        };
    }
    fn add_polygon(self: &mut Builders, polygon: Polygon, add_row: bool) {
        let p_take = self.polygon.take();

        let mut p_builder = p_take.unwrap_or_else(|| {
            get_list_builder(
                &DataType::List(Box::new(DataType::Array(Box::new(DataType::Float64), 2))),
                INIT_CAPACITY,
                INIT_CAPACITY / 2,
                POLYGON,
            )
        });
        for _ in self.polygon_row..self.row {
            p_builder.append_null();
        }
        self.polygon_row = self.row;
        let poly_s = poly_to_series(polygon);
        if add_row {
            self.row += 1;
        }
        self.polygon_row += 1;
        p_builder.append_series(&poly_s).unwrap();
        self.polygon = Some(p_builder);
    }
    fn add_polygons(self: &mut Builders, polygons: Vec<Polygon>, add_row: bool) {
        let p_take = self.polygons.take();

        let mut p_builder = p_take.unwrap_or_else(|| {
            get_list_builder(
                &DataType::List(Box::new(DataType::List(Box::new(DataType::Array(
                    Box::new(DataType::Float64),
                    2,
                ))))),
                INIT_CAPACITY,
                INIT_CAPACITY / 2,
                MULTIPOLYGON,
            )
        });
        for _ in self.polygons_row..self.row {
            p_builder.append_null();
        }
        self.polygons_row = self.row;
        let mut ls_builder = get_list_builder(
            &DataType::List(Box::new(DataType::Array(Box::new(DataType::Float64), 2))),
            INIT_CAPACITY,
            INIT_CAPACITY / 2,
            POLYGON,
        );

        let poly_s: Vec<Series> = polygons
            .into_iter()
            .map(|polygon| poly_to_series(polygon))
            .collect();
        for s in poly_s.iter() {
            ls_builder.append_series(s).unwrap();
        }
        let polys_s = ls_builder.finish().into_series();
        if add_row {
            self.row += 1;
        }
        self.polygons_row += 1;
        p_builder.append_series(&polys_s).unwrap();
        self.polygons = Some(p_builder);
    }
    fn add_name(self: &mut Builders, name: Option<&str>) {
        let name_take = self.name.take();
        let mut name_build = name_take.unwrap();
        name_build.append_option(name);
        self.name = Some(name_build);
    }
    fn add_description(self: &mut Builders, description: Option<&str>) {
        let desc_take = self.description.take();
        let mut desc = desc_take.unwrap();
        desc.append_option(description);
        self.description = Some(desc);
    }
    fn finish_geometry(self: &mut Builders) -> DataFrame {
        let point_take = self.point.take();
        if let Some(mut point) = point_take {
            for _ in self.point_row..self.row {
                point.push_null();
            }
            let point = Series::from_arrow(POINT, point.as_box())
                .unwrap()
                .into_column();
            self.finished_geom.push(point);
        }
        let names = self.name.take().unwrap().finish().into_column();
        let description = self.description.take().unwrap().finish().into_column();
        process_finisher!(
            self,
            points,
            line_string,
            line_strings,
            linear_ring,
            linear_rings,
            polygon,
            polygons
        );
        let mut geom =
            StructChunked::from_columns(GEOMETRY, self.row, &self.finished_geom).unwrap();
        geom.shrink_to_fit();
        let geom = geom.into_column();
        DataFrame::new(vec![names, description, geom]).unwrap()
    }
}
fn main() {
    let mut args: Vec<String> = env::args().collect();

    let (source, sink) = match args.len() {
        2=> (args.remove(1), None),
        3=> {
            let sink = args.remove(2);
            let source = args.remove(1);
            (source, Some(sink))
        },
        _=> panic!("unsupported args")
    };
    let df = read_kml(
        source, sink
    );
    eprintln!("{}", df);
}
fn parse_point(builders: &mut Builders, point: Point) {
    builders.add_point(point, true);
}
fn parse_multigeometry(builders: &mut Builders, geoms: Vec<Geometry>, add_row: bool) {
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

    geoms.into_iter().for_each(|geom| match geom {
        Geometry::LineString(ls) => line_strings.push(ls),
        Geometry::LinearRing(lr) => linear_rings.push(lr),
        Geometry::Point(p) => points.push(p),
        Geometry::Polygon(poly) => polygons.push(poly),
        _ => {
            eprintln!("found multi with unsupported {:?}", geom);
        }
    });
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
    builders.row += 1;
}

fn parse_geometry(builders: &mut Builders, geometry: Geometry, add_row: bool) {
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
fn parse_placemark(builders: &mut Builders, placemark: Placemark) {
    match placemark.geometry {
        Some(geometry) => parse_geometry(builders, geometry, true),
        None => builders.row += 1,
    };
    builders.add_name(placemark.name.as_deref());
    builders.add_description(placemark.description.as_deref());
}
fn iter_elems(builders: &mut Builders, elems: Vec<Kml>) {
    elems
        .into_iter()
        .for_each(|kml| parse_kml_inner(builders, kml))
}
fn parse_kml_inner(builders: &mut Builders, kml: Kml) {

    match kml {
        Kml::KmlDocument(doc) => iter_elems(builders, doc.elements),
        Kml::Point(point) => parse_point(builders, point),
        Kml::Placemark(placemark) => parse_placemark(builders, placemark),
        Kml::Document { attrs: _, elements } => iter_elems(builders, elements),
        Kml::Folder { attrs:_, elements } => {
            iter_elems(builders, elements);
        }
        Kml::Style(_) => {}
        _ => {
        }
    }
}
fn parse_kml(kml: Kml) -> DataFrame {
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
            if let Some(save_path)=sink_path {
            let file_save = File::create(save_path).unwrap();
            ParquetWriter::new(file_save).finish(&mut df.clone()).unwrap();
            }
            return df;
        }
    }
    DataFrame::empty()
}
