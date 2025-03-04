use crate::kmz::enums::*;
use crate::kmz::parse_fn::*;
use kml::types::{Coord, Point, Polygon};
use paste::paste;
use polars::chunked_array::builder::get_list_builder;
use polars::prelude::*;
use polars_arrow::array::{
    MutableArray, MutableFixedSizeListArray, MutablePrimitiveArray, TryPush,
};
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
const INIT_CAPACITY: usize = 1000;
const POINT: PlSmallStr = PlSmallStr::from_static("POINT");
const MULTIPOINT: PlSmallStr = PlSmallStr::from_static("MULTIPOINT");
const POLYGON: PlSmallStr = PlSmallStr::from_static("POLYGON");
const MULTIPOLYGON: PlSmallStr = PlSmallStr::from_static("MULTIPOLYGON");
const GEOMETRY: PlSmallStr = PlSmallStr::from_static("GEOMETRY");

pub struct Builders {
    point: Option<MutableFixedSizeListArray<MutablePrimitiveArray<f64>>>, //There isn't a ChunkedBuilder for Array so use polars_arrow mutable
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
    pub(crate) row: usize,
    finished_geom: Vec<Column>,
}

impl Builders {
    pub fn new() -> Builders {
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

    pub fn add_point(self: &mut Builders, point: Point, add_row: bool) {
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
    pub fn add_points(self: &mut Builders, points: Vec<Point>, add_row: bool) {
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
    pub fn add_line(self: &mut Builders, coords: Vec<Coord>, line_kind: LineKind, add_row: bool) {
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
    pub fn add_lines(
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
    pub fn add_polygon(self: &mut Builders, polygon: Polygon, add_row: bool) {
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
    pub fn add_polygons(self: &mut Builders, polygons: Vec<Polygon>, add_row: bool) {
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
    pub fn add_name(self: &mut Builders, name: Option<&str>) {
        let name_take = self.name.take();
        let mut name_build = name_take.unwrap();
        name_build.append_option(name);
        self.name = Some(name_build);
    }
    pub fn add_description(self: &mut Builders, description: Option<&str>) {
        let desc_take = self.description.take();
        let mut desc = desc_take.unwrap();
        desc.append_option(description);
        self.description = Some(desc);
    }
    pub fn finish_geometry(self: &mut Builders) -> DataFrame {
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
