mod kmz;
use geo::{
    Centroid, GeodesicArea, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Orient,
    Point, Polygon, orient::Direction,
};
use kmz::read_kml;
use polars::chunked_array::builder::get_list_builder;
use polars::prelude::*;
use polars_arrow::array::{
    MutableArray, MutableFixedSizeListArray, MutablePrimitiveArray, TryPush,
};
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::error::PyPolarsErr;
use pyo3_polars::{PolarsAllocator, PyDataFrame};

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();
fn points_to_series(p: &[Point]) -> PolarsResult<Series> {
    let avs: Vec<AnyValue> = p
        .into_iter()
        .map(|p| AnyValue::List(Series::new(PlSmallStr::EMPTY, vec![p.x(), p.y()])))
        .collect();
    Series::from_any_values_and_dtype(
        PlSmallStr::EMPTY,
        &avs,
        &DataType::Array(Box::new(DataType::Float64), 2),
        true,
    )
}
enum GeomTypes {
    Point,
    MultiPoint,
    Line,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
}
enum Builder {
    Pending((usize, usize)),
    Scalar(PrimitiveChunkedBuilder<Float64Type>),
    Point(MutableFixedSizeListArray<MutablePrimitiveArray<f64>>),
    MultiPoint(Box<dyn ListBuilderTrait>),
    LineString(Box<dyn ListBuilderTrait>),
    MultiLineString(Box<dyn ListBuilderTrait>),
    Polygon(Box<dyn ListBuilderTrait>),
    MultiPolygon(Box<dyn ListBuilderTrait>),
}

#[pyfunction]
#[pyo3(signature=(path))]
fn read_kmz(path: &str) -> PyResult<PyDataFrame> {
    let df = read_kml(path.to_string(), None);
    Ok(PyDataFrame(df))
}

#[pymodule]
#[pyo3(name="_geopl")]
fn _geopl(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_kmz, m)?)?;
    Ok(())
}
impl Builder {
    fn new(size: usize) -> Builder {
        Builder::Pending((size, 0))
    }
    fn make_scalar(&mut self) {
        let (size, skips) = match self {
            Builder::Pending((size, skips)) => (*size, *skips),
            _ => panic!("can't make new arr from other than pending"),
        };
        let mut builder = PrimitiveChunkedBuilder::<Float64Type>::new(PlSmallStr::EMPTY, size);
        for _ in 0..skips {
            builder.append_null();
        }
        *self = Builder::Scalar(builder);
    }
    fn make_point(&mut self) {
        let (size, skips) = match self {
            Builder::Pending((size, skips)) => (*size, *skips),
            _ => panic!("can't make new arr from other than pending"),
        };
        let mut builder = MutableFixedSizeListArray::new_from(
            MutablePrimitiveArray::with_capacity(size * 2),
            ArrowDataType::FixedSizeList(
                Box::new(ArrowField::new(
                    PlSmallStr::EMPTY,
                    ArrowDataType::Float64,
                    false,
                )),
                2,
            ),
            2,
        );
        for _ in 0..skips {
            builder.push_null();
        }
        *self = Builder::Point(builder);
    }
    fn make_list_arr(&mut self, geom: GeomTypes) {
        let (size, skips) = match self {
            Builder::Pending((size, skips)) => (*size, *skips),
            _ => panic!("can't make new listarr from other than pending"),
        };
        match geom {
            GeomTypes::MultiPoint | GeomTypes::Line | GeomTypes::LineString => {}
            _ => unimplemented!(),
        }
        let mut builder = get_list_builder(
            &DataType::Array(Box::new(DataType::Float64), 2),
            size * 2,
            size,
            PlSmallStr::EMPTY,
        );
        for _ in 0..skips {
            builder.append_null();
        }
        match geom {
            GeomTypes::MultiPoint => *self = Builder::MultiPoint(builder),
            GeomTypes::Line | GeomTypes::LineString => *self = Builder::LineString(builder),
            _ => unimplemented!(),
        }
    }
    fn make_list_list_arr(&mut self, geom: GeomTypes) {
        let (size, skips) = match self {
            Builder::Pending((size, skips)) => (*size, *skips),
            _ => panic!("can't make new listlistarr from other than pending"),
        };
        match geom {
            GeomTypes::MultiLineString | GeomTypes::Polygon => {}
            _ => unimplemented!(),
        }
        let mut builder = get_list_builder(
            &DataType::List(Box::new(DataType::Array(Box::new(DataType::Float64), 2))),
            size * 2,
            size,
            PlSmallStr::EMPTY,
        );
        for _ in 0..skips {
            builder.append_null();
        }
        match geom {
            GeomTypes::MultiLineString => *self = Builder::MultiLineString(builder),
            GeomTypes::Polygon => *self = Builder::Polygon(builder),
            _ => unimplemented!(),
        }
    }
    fn new_list_list_list_arr(&mut self, geom: GeomTypes) {
        let (size, skips) = match self {
            Builder::Pending((size, skips)) => (*size, *skips),
            _ => panic!("can't make new listlistarr from other than pending"),
        };
        match geom {
            GeomTypes::MultiPolygon => {}
            _ => unimplemented!(),
        }
        let mut builder = get_list_builder(
            &DataType::List(Box::new(DataType::List(Box::new(DataType::Array(
                Box::new(DataType::Float64),
                2,
            ))))),
            size * 2,
            size,
            PlSmallStr::EMPTY,
        );
        for _ in 0..skips {
            builder.append_null();
        }
        *self = Builder::MultiPolygon(builder)
    }
    fn add_scalar(&mut self, p: f64) {
        match self {
            Builder::Pending(_) => {
                self.make_scalar();
                self.add_scalar(p);
            }
            Builder::Scalar(builder) => {
                builder.append_value(p);
            }
            _ => unimplemented!(),
        }
    }
    fn add_point(&mut self, p: Point) {
        match self {
            Builder::Pending(_) => {
                self.make_point();
                self.add_point(p);
            }
            Builder::Point(builder) => {
                builder
                    .try_push(Some(vec![Some(p.x()), Some(p.y())]))
                    .unwrap();
            }
            _ => unimplemented!(),
        }
    }
    fn add_multipoint(&mut self, p: MultiPoint) {
        match self {
            Builder::Pending(_) => {
                self.make_list_arr(GeomTypes::MultiPoint);
                self.add_multipoint(p);
            }
            Builder::MultiPoint(builder) => {
                let points = p.0;
                let s = points_to_series(&points).unwrap();
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    fn add_line(&mut self, p: Line) {
        // TODO: add another type that is a FixedSizeList(FixedSizeList) for this
        match self {
            Builder::Pending(_) => {
                self.make_list_arr(GeomTypes::Line);
                self.add_line(p);
            }
            Builder::LineString(builder) => {
                let points = p.points();
                let s = points_to_series(&[points.0, points.1]).unwrap();
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    fn add_linestring(&mut self, p: LineString) {
        match self {
            Builder::Pending(_) => {
                self.make_list_arr(GeomTypes::LineString);
                self.add_linestring(p);
            }
            Builder::LineString(builder) => {
                let points = p.into_points();
                let s = points_to_series(&points).unwrap();
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    fn add_multi_line_string(&mut self, p: MultiLineString) {
        match self {
            Builder::Pending(_) => {
                self.make_list_list_arr(GeomTypes::MultiLineString);
                self.add_multi_line_string(p);
            }
            Builder::MultiLineString(builder) => {
                let linestrings = p.0;
                let lil_size = linestrings.len();
                let mut lil_builder = Builder::new(lil_size);
                linestrings.into_iter().for_each(|l| {
                    lil_builder.add_linestring(l);
                });
                let s = lil_builder.finish(PlSmallStr::EMPTY);
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    fn add_polygon(&mut self, p: Polygon) {
        match self {
            Builder::Pending(_) => {
                self.make_list_list_arr(GeomTypes::Polygon);
                self.add_polygon(p);
            }
            Builder::MultiLineString(builder) => {
                let exterior = p.exterior().to_owned();
                let interiors = p.interiors();
                let size = interiors.len() + 1;
                let mut inner_lines = Builder::new(size);
                inner_lines.add_linestring(exterior);
                interiors.into_iter().for_each(|l| {
                    inner_lines.add_linestring(l.to_owned());
                });
                let s = inner_lines.finish(PlSmallStr::EMPTY);
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    fn add_multi_polygon(&mut self, p: MultiPolygon) {
        match self {
            Builder::Pending(_) => {
                self.make_list_list_arr(GeomTypes::MultiPolygon);
                self.add_multi_polygon(p);
            }
            Builder::MultiPolygon(builder) => {
                let polygons = p.0;
                let size = polygons.len();
                let mut inner_polys = Builder::new(size);
                polygons.into_iter().for_each(|poly| {
                    inner_polys.add_polygon(poly.to_owned());
                });
                let s = inner_polys.finish(PlSmallStr::EMPTY);
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    fn add_null(&mut self) {
        match self {
            Builder::Pending((_, skips)) => *skips += 1,
            Builder::Point(builder) => {
                builder.push_null();
            }
            Builder::MultiPoint(builder)
            | Builder::LineString(builder)
            | Builder::MultiLineString(builder)
            | Builder::Polygon(builder)
            | Builder::MultiPolygon(builder) => {
                builder.append_null();
            },
            Builder::Scalar(builder)=>builder.append_null(),
            _ => unimplemented!(),
        }
    }
    fn add(&mut self, value: GeomOpResult) {
        match value {
            GeomOpResult::Point(p) => self.add_point(p),
            GeomOpResult::MultiPoint(ps) => self.add_multipoint(ps),
            GeomOpResult::Line(l) => self.add_line(l),
            GeomOpResult::LineString(l) => self.add_linestring(l),
            GeomOpResult::MultiLineString(ml) => self.add_multi_line_string(ml),
            GeomOpResult::Polygon(poly) => self.add_polygon(poly),
            GeomOpResult::MultiPolygon(mpoly) => self.add_multi_polygon(mpoly),
            GeomOpResult::Float(val) => self.add_scalar(val),
            GeomOpResult::Null => self.add_null(),
        }
    }
    fn finish(self, name: PlSmallStr) -> Series {
        match self {
            Builder::Point(mut builder) => Series::from_arrow(name, builder.as_box()).unwrap(),
            Builder::MultiPoint(mut builder)
            | Builder::LineString(mut builder)
            | Builder::MultiLineString(mut builder)
            | Builder::Polygon(mut builder)
            | Builder::MultiPolygon(mut builder) => builder.finish().into_series().with_name(name),
            Builder::Scalar(mut builder) => builder.finish().into_series().with_name(name),
            Builder::Pending(_) => panic!("can't finish pending builder"),
        }
    }
}
// fn main() {
//     let mut args: Vec<String> = env::args().collect();

//     let (source, sink) = match args.len() {
//         2 => (args.remove(1), None),
//         3 => {
//             let sink = args.remove(2);
//             let source = args.remove(1);
//             (source, Some(sink))
//         }
//         _ => panic!("unsupported args"),
//     };
//     let mut df = read_kml(source, sink);
//     let geo = df.column("GEOMETRY").unwrap().as_materialized_series();
//     let area = geodesic_area_signed(&[geo.clone()]).unwrap();
//     let center = centroid(&[geo.clone()]).unwrap();
//     let df = df.with_column(area.into_column()).unwrap();
//     let df = df.with_column(center.into_column()).unwrap();

//     eprintln!("{}", df);
// }



enum GeomOpResult {
    Null,
    Point(Point),
    MultiPoint(MultiPoint),
    Line(Line),
    LineString(LineString),
    MultiLineString(MultiLineString),
    Polygon(Polygon),
    MultiPolygon(MultiPolygon),
    Float(f64),
}

impl From<Point> for GeomOpResult {
    fn from(p: Point) -> Self {
        GeomOpResult::Point(p)
    }
}
impl From<MultiPoint> for GeomOpResult {
    fn from(mp: MultiPoint) -> Self {
        GeomOpResult::MultiPoint(mp)
    }
}
impl From<Line> for GeomOpResult {
    fn from(x: Line) -> Self {
        GeomOpResult::Line(x)
    }
}
impl From<LineString> for GeomOpResult {
    fn from(x: LineString) -> Self {
        GeomOpResult::LineString(x)
    }
}
impl From<MultiLineString> for GeomOpResult {
    fn from(x: MultiLineString) -> Self {
        GeomOpResult::MultiLineString(x)
    }
}
impl From<Polygon> for GeomOpResult {
    fn from(x: Polygon) -> Self {
        GeomOpResult::Polygon(x)
    }
}
impl From<MultiPolygon> for GeomOpResult {
    fn from(x: MultiPolygon) -> Self {
        GeomOpResult::MultiPolygon(x)
    }
}
impl From<f64> for GeomOpResult {
    fn from(value: f64) -> Self {
        GeomOpResult::Float(value)
    }
}

fn chunked_to_linestrings(ca: &ChunkedArray<ListType>) -> Vec<LineString> {
    ca.amortized_iter()
        .map(|s2| match s2 {
            Some(s2) => {
                let s2 = s2.as_ref();
                let ca_points = s2.array().unwrap();
                let points: Vec<(f64, f64)> = ca_points
                    .amortized_iter()
                    .filter_map(|s3| match s3 {
                        Some(s3) => {
                            let s3 = s3.as_ref();
                            let ca_coords = s3.f64().unwrap();
                            Some((ca_coords.get(0).unwrap(), ca_coords.get(1).unwrap()))
                        }
                        None => None,
                    })
                    .collect();
                LineString::from(points)
            }
            None => {
                let empty: Vec<(f64, f64)> = vec![];
                LineString::from(empty)
            }
        })
        .collect()
}
fn chunked_to_polygon(ca: &ChunkedArray<ListType>) -> Polygon {
    let mut linestrings = chunked_to_linestrings(ca);
    let exterior = linestrings.remove(0);
    let geo_poly = Polygon::new(exterior, linestrings);
    geo_poly.orient(Direction::Default)
}

fn polygon_fn<'a, F, T>(poly_chunked: &'a ListChunked, f: F) -> Series
where
    F: Fn(Polygon) -> T,
    T: Into<GeomOpResult>,
{
    let poly_len = poly_chunked.len();
    let mut builder = Builder::new(poly_len);
    poly_chunked.amortized_iter().for_each(|s1| match s1 {
        Some(s1) => {
            let inner_list = s1.as_ref().list().unwrap();
            let oriented = chunked_to_polygon(inner_list);
            builder.add(f(oriented).into());
        }
        None => {
            builder.add_null();
        }
    });
    builder.finish(PlSmallStr::EMPTY)
}
pub fn float_output(fields: &[Field]) -> PolarsResult<Field> {
    FieldsMapper::new(fields).map_to_float_dtype()
}
#[polars_expr(output_type_func=float_output)]
fn geodesic_area_signed(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca_struct = s.struct_()?;

    let geometries = ca_struct.fields_as_series();
    let polygon = geometries.iter().find_map(|s| {
        (s.name().contains("POLYGON") && !s.name().contains("MULTIPOLYGON")).then_some(s)
    });

    match polygon {
        Some(polygon) => {
            let poly_chunked = polygon.list()?;
            let area = polygon_fn(poly_chunked, |poly: Polygon| poly.geodesic_area_signed());
            Ok(area.into_series().with_name("Area".into()))
        }
        None => Ok(Series::full_null(
            "Area".into(),
            inputs[0].len(),
            &DataType::Float64,
        )),
    }
}
pub fn point_2d_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        PlSmallStr::from_static("point_2d"),
        DataType::Array(Box::new(DataType::Float64), 2),
    ))
}

#[polars_expr(output_type_func=point_2d_output)]
fn centroid(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca_struct = s.struct_()?;
    let geometries = ca_struct.fields_as_series();
    let polygon = geometries.iter().find_map(|s| {
        (s.name().contains("POLYGON") && !s.name().contains("MULTIPOLYGON")).then_some(s)
    });
    match polygon {
        Some(polygon) => {
            let poly_chunked = polygon.list()?;
            let centers = polygon_fn(poly_chunked, |poly: Polygon| match poly.centroid() {
                Some(center) => center.into(),
                None => GeomOpResult::Null,
            });
            Ok(centers.with_name("Centroid".into()))
        }
        None => Ok(Series::full_null(
            "Centroid".into(),
            inputs[0].len(),
            &DataType::Float64,
        )),
    }
}
