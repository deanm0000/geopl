use geo::{
    Centroid, GeodesicArea, Geometry, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Orient,
    Point, Polygon, orient::Direction,
};
use polars::chunked_array::builder::get_list_builder;
use polars::prelude::*;
use polars::prelude::*;
use polars_arrow::array::{
    MutableArray, MutableFixedSizeListArray, MutablePrimitiveArray, TryPush,
};

pub fn points_to_series(p: &[Point]) -> PolarsResult<Series> {
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
pub enum GeomTypes {
    Point,
    MultiPoint,
    Line,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
}
pub enum GeomOpResult {
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
impl<T> From<Option<T>> for GeomOpResult
where
    T: Into<GeomOpResult>,
{
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(value) => value.into(),
            None => GeomOpResult::Null,
        }
    }
}
impl From<Geometry> for GeomOpResult {
    fn from(x: Geometry) -> Self {
        match x {
            Geometry::Point(p)=>GeomOpResult::Point(p),
            Geometry::MultiPoint(p)=>GeomOpResult::MultiPoint(p),
            Geometry::Line(p)=>GeomOpResult::Line(p),
            Geometry::LineString(p)=>GeomOpResult::LineString(p),
            Geometry::MultiLineString(p)=>GeomOpResult::MultiLineString(p),
            Geometry::Polygon(p)=>GeomOpResult::Polygon(p),
            Geometry::MultiPolygon(p)=>GeomOpResult::MultiPolygon(p),
            _=>panic!("missing geom type from geoopresult {:?}",x)
        }
    }
}


pub enum Builder {
    Pending((usize, usize)),
    Scalar(PrimitiveChunkedBuilder<Float64Type>),
    Point(MutableFixedSizeListArray<MutablePrimitiveArray<f64>>),
    MultiPoint(Box<dyn ListBuilderTrait>),
    LineString(Box<dyn ListBuilderTrait>),
    MultiLineString(Box<dyn ListBuilderTrait>),
    Polygon(Box<dyn ListBuilderTrait>),
    MultiPolygon(Box<dyn ListBuilderTrait>),
}

impl Builder {
    pub fn new(size: usize) -> Builder {
        Builder::Pending((size, 0))
    }
    pub fn make_scalar(&mut self) {
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
    pub fn make_point(&mut self) {
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
    pub fn make_list_arr(&mut self, geom: GeomTypes) {
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
    pub fn make_list_list_arr(&mut self, geom: GeomTypes) {
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
    pub fn new_list_list_list_arr(&mut self, geom: GeomTypes) {
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
    pub fn add_scalar(&mut self, p: f64) {
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
    pub fn add_point(&mut self, p: Point) {
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
    pub fn add_multipoint(&mut self, p: MultiPoint) {
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
    pub fn add_line(&mut self, p: Line) {
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
    pub fn add_linestring(&mut self, p: LineString) {
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
    pub fn add_multi_line_string(&mut self, p: MultiLineString) {
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
                let s = lil_builder.finish();
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    pub fn add_polygon(&mut self, p: Polygon) {
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
                let s = inner_lines.finish();
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    pub fn add_multi_polygon(&mut self, p: MultiPolygon) {
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
                let s = inner_polys.finish();
                builder.append_series(&s).unwrap();
            }
            _ => unimplemented!(),
        }
    }
    pub fn add_null(&mut self) {
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
            }
            Builder::Scalar(builder) => builder.append_null(),
            _ => unimplemented!(),
        }
    }
    pub fn add(&mut self, value: GeomOpResult) {
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
    pub fn finish(self) -> Series {
        match self {
            Builder::Point(mut builder) => Series::from_arrow(PlSmallStr::EMPTY, builder.as_box()).unwrap(),
            Builder::MultiPoint(mut builder)
            | Builder::LineString(mut builder)
            | Builder::MultiLineString(mut builder)
            | Builder::Polygon(mut builder)
            | Builder::MultiPolygon(mut builder) => builder.finish().into_series(),
            Builder::Scalar(builder) => builder.finish().into_series(),
            Builder::Pending(_) => panic!("can't finish pending builder"),
        }
    }
}
