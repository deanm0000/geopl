use polars::datatypes::PlSmallStr;
#[derive(Clone)]
pub(crate) enum LineKind {
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
pub(crate) enum MultiLineKind {
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