use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use crate::ops::run_op_on_struct;
use geo::{Area, Centroid, ChaikinSmoothing, InteriorPoint,HaversineClosestPoint,Distance,Closest, ClosestPoint, GeodesicArea, Point};
use serde::Deserialize;

pub fn float_output(fields: &[Field]) -> PolarsResult<Field> {
    FieldsMapper::new(fields).map_to_float_dtype()
}
pub fn point_2d_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        PlSmallStr::from_static("point_2d"),
        DataType::Array(Box::new(DataType::Float64), 2),
    ))
}

#[polars_expr(output_type_func=float_output)]
fn geodesic_perimeter(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.geodesic_perimeter(),
    )
}

#[polars_expr(output_type_func=float_output)]
fn geodesic_area_signed(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.geodesic_area_signed(),
    )
}

#[polars_expr(output_type_func=float_output)]
fn geodesic_area_unsigned(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.geodesic_area_signed(),
    )
}

#[polars_expr(output_type_func=float_output)]
fn signed_area(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.signed_area(),
    )
}



#[polars_expr(output_type_func=float_output)]
fn unsigned_area(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.unsigned_area(),
    )
}
#[derive(Deserialize)]
struct OtherPointKwarg {
    x: f64,
    y: f64
}


#[polars_expr(output_type_func=point_2d_output)]
fn closest_point(inputs: &[Series], kwargs: OtherPointKwarg) -> PolarsResult<Series> {
    run_op_on_struct(inputs, |g| {
        let other_point: Point = (kwargs.x, kwargs.y).into();
        let closest = g.closest_point(&other_point);
        match closest {
            Closest::Indeterminate=>None,
            Closest::Intersection(p)| Closest::SinglePoint(p)=>Some(p)
        }
    }
    )
}

#[polars_expr(output_type_func=point_2d_output)]
fn haversine_closest_point(inputs: &[Series], kwargs: OtherPointKwarg) -> PolarsResult<Series> {
    run_op_on_struct(inputs, |g| {
        let other_point: Point = (kwargs.x, kwargs.y).into();
        let closest = g.haversine_closest_point(&other_point);
        match closest {
            Closest::Indeterminate=>None,
            Closest::Intersection(p)| Closest::SinglePoint(p)=>Some(p)
        }
    }
    )
}

#[polars_expr(output_type_func=point_2d_output)]
fn centroid(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(inputs, |g| g.centroid())
}

#[polars_expr(output_type_func=point_2d_output)]
fn interior_point(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(inputs, |g| g.interior_point())
}


