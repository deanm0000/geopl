
// Might be a big deal that we have to know the output type in advance. 
// One idea is to output as a struct of list(list(list(array))) and a uint8
// where the uint8 would be a code for the geometry type and there might
// just need to be redundant lists to keep things consistent
#[derive(Deserialize)]
struct ChaikinSmoothingKwargs {
    n_iterations: usize
}
#[polars_expr(output_type_func=float_output)]
fn chaikin_smoothing(inputs: &[Series], kwargs: ChaikinSmoothingKwargs) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.chaikin_smoothing(kwargs.n_iterations),
    )
}


// Need to make run_op_on_struct for two inputs
#[polars_expr(output_type_func=float_output)]
fn distance(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.distance(),
    )
}

// Need to make run_op_on_struct for two inputs
#[polars_expr(output_type_func=float_output)]
fn intersects(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.distance(),
    )
}