mod kmz;
mod ops;
use kmz::read_kml;
mod exprs;
use pyo3::prelude::*;
use pyo3_polars::{PolarsAllocator, PyDataFrame};

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();

#[pyfunction]
#[pyo3(signature=(path))]
fn read_kmz(path: &str) -> PyResult<PyDataFrame> {
    let df = read_kml(path.to_string(), None);
    Ok(PyDataFrame(df))
}

#[pymodule]
#[pyo3(name = "_geopl")]
fn _geopl(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_kmz, m)?)?;
    Ok(())
}

