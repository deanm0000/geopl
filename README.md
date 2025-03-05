## GeoPL

GeoGL isn't much of anything yet but one day I want it to be a full (or nearly so) replacement of geopandas but for Polars. Right now it can read a KMZ/KML file and save it as a Parquet file from the command line. 

## Goals
1. To have a working geospatial extension for Polars with its current dtypes (no wkt or wkb)
2. Python bindings
3. Most of the functionality offered by GeoRust
4. Spatial indexing and joins
5. proj transformations 
6. PostGIS queries/updates

## Non-Goals
1. Creating a standard, I just want geospatial in Polars without waiting for extra dtypes, I'm not trying to compete with GeoArrow, just making polars+geospatial.
2. Interoperability with geopandas, the point (for me) is to replace it entirely
3. Three or greater dimensions within the coordinates (at least not anytime soon)

## Why not GeoArrow interoperability
GeoArrow requires two datatypes which Polars doesn't support, Extension Types and Union Types. 

[Extension Types](https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types) allow some metadata to be attached to the Array (or Column). In the case of geospatial, it would contain metadata on which type of Geometry it is, for example to distinguish a Polygon from a MultiLineString. It also allows for CRS data to be stored.

[Union Types](https://wesm.github.io/arrow-site-test/format/Layout.html) allow for one Column to have multiple types so that a Geometry column can have any combination of geometry types within it.

I did some experimenting with going from polars to geoarrow but it seems to reconcile the above would have meant frequent copying data from one form to another which I thought

## Fitting Geospatial data to Polars Data Types.
In order to work around the above limitations, we can use a Struct as a Union and use the field names within the struct as the metadata. For example a Polars schema could look like this:

```python
Schema(
    [
        ("Name", String),
        ("Description", String),
        (
            "GEOMETRY",
            Struct(
                {
                    "POINT": Array(Float64, shape=(2,)),
                    "LINESTRING": List(Array(Float64, shape=(2,))),
                    "MULTILINESTRING": List(List(Array(Float64, shape=(2,)))),
                    "POLYGON": List(List(Array(Float64, shape=(2,)))),
                    "MULTIPOLYGON": List(List(List(Array(Float64, shape=(2,))))),
                }
            ),
        ),
    ]
)
```

I think for most use cases it is sufficient to use EPSG codes without using an entire CRS or proj4 string. Those codes can be appended to the field name, for example `"POLYGON:EPSG:4326"`. Even with full CRS strings, it appears to be possible to append that to the field name as Polars doesn't complain at `df.columns=["".join(["a" for _ in range(100000)])]` or at `df.columns=["".join([chr(x) for x in range(128)])]` so the only real limitation (famous last words) is that it feels kinda stupid but... ![](https://media1.tenor.com/m/CzpafO9hVaYAAAAd/its-not-stupid-if-it-works-alina.gif)



```python
Struct(
    {
        "type": List(UInt8),
        "geometries": List(List(List(List(Array(Float64, shape=(2,))))))
    }
)
```

The length of `type` and the outermost `geometries` would be the same.

Another idea is to return another struct but this one would have to have a field for every possible type, not just the ones being used. 



## What can it do RIGHT NOW?

<s>Right now, the only thing this can do is, from the command line, read a KMZ file and save it to a parquet file. It ignores all attributes and styles. It captures only geometries, names, and descriptions.</s>

Python bindings are setup with a bunch of geo algos that are implemented on all Geometry types. 

```python
from geopl import geo, read_kmz
import polars as pl

df = read_kmz(some_path)
print(df.with_columns(
    area = geo.geodesic_area_signed(), 
    centroid = geo.centroid()
    interior = geo.interior_point()
    ))
```

## Beginnings of real documentation

### Expressions
The expressions are all in a class with an initialized instance called `geo`. It defaults to using "GEOMETRY" as the column name. This makes it easier to call expressions. For example to get the geodesic_area_signed and centroid you'd do.

```python
from geopl import geo
df.with_columns(
    area = geo.geodesic_area_signed(), 
    centroid = geo.centroid()
    )
```

Further, the methods of `geo`, by default, will name their output by the function that is called.

```python
df.with_columns(geo.geodesic_area_signed())
```
will return a column called geodesic_area_signed.

This can be turned off with `geo.set_func_as_output(False)`

If you have multiple Geometry columns and want to change which column geo targets then use `geo.change_column("Other_column_name")` or if you want to do it inline, import the uninitiated class `Geo`

This is another approach

```python
from geopl import Geo
df.with_columns(
    area = Geo("geo_col1").geodesic_area_signed(), 
    centroid = Geo("geo_col2").centroid()
    )
```




## Rough order of future work.  (completely subject to change without notice) 
<s>1. Add a few calculations from GeoRust (ie area and distance) for existing df
a. Made polygon_fn to take a ListChunked and do polygon methods on them
b. Need to do the same for multipolygon
c. Then other geometries
2. Python bindings (ie. `def read_kmz(path)->pl.DataFrame` and `geometry.area() -> Expr`)
* Create a rust struct with a hashmap of all the ChunkedArrays from the struct column. The key will be the column name. The value will be a tuple of the ChunkedArray, and validity bitmap. Iterate over 0..len() using value_unchecked after checking the validity bitmap (I think faster than going through Option).</s>

* Add missing Geometry types (rect and triangle) and handling of more output types (bool, int)
* Handle 2 geometry inputs
* implode/explode operations
* Proj/crs implementation
* [Spatial indexes](https://docs.rs/rstar/0.12.0/rstar/struct.RTree.html#usage)/joins
* More calculations from [here](https://docs.rs/geo/latest/geo/)
* voronoi from [voronator](https://docs.rs/voronator/latest/voronator/)
* Save geospatial files
* Query from PostGIS
* Insert/copy to PostGIS


## Ramblings and some high level infrastructure

The KML crate has a LinearRing geometry but geo-types doesn't have that. Should treat LinearRing as another LineString and not keep track of both.

I separated to_pl and to_geom funcationality which seems to work well so far. TODO: I need to capture all the Geo types, only Rect and Triangle are missing though.

I created an Enum of builders to capture the varying potential builders necessary TODO: add Rect, Triangle, Boolean, and Integer to Builder

```rust
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
```

That enum can impl for new, add (taking a Geometry), and finish to yield a Series.

It is used with this function which can wrap functions that take a geometry and spit out a Series. I can then wrap this function with expr fns for the plugin.

```rust
pub fn run_op_on_struct<'a, F, T>(
    inputs: &[Series],
    f: F,
) -> PolarsResult<Series>
where
    F: Fn(&Geometry) -> T,
    T: Into<GeomOpResult>,
{
    let s = &inputs[0];
    let rows = s.len();
    let geometries = Geos::new(s);
    let mut builder = Builder::new(rows);
    for i in 0..rows {
        match &geometries.get_row(i) {
            Some(geom) => builder.add(f(geom).into()),
            None => builder.add_null(),
        }
    }
    Ok(builder.finish())
}
```

With the above helper (and all the utility functions not seen), constructing the Exprs for the plugin is reduced to

```rust
#[polars_expr(output_type_func=float_output)]
fn geodesic_perimeter(inputs: &[Series]) -> PolarsResult<Series> {
    run_op_on_struct(
        &inputs,
        |g| g.geodesic_perimeter(),
    )
}
```

Going back to the `Geos` struct seen in `run_op_on_struct`, it's just a hashmap where the key is the geometry type and the value is a tuple of the chunkedarray (as an enum) and a hashset of its null indices. With that, I can have an outer loop of row iteration and the inner iteration is over the hashmap where before I try to extract anything from the chunkedarray I can check if its null using the hashset. Since I'm already only iterating on its known height and I know the null values, I can extract from the chunkedarray using the unsafe, but faster, `value_unchecked`. Doing it this way allows for 

### Implode

I think there might need to be two versions of implode. The first would be used like

```python
df.group_by(something).agg(geo.implode())
```

Without the `.implode()` then polars would return a list(struct) which then couldn't be operated on as all the plugin exprs expect a struct not a list(struct). This method would be to put everything in their Multi____ version and keep the struct on the outer most layer.

The other version would be to take in an already collapsed list(struct) and convert it back to struct(Multi).

### Explode

We can't explode from an expression but we need to be able to "pre-explode" to rearrange the struct with Multi___ inside it and rewrite it as list(struct) so then polars can explode it. It might look like:

```python
df.with_columns(geo.explode()).explode("GEOMETRY")
```

Alternatively, it could even just be `geo.explode(df)` using overload and an optional input on `explode`.