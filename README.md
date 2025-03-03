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


## What can it do RIGHT NOW?

<s>Right now, the only thing this can do is, from the command line, read a KMZ file and save it to a parquet file. It ignores all attributes and styles. It captures only geometries, names, and descriptions.</s>

Python bindings are setup with area and centroid functions.

```python
from geopl import geo, read_kmz
import polars as pl

df = read_kmz(some_path)
print(df.with_columns(
    area = geo.geodesic_area_signed(), 
    centroid = geo.centroid()
    ))
```

## Beginnings of real documentation

The expressions are all in a class with an initialized instance called geo. It defaults to using "GEOMETRY" as the column name. This makes it easier to call expressions. For example to get the geodesic_area_signed and centroid you'd do.

```python
from geopl import geo
df.with_columns(
    area = geo.geodesic_area_signed(), 
    centroid = geo.centroid()
    )
```

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
2. Python bindings (ie. `def read_kmz(path)->pl.DataFrame` and `geometry.area() -> Expr`)</s>
* Create a rust struct with a hashmap of all the ChunkedArrays from the struct column. The key will be the column name. The value will be a tuple of the ChunkedArray, and validity bitmap. Iterate over 0..len() using value_unchecked after checking the validity bitmap (I think faster than going through Option).
* More calculations from [here](https://docs.rs/geo/latest/geo/)
* Proj/crs implementation
* [Spatial indexes](https://docs.rs/rstar/0.12.0/rstar/struct.RTree.html#usage)/joins 
* Save geospatial files
* Query from PostGIS
* Insert/copy to PostGIS


## Ramblings and some high level infrastructure

The KML crate has a LinearRing geometry but geo-types doesn't have that. Should treat LinearRing as another LineString and not keep track of both.

Principally what is needed is to be able to round trip from polars format to geo struct format to execute an operation and then convert it back.

Complicating that issue that each operation can return a different type so I need a somewhat extensible way to go in and out of types.

I created an Enum of builders to capture the varying potential builders necessary

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

It is used with this function

```rust
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
```

Notice how that function takes a function as input so it's just a helper. The purpose of it is as a wrapper for the geo operations, for example:

```rust
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
```

The way this is setup so far is that it would need to have more functions similar to `polygon_fn` for the other types. It would then create a result Series for each geometry in the Geometry struct and then it would need to coalesce all of those. When there is only one geometry type then it works well but if there are multiple geometry types/fields it could be problematic. If there are multiple geometry types that aren't null in any row then it just fails to give the right answer. Another idea would be to loop over all the Series of geometry types a bit like the following to create a GeometryCollection for each row. This has the benefit of only creating one result builder and it ensures that if there's ever multiple geometries it's automatically handling them.

```rust
    let s = &inputs[0];
    let s_len = s.len();
    let ca_struct = s.struct_()?;
    let geometries = ca_struct.fields_as_series();
    let mut result_builder = Builder::new(poly_len);
    for i in 0..s_len() {
        let geos:Vec<Geometry> = vec![];
        for geom_series in geometries {
            geos.push(s_to_geom(geom_series));
        }
        let geos=GeometryCollection::new_from(geos);
        result_builder.add(geos.geodesic_area_signed())
    }
```

