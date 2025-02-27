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

Right now, the only thing this can do is, from the command line, read a KMZ file and save it to a parquet file. It ignores all attributes and styles. It captures only geometries, names, and descriptions.

For example:

```shell
geopl my_file.kmz df.parquet
```

## Rough order (completely subject to change without notice) of future work.
1. Add a few calculations from GeoRust (ie area and distance) for existing df
2. Python bindings (ie. `def read_kmz(path)->pl.DataFrame` and `geometry.area() -> Expr`)
3. More calculations from [here](https://docs.rs/geo/latest/geo/)
4. Proj/crs implementation
5. [Spatial indexes](https://docs.rs/rstar/0.12.0/rstar/struct.RTree.html#usage)/joins 
6. Save geospatial files
7. Query from PostGIS
8. Insert/copy to PostGIS