from __future__ import annotations

from pathlib import Path


import polars as pl
from polars.plugins import register_plugin_function
import geopl._geopl as geopl  # type: ignore


def read_kmz(path: str) -> pl.DataFrame:
    """Read a kmz or kml file into a df.

    This only captures the name and description columns.
    It also ignores any coordinate dimensions above 2.

    Args:
        path (str): Path to file

    Returns:
        DataFrame
    """
    return geopl.read_kmz(path)


class Geo:
    def __init__(self, geometry_column: str, func_as_output=True):
        self.__column = geometry_column
        self.__func_as_output = func_as_output

    def set_func_as_output(self, func_as_output):
        """
        Method to change the naming behavior of methods called from this class.

        Args:
            func_as_output: True if methods should return a column named after them
        """
        self.__func_as_output = func_as_output

    def change_column(self, column: str):
        """
        By default the class uses the column "GEOMETRY".

        This changes the column"""
        self.__column = column

    def check_column(self):
        """
        Check which column the class will operate on."""
        return self.__column

    def geodesic_perimeter(self) -> pl.Expr:
        """
        Determine the perimeter of a geometry on an ellipsoidal model of the earth.

        This uses the geodesic measurement methods given by Karney (2013).

        For a polygon this returns the sum of the perimeter of the exterior ring and interior
        rings. To get the perimeter of just the exterior ring of a polygon, do
        polygon.exterior().geodesic_length().
        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="geodesic_perimeter",
            args=[expr],
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("geodesic_perimeter")
        else:
            return plugin

    def geodesic_area_signed(self) -> pl.Expr:
        """
        Determine the area of a geometry on an ellipsoidal model of the earth.

        This uses the geodesic measurement methods given by Karney (2013).

        Returns:meter²

        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="geodesic_area_signed",
            args=[expr],
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("geodesic_area_signed")
        else:
            return plugin

    def geodesic_area_unsigned(self) -> pl.Expr:
        """
        Determine the perimeter and area of a geometry on an ellipsoidal model of the earth,
        all in one operation. Supports very large geometries that cover a significant portion of the earth.

        This returns the perimeter and area in a (perimeter, area) tuple and uses the geodesic measurement
        methods given by Karney (2013).

        Area Assumptions
        Polygons are assumed to be wound in a counter-clockwise direction for the exterior ring and a clockwise
        direction for interior rings. This is the standard winding for Geometries that follow the Simple Features
        standard. Using alternative windings will result in incorrect results.

        Units
        return value: (meter, meter²)

        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="geodesic_area_unsigned",
            args=[expr],
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("geodesic_area_unsigned")
        else:
            return plugin

    def signed_area(self) -> pl.Expr:
        """
        signed planar area of a geometry
        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="signed_area",
            args=[expr],
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("signed_area")
        else:
            return plugin

    def unsigned_area(self) -> pl.Expr:
        """
        unsigned planar area of a geometry
        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="unsigned_area",
            args=[expr],
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("unsigned_area")
        else:
            return plugin

    def closest_point(self, *, x, y) -> pl.Expr:
        """
        The result of trying to find the closest spot on an object to a point.

        TODO: make this work with two geometry column inputs.

        Right now it works with the column as given plus a fixed point input by
        x and y.

        The rust implementation of this denotes if a point is an intersection, singlepoint, or indeterminate.

        In this implementation that info is discarded.
        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="closest_point",
            args=[expr],
            kwargs={"x": x, "y": y},
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("closest_point")
        else:
            return plugin

    def haversine_closest_point(self, *, x, y) -> pl.Expr:
        """
        Calculate the closest point on a Great Circle arc geometry to a given point.

        TODO: make this work with two geometry column inputs.

        Right now it works with the column as given plus a fixed point input by
        x and y.

        The rust implementation of this denotes if a point is an intersection, singlepoint, or indeterminate.

        In this implementation that info is discarded.
        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="haversine_closest_point",
            args=[expr],
            kwargs={"x": x, "y": y},
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("haversine_closest_point")
        else:
            return plugin

    def centroid(self) -> pl.Expr:
        """
        Calculation of the centroid.

        The centroid is the arithmetic mean position of all points in the shape.
        Informally, it is the point at which a cutout of the shape could be
        perfectly balanced on the tip of a pin. The geometric centroid of a
        convex object always lies in the object. A non-convex object might have
        a centroid that is outside the object itself.
        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="centroid",
            args=[expr],
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("haversine_closest_point")
        else:
            return plugin

    def interior_point(self) -> pl.Expr:
        """
        Calculation of interior points. An interior point is a point that’s guaranteed
        to intersect a given geometry, and will be strictly on the interior of the geometry if
        possible, or on the edge if the geometry has zero area. A best effort will additionally
        be made to locate the point reasonably centrally.
        """
        expr = pl.col(self.__column)
        plugin = register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="interior_point",
            args=[expr],
            is_elementwise=True,
        )
        if self.__func_as_output:
            return plugin.alias("haversine_closest_point")
        else:
            return plugin


geo = Geo("GEOMETRY")

__all__ = ["geo", "Geo", "read_kmz"]
