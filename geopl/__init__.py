from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

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
    def __init__(self, column: str):
        self.__column = column

    def change_column(self, column: str):
        """
        By default the class uses the column "GEOMETRY".

        This changes the column"""
        self.__column = column

    def check_column(self):
        """
        Check which column the class will operate on."""
        return self.__column

    def geodesic_area_signed(self) -> pl.Expr:
        """
        Determine the area of a geometry on an ellipsoidal model of the earth.

        This uses the geodesic measurement methods given by Karney (2013).

        Returns:meter²

        """
        expr = pl.col(self.__column)
        return register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="geodesic_area_signed",
            args=[expr],
            is_elementwise=True,
        )

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
        return register_plugin_function(
            plugin_path=Path(__file__).parent,
            function_name="centroid",
            args=[expr],
            is_elementwise=True,
        )


geo = Geo("GEOMETRY")

__all__ = ["geo", "Geo", "read_kmz"]
