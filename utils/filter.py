import fiona
import pandas as pd
from osgeo import gdal
import geopandas as gpd
from pathlib import Path
from datetime import datetime
from shapely.geometry import MultiPolygon
from typing import Any, Dict, Optional, Sequence, Set, Union


gdal.UseExceptions()
BASE = Union[int, float, str, type(None)]
ORDR = Union[int, float, datetime]


def alter_data(
        src_path: Union[str, Path],
        layer: Optional[Union[int, str]] = 0,
        allowed_drivers: Optional[Sequence[str]] = None,
        open_options: Union[Sequence[str], Dict[str, Any]] = None,
        update_statement: Optional[str] = None,
        dialect: Optional[str] = "SQLITE",
):
    kwargs = dict()
    kwargs["utf8_path"] = src_path
    kwargs["nOpenFlags"] = (gdal.OF_VECTOR | gdal.GA_Update)
    if allowed_drivers:
        kwargs["allowed_drivers"] = allowed_drivers
    if open_options:
        kwargs["open_options"] = open_options
    if isinstance(layer, int):
        layer = fiona.listlayers(src_path)[layer]
    update_statement = update_statement.format(layer=layer).strip()
    with gdal.OpenEx(**kwargs) as data_sink:
        if update_statement:
            data_sink.ExecuteSQL(
                statement=update_statement,
                dialect=dialect,
                keep_ref_on_ds=True
            )
    return src_path


def filter_data(
        src_path: Union[str, Path],
        dst_path: Union[str, Path],
        src_drivers: Optional[Sequence[str]] = None,
        dst_driver: Optional[str] = None,
        open_options: Optional[Union[Sequence[str], Dict[str, Any]]] = None,
        filter_query: Optional[str] = None,
        dialect: Optional[str] = "SQLITE"
):

    kwargs = dict()
    src_path = Path(src_path).expanduser().absolute()
    dst_path = Path(dst_path).expanduser().absolute()
    layers = {
        f"layer_{idx}": layer_name
        for idx, layer_name in enumerate(fiona.listlayers(src_path))
    }
    layers["layer"] = layers["layer_0"]
    filter_query = filter_query.format(**layers).strip()
    kwargs["utf8_path"] = src_path
    kwargs["nOpenFlags"] = gdal.OF_VECTOR
    if src_drivers:
        kwargs["allowed_drivers"] = src_drivers
    if open_options:
        kwargs["open_options"] = open_options
    with gdal.OpenEx(**kwargs) as data_sink:
        if dst_driver is None:
            dst_driver = data_sink.GetDriver()
        with data_sink.ExecuteSQL(
            statement=filter_query,
            dialect=dialect,
            keep_ref_on_ds=True
        ) as query_sink:
            gdal.VectorTranslate(
                destNameOrDestDS=str(dst_path),
                srcDS=query_sink,
                options=gdal.VectorTranslateOptions(
                    format=dst_driver
                )
            )
    return dst_path


def prepare_data(
        src_path: Union[str, Path],
        geom_types: Optional[Union[Set[str], Sequence[str]]] = None,  # {"Polygon", "MultiPolygon", "Unknown"}
        infer_datetime: Optional[Dict[str, str]] = None,  # {"attr": "%Y-%m-%d"}
        new_attr: Optional[str] = None,  # "DN"
        infer_attr: Optional[str] = None,
        value_map: Union[Dict[BASE, BASE], BASE] = None,
        default_fill: Optional[BASE] = None,
        sort_by: Optional[Union[str, Sequence[str]]] = None,
        sort_asc: Optional[bool] = True,
        dst_path: Optional[Union[str, Path]] = None,  # None => Inplace
        dst_driver: Optional[str] = None  # None => Same driver as source
) -> Path:
    layer_names = fiona.listlayers(src_path)
    for layer_name in layer_names:
        gdf = gpd.read_file(src_path, layer=layer_name)
        if geom_types is not None:
            gdf = gdf[gdf.geom_type.isin(geom_types)]
        if infer_datetime:
            for attr, fmt in infer_datetime.items():
                gdf[attr] = pd.to_datetime(
                    gdf[attr],
                    format=fmt,
                    errors='coerce'
                )
        if gdf.empty:
            dummy = gpd.GeoDataFrame(
                data={'geometry': [MultiPolygon()]},
                geometry='geometry',
                crs=gdf.crs
            )
            gdf = pd.concat(objs=[gdf, dummy], ignore_index=True)
        if new_attr is not None:
            if isinstance(value_map, dict):
                gdf[new_attr] = gdf[infer_attr].map(value_map).fillna(default_fill)
            else:
                gdf[new_attr] = value_map
        if sort_by is not None:
            gdf = gdf.sort_values(by=sort_by, ascending=sort_asc)
        if dst_path is None:
            dst_path = src_path
        gdf.to_file(filename=dst_path, layer=layer_name, driver=dst_driver)
    return dst_path
