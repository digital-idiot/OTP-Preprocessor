from pyproj import CRS
from typing import Tuple
from numbers import Number
from pyproj import Transformer


def reproject_bbox(
        bbox: Tuple[Number, Number, Number, Number],
        src_crs: CRS,
        dst_crs: CRS
):
    transformer = Transformer.from_crs(
        crs_from=src_crs,
        crs_to=dst_crs,
        always_xy=True
    )
    minx, miny = transformer.transform(bbox[0], bbox[1])
    maxx, maxy = transformer.transform(bbox[2], bbox[3])
    return minx, miny, maxx, maxy
