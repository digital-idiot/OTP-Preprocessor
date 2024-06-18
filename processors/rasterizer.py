import fiona
import numpy as np
from osgeo import gdal
import rasterio as rio
import geopandas as gpd
from pathlib import Path
from numbers import Number
from utils.meta import RAT
from itertools import product
from utils import reproject_bbox
from rich.progress import Progress
from rasterio.enums import MergeAlg
from rasterio.windows import Window
from rasterio.features import rasterize
from typing import Iterable, Literal, Optional, Tuple, Union


gdal.UseExceptions()


def rasterize_layers(
        gdf_list: Tuple[gpd.GeoDataFrame],
        attr: str,
        src_image: Union[str, Path],
        dst_image: Union[str, Path],
        mode: Optional[Literal["burn", "stack"]] = "burn",
        tile_size: Optional[Tuple[int, int]] = None,
        fill: Optional[Number] = 0,
        all_touched: Optional[bool] = False,
        merge_alg: MergeAlg = MergeAlg.replace,
        default_value: Optional[Number] = 1,
        dtype: Optional[np.dtype] = np.int32,
        dst_driver: Optional[str] = "GTiff",
        color_maps: Optional[Tuple[dict]] = None,
        rats: Optional[Iterable[RAT]] = None,
        **dst_options
) -> None:
    """
    Rasterize a list of GeoDataFrames to an image.
    Args:
        gdf_list (Tuple[gpd.GeoDataFrame]): List of GeoDataFrames to rasterize.
        attr (str): Attribute to rasterize.
        src_image (Union[str, Path]): Source image to get the metadata from.
        dst_image (Union[str, Path]): Destination image to write the rasterized
            data to.
        mode (Optional[Literal["burn", "stack"]]): Mode of rasterization.
        tile_size (Optional[Tuple[int, int]]): Size of the tiles to rasterize.
        fill (Optional[Number]): Fill value for the rasterized data.
        all_touched (Optional[bool]): Whether to rasterize all pixels touched
            by the corresponding feature.
        merge_alg (MergeAlg): Algorithm to use for merging the rasterized data.
        default_value (Optional[Number]): Default value for the rasterized data.
        dtype (Optional[np.dtype]): Data type of the rasterized data.
        dst_driver (Optional[str]): Driver to use for writing the rasterized
            image.
        color_maps (Optional[Tuple[dict]]): Color maps for the rasterized data.
        rats (Optional[Iterable[RAT]]): Raster Attribute Tables with semantics
            for the rasterized image.
        **dst_options (Any): Arbitrary keyword arguments specifying additional
            options.
    Returns:
        None

    """
    with rio.open(src_image, mode='r') as src:
        dst_options["height"] = src.height
        dst_options["width"] = src.width
        dst_options["crs"] = src.crs
        dst_options["transform"] = src.transform
    raster_window = Window(
        row_off=0,
        col_off=0,
        height=dst_options["height"],
        width=dst_options["width"]
    )

    dst_options["driver"] = dst_driver
    dst_options["dtype"] = dtype
    dst_options["nodata"] = fill

    if mode.lower() == "stack":
        dst_options["count"] = len(gdf_list)
    elif mode.lower() == "burn":
        dst_options["count"] = 1
    else:
        raise ValueError(f"Unknown mode: {mode}")

    gdf_list = [
        gdf[["geometry", attr]].to_crs(dst_options["crs"])
        for gdf in gdf_list
    ]
    ###
    import warnings
    with warnings.catch_warnings(record=True) as w:
        for g in gdf_list:
            g = g[attr].to_numpy()
            g.astype(dtype)
            if w:
                for warning in w:
                    if issubclass(warning.category, RuntimeWarning):
                        print(g)
                        raise ValueError(warning.message)
    ###
    with rio.open(dst_image, mode='w', **dst_options) as dst:
        if tile_size is None:
            arr = np.full(
                shape=(dst.count, dst.height, dst.width),
                dtype=dst_options["dtype"],
                fill_value=fill
            )
            for idx, gdf in enumerate(gdf_list):
                if len(gdf) > 0:
                    arr[idx % dst.count] = rasterize(
                        shapes=gdf.itertuples(index=False, name=None),
                        out_shape=(dst.height, dst.width),
                        transform=dst.transform,
                        fill=fill,
                        out=arr[idx % dst.count],
                        all_touched=all_touched,
                        merge_alg=merge_alg,
                        default_value=default_value,
                        dtype=dst_options["dtype"]
                    )
            dst.write(arr=arr)
        else:
            for rf, cf in product(
                range(0, dst.height, tile_size[0]),
                range(0, dst.width, tile_size[1])
            ):
                tile_window = Window(
                    row_off=rf,
                    col_off=cf,
                    height=tile_size[0],
                    width=tile_size[1]
                ).intersection(raster_window)
                tile_transform = rio.windows.transform(
                    window=tile_window,
                    transform=dst.transform
                )
                arr = np.full(
                    shape=(dst.count, tile_window.height, tile_window.width),
                    dtype=dst_options["dtype"],
                    fill_value=fill
                )
                for idx, gdf in enumerate(gdf_list):
                    if len(gdf) > 0:
                        arr[idx % dst.count] = rasterize(
                            shapes=gdf.itertuples(index=False, name=None),
                            out_shape=(tile_window.height, tile_window.width),
                            transform=tile_transform,
                            fill=fill,
                            out=arr[idx % dst.count],
                            all_touched=all_touched,
                            merge_alg=merge_alg,
                            default_value=default_value,
                            dtype=dst_options["dtype"],
                            # skip_invalid=skip_invalid
                        )
                dst.write(arr=arr, window=tile_window)
        if isinstance(color_maps, Iterable):
            for i, cm in enumerate(color_maps):
                if cm:
                    dst.write_colormap(bidx=i+1, colormap=cm)

    if isinstance(rats, Iterable):
        dataset = gdal.Open(str(dst_image), gdal.GA_Update)
        for j, rat in enumerate(rats):
            band = dataset.GetRasterBand(j+1)
            band.SetDefaultRAT(rat())
        dataset.FlushCache()
        # noinspection PyUnusedLocal
        band = None
        # noinspection PyUnusedLocal
        dataset = None


def make_labels(
        vectors: Iterable[Tuple[Path, str]],
        attr: str,
        image_paths: Iterable[Path],
        dst_dir: Path,
        mode: Optional[Literal["burn", "stack"]] = "burn",
        tile_size: Optional[Tuple[int, int]] = None,
        fill: Optional[Number] = 0,
        all_touched: Optional[bool] = False,
        merge_alg: MergeAlg = MergeAlg.replace,
        default_value: Optional[Number] = 1,
        dtype: Optional[np.dtype] = np.int32,
        dst_driver: Optional[str] = "GTiff",
        color_maps: Optional[Tuple[dict]] = None,
        rats: Optional[Iterable[RAT]] = None,
        progress_desc: Optional[str] = "Rasterizing:",
        progress_host: Optional[Progress] = None,
        **dst_options
) -> None:
    progress_desc = "Rasterizing:" if progress_desc is None else progress_desc
    crs_list = [
        fiona.open(src_path, mode='r', layer=layer).crs
        for src_path, layer in vectors
    ]
    image_paths = tuple(image_paths)
    if progress_host is not None:
        task = progress_host.add_task(
            description=progress_desc,
            total=len(image_paths)
        )
    else:
        task = None
    for image_path in image_paths:
        with rio.open(image_path, mode='r') as img_src:
            img_crs = img_src.crs
            img_bbox = img_src.bounds

        gdf_list = [
            gpd.read_file(
                filename=src_path,
                bbox=reproject_bbox(
                    bbox=img_bbox,
                    src_crs=img_crs,
                    dst_crs=crs_list[i]
                ),
                layer=layer
            )
            for i, (src_path, layer) in enumerate(vectors)
        ]
        dst_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        dst_path = dst_dir / f"{image_path.stem}.{dst_driver.lower()}"
        rasterize_layers(
            gdf_list=gdf_list,
            attr=attr,
            src_image=image_path,
            dst_image=dst_path,
            mode=mode,
            tile_size=tile_size,
            fill=fill,
            all_touched=all_touched,
            merge_alg=merge_alg,
            default_value=default_value,
            dtype=dtype,
            dst_driver=dst_driver,
            color_maps=color_maps,
            rats=rats,
            **dst_options
        )
        if task is not None:
            if not progress_host.tasks[task].finished:
                progress_host.update(task, advance=1)
            else:
                progress_host.remove_task(task)
