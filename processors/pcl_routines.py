import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union, OrderedDict


__all__ = [
    "read_las",
    "make_chips",
    "add_attribute",
    "colorize",
    "map_overlay",
    "write_las",
    "compose_pipeline"
]


def read_las(
        src_las_path: Union[str, Path],
        **read_options: Optional[Dict[str, str]]
) -> Dict[str, str]:
    """
    Make process node to read a LAS / LAZ file using the specified path and
        options.

    Args:
        src_las_path (Union[str, Path]):
        **read_options (Optional[Dict[str, str]]): Arbitrary keyword arguments
            specifying reading options. For details on the available options,
            refer to the PDAL documentation:
            https://pdal.io/en/latest/stages/readers.las.html

    Returns:
        Dict[str, str]: A dictionary representing the node mapping for reading
            the file.
    """
    conf = {
        "type": "readers.las",
        "filename": str(src_las_path)
    }
    if read_options:
        conf.update(read_options)
    return conf


def make_chips(
        capacity: int,
        **chip_options: Optional[Dict[str, str]]
) -> Dict[str, str]:
    """
    Make process node to generate chips out of a point cloud with options.

    Args:
        capacity (int): Max allowed points in a chip (tile).
        **chip_options (Optional[Dict[str, str]]): Arbitrary keyword arguments
            specifying chipping options. For details on the available options,
            refer to the PDAL documentation:
            https://pdal.io/en/latest/stages/filters.chipper.html

    Returns:
        Dict[str, str]: A dictionary representing the node mapping for chipping
            (tiling) the point cloud.
    """
    conf = {
        "type": "filters.chipper",
        "capacity": capacity
    }
    if chip_options:
        conf.update(chip_options)
    return conf


def add_attribute(
        target: str,
        source: Optional[str] = str(),
        **additional_options: Optional[Dict[str, str]]
) -> Dict[str, str]:
    """
    Make process node to add a new attribute (target) to the point cloud
        optionally copying an existing attribute (source) using options.

    Args:
        target (str): Name of the new attribute to be added.
        source (Optional[str]): Name of the existing attribute to be copied
            from.
        **additional_options (Optional[Dict[str, str]]): Arbitrary keyword
            arguments specifying additional options. For details on the
            available options, refer to the PDAL documentation:
            https://pdal.io/en/latest/stages/filters.ferry.html

    Returns:
        Dict[str, str]: A dictionary representing the node mapping for adding
            new attribute to the point cloud.
    """
    conf = {
        "type": "filters.ferry",
        "dimensions": f"{source}=>{target}"
    }
    if additional_options:
        conf.update(additional_options)
    return conf


def colorize(
        raster: Union[str, Path],
        **colorization_options: Optional[Dict[str, str]]
) -> Dict[str, str]:
    """
    Make process node to colorize the point cloud using the specified raster.

    Args:
        raster (Union[str, Path]): Path to the raster file to be used for
            colorization.
        **colorization_options (Optional[Dict[str, str]]): Arbitrary keyword
            arguments specifying additional options. For details on the
            available options, refer to the PDAL documentation:
            https://pdal.io/en/latest/stages/filters.colorization.html

    Returns:
        Dict[str, str]: A dictionary representing the node mapping for
            colorizing the point cloud.
    """
    conf = {
        "type": "filters.colorization",
        "raster": str(raster)
    }
    if colorization_options:
        conf.update(colorization_options)
    return conf


def map_overlay(
        src_map: Union[str, Path],
        src_attribute: str,
        dst_attribute: str,
        **additional_options: Optional[Dict[str, str]]
) -> Dict[str, str]:
    """
    Make process node to overlay a map on the point cloud and copy specified
        attribute.
    Args:
        src_map (Union[str, Path): Path to the GIS map file to be overlayed.
        src_attribute (str): Attribute to be copied from the map.
        dst_attribute (str): Attribute of the point cloud to be inflated.
        **additional_options (Optional[Dict[str, str]]): Arbitrary keyword
            arguments specifying additional options. For details on the
            available options, refer to the PDAL documentation:
            https://pdal.io/en/latest/stages/filters.overlay.html
    Returns:
        Dict[str, str]: A dictionary representing the node mapping for
            the GIS map draping of the point cloud.
    """
    conf = {
        "type": "filters.overlay",
        "column": src_attribute,
        "dimension": dst_attribute,
        "datasource": str(src_map)
    }
    if additional_options:
        conf.update(additional_options)
    return conf


def write_las(
        dst_las_path: Union[str, Path],
        **write_options: Optional[Dict[str, str]]
) -> Dict[str, str]:
    """
    Make process node to write the point cloud to a LAS / LAZ file.
    Args:
        dst_las_path (Union[str, Path): Path to the output LAS / LAZ file.
        **write_options (Optional[Dict[str, str]]): Arbitrary keyword
            arguments specifying writing options. For details on the
            available options, refer to the PDAL documentation:
            https://pdal.io/en/latest/stages/writers.las.html

    Returns:
        Dict[str, str]: A dictionary representing the node mapping for
            writing the point cloud to the file.
    """
    conf = {
        "type": "readers.las",
        "filename": str(dst_las_path)
    }
    if write_options:
        conf.update(write_options)
    return conf


def compose_pipeline(
        process_config: OrderedDict[Callable, Dict[str, Any]]
) -> str:
    """
    Compose a PDAL pipeline from the specified process configuration.
    Args:
        process_config (OrderedDict[Callable, Dict[str, Any]]): Ordered
            dictionary containing the process functions and their arguments.
    Returns:
        str: A json string containing the PDAL processing pipeline.

    """
    return json. dumps(
        obj={
            "pipeline": [
                function(**arg_dict)
                for function, arg_dict in process_config.items()
            ]
        }
    )
