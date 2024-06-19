import fiona
from uuid import uuid4
from pyproj import CRS
from pathlib import Path
from downloader.wfs import WFS200
from downloader.bgt import BGTDownloader
from fiona.transform import transform_geom
from shapely.geometry import shape, Polygon
from utils.filter import alter_data, prepare_data
from utils.converter import vector_translate, bgt_convert
from typing import Any, Dict, Iterable, Optional, Sequence, Set, Union
from rich.progress import Progress  # BarColumn, TextColumn, SpinnerColumn


class MapProcessor(object):
    crs = CRS.from_epsg(code=28992)
    wfs_src = WFS200(
        url="https://data.3dbag.nl/api/BAG3D/wfs",
        version="2.0.0"
    )
    # TODO: Add progressbars

    def __init__(
            self,
            roi_src: Union[str, Path],
            dst_dir: Union[str, Path],
            roi_attr: Optional[str] = "City",
            roi_layer: Optional[Union[int, str]] = None,
            download_format: Optional[str] = "gmllight",
            convert_driver: Optional[str] = "GPKG",
            convert_cleanup: Optional[bool] = True,
            use_dummy: Optional[bool] = True,
            attr_name: Optional[str] = "DN",
            allowed_geometries: Optional[
                Union[Set[str], Sequence[str]]
            ] = tuple(["MultiPolygon"]),
            nodata_value: Optional[Any] = 0,
            sort_by: Optional[
                Union[str, Sequence[str]]
            ] = "relatieveHoogteligging",
            sort_ascending: Optional[bool] = True,
            pand2bag: Optional[bool] = True,
            bag_chunk: Optional[int] = 1000,
            bag_filter: Optional[str] = """DELETE FROM {layer} WHERE "oorspronkelijkbouwjaar" > 2022;"""
    ):
        self.roi_src = Path(roi_src).expanduser().absolute()
        self.roi_attr = roi_attr
        self.dst_dir = Path(dst_dir).expanduser().absolute()
        self.roi_layer = roi_layer
        self.download_format = download_format
        self.convert_driver = convert_driver
        self.convert_cleanup = convert_cleanup
        self.use_dummy = use_dummy
        self.attr_name = attr_name
        self.allowed_geometries = allowed_geometries
        self.nodata_value = nodata_value
        self.sort_by = sort_by
        self.sort_ascending = sort_ascending
        self.layer_map = {
            'waterdeel': {"infer": None, "value": 11},  # 1
            'begroeidterreindeel': {"infer": None, "value": 21},  # 2
            'onbegroeidterreindeel': {"infer": None, "value": 31},  # 3
            'pand': {"infer": None, "value": 81},  # 4
            'gebouwinstallatie': {"infer": None, "value": 82},  # 5
            'wegdeel': {
                "infer": "bgt-functie",
                "value": {
                    "OV-baan": 51,
                    "overweg": 52,
                    "rijbaan autoweg": 56,
                    "rijbaan autosnelweg": 56,
                    "rijbaan regionale weg": 57,
                    "rijbaan lokale weg": 58,
                    "woonerf": 58,
                    "fietspad": 56,
                    "voetpad": 60,
                    "voetpad op trap": 61,
                    "parkeervlak": 63,
                    "voetgangersgebied": 64,
                    "inrit": 65,
                    "spoorbaan": 69,
                    "transitie": 82
                },
                "alter": {
                    "layer": 0,
                    "statement": """
                    UPDATE {layer}
                    SET relatieveHoogteligging = 
                        CASE
                            WHEN "bgt-functie" = 'transitie' THEN -1
                            ELSE "bgt-functie"
                        END
                    ;
                """
                }
            },  # 6
            'ondersteunendwegdeel': {"infer": None, "value": 67},  # 7
            'kunstwerkdeel': {"infer": None, "value": 83},  # 8
            'ondersteunendwaterdeel': {"infer": None, "value": 12},  # 9
            'overbruggingsdeel': {"infer": None, "value": 84}  # 10
        }
        self.pand2bag = pand2bag
        self.bag_chunk = bag_chunk
        # self.filter_dates = filter_dates
        self.bag_filter = bag_filter

    def download(
            self,
            path_prefix: Optional[str] = "bgt",
            chunk_size: Optional[int] = 1024,
            checking_interval: Optional[int] = 2,
            progress_host: Optional[Progress] = None
    ) -> Path:
        path_prefix = f"{path_prefix}_" if path_prefix else str()
        with fiona.open(fp=self.roi_src, layer=self.roi_layer) as src:
            src_crs = src.crs
            for feature in src:
                if feature["geometry"].type in {"Polygon"}:
                    if src_crs != self.crs:
                        feature["geometry"] = transform_geom(
                            src_crs=src_crs,
                            dst_crs=self.crs,
                            geom=feature["geometry"]
                        )
                    polygon = shape(feature["geometry"])
                    bgt = BGTDownloader()
                    attr_table = dict(feature["properties"])
                    feature_name = attr_table.get(self.roi_attr, feature['id'])
                    bgt_zip = self.dst_dir / f"{path_prefix}{self.roi_src.stem}_{feature_name}.zip"
                    if self.pand2bag:
                        layer_list = tuple(
                            set(self.layer_map.keys()) - {"pand"}
                        )
                    else:
                        layer_list = tuple(self.layer_map.keys())
                    bgt.download(
                        geo_filter=polygon,
                        dst_filepath=bgt_zip,
                        feature_types=layer_list,
                        progress_host=progress_host,
                        chunk_size=chunk_size,
                        format_type=self.download_format,
                        checking_interval=checking_interval
                    )
                    yield {
                        "feature_id": feature['id'],
                        "bgt_zip": bgt_zip,
                        "boundary": polygon
                    }

    def convert(
            self,
            kwargs_list: Iterable[Dict[str, Any]]
    ):
        for kwargs in kwargs_list:
            bgt_zip = kwargs["bgt_zip"]
            dst_dir = kwargs.get("dst_dir", bgt_zip.with_suffix(suffix=str()))
            dst_driver = kwargs.get("dst_driver", self.convert_driver)
            boundary = kwargs.get("boundary", None)
            clean_up = kwargs.get("clean_up", self.convert_cleanup)
            use_dummy = kwargs.get("use_dummy", self.use_dummy)
            dst_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
            converted = bgt_convert(
                bgt_zip=bgt_zip,
                dst_dir=dst_dir,
                dst_driver=dst_driver,
                fix_schema_source=False,
                src_crs=self.crs.to_string(),
                clip_src=boundary.wkt if isinstance(
                    boundary, Polygon
                ) else None,
                clean_up=clean_up,
                use_dummy=use_dummy
            )
            yield {
                "feature_id": kwargs["feature_id"],
                "boundary": boundary,
                "parent_dir": dst_dir,
                "layer_map": converted
            }

    def prepare(self, kwargs_list: Iterable[Dict[str, Any]]):
        for kwargs in kwargs_list:
            src_dict = kwargs["layer_map"]
            geom_types = kwargs.get("geom_types", self.allowed_geometries)
            infer_datetime = kwargs.get("infer_datetime", None)
            attr_name = kwargs.get("new_attr", self.attr_name)
            sort_by = kwargs.get("sort_by", self.sort_by)
            sort_asc = kwargs.get("sort_asc", self.sort_ascending)
            dst_path = kwargs.get("dst_path", None)
            dst_driver = kwargs.get("dst_driver", None)
            dst_map = dict()
            for layer_name, layer_path in src_dict.items():
                filler = self.layer_map.get(
                    layer_name,
                    {"infer": None, "value": self.nodata_value}
                )
                if self.layer_map[layer_name].get("alter", None):
                    layer_path = alter_data(
                        src_path=layer_path,
                        layer=self.layer_map[layer_name]["alter"]["layer"],
                        update_statement=self.layer_map[layer_name]["alter"]["statement"]
                    )
                prepared = prepare_data(
                    src_path=layer_path,
                    geom_types=geom_types,
                    infer_datetime=infer_datetime,
                    new_attr=attr_name,
                    infer_attr=filler["infer"],
                    value_map=filler["value"],
                    default_fill=self.nodata_value,
                    sort_by=sort_by,
                    sort_asc=sort_asc,
                    dst_path=dst_path,
                    dst_driver=dst_driver
                )
                dst_map[layer_name] = prepared
            yield {
                "feature_id": kwargs["feature_id"],
                "boundary": kwargs["boundary"],
                "parent_dir": kwargs["parent_dir"],
                "layer_map": dst_map
            }

    def download_bag(
            self,
            kwargs_list: Iterable[Dict[str, Any]]
    ):
        for kwargs in kwargs_list:
            pand_path = kwargs["parent_dir"] / f"bag_pand.{self.convert_driver.lower()}"
            boundary = kwargs["boundary"]
            bag_path = self.wfs_src.stream_feature(
                dst_file=pand_path,
                bbox=boundary.bounds,
                type_name="BAG3D:lod12",
                dst_layer="Pand",
                data_crs=self.crs,
                data_driver="GML",
                dst_driver=self.convert_driver,
                property_name=None,
                method="POST",
                output_format="GML2",
                output_crs=None,
                start_index=0,
                max_features=self.bag_chunk,
                progress_handle=None,
                clear_progressbar=True
            )
            convert_path = vector_translate(
                src_path=bag_path,
                dst_path=bag_path.parent / f"{uuid4().hex}.{self.convert_driver.lower()}",
                src_driver=self.convert_driver,
                dst_driver=self.convert_driver,
                src_crs=None,
                dst_crs=None,
                src_options=None,
                convert_options={
                    "clipSrc": boundary.wkt,
                    "geometryType": ("CONVERT_TO_LINEAR", "PROMOTE_TO_MULTI"),
                    "dim": "XY",
                    "makeValid": True,
                    "skipFailures": False
                }
            )

            if self.bag_filter:
                bag_path = alter_data(
                    src_path=bag_path,
                    layer=0,
                    update_statement=self.bag_filter
                )

            bag_path = convert_path.rename(target=bag_path)
            bag_path = prepare_data(
                src_path=bag_path,
                geom_types={"MultiPolygon"},
                infer_datetime={
                    "oorspronkelijkbouwjaar": "%Y",
                },
                new_attr=self.attr_name,
                infer_attr=None,
                value_map=self.layer_map["pand"]["value"],
                default_fill=self.nodata_value,
                sort_by=None,
                sort_asc=True,
                dst_path=None,
                dst_driver=None
            )
            kwargs["layer_map"]["pand"] = bag_path
            yield kwargs

    def process(self):
        if self.pand2bag:
            return list(
                self.download_bag(self.prepare(self.convert(self.download())))
            )
        else:
            return list(self.prepare(self.convert(self.download())))
