import re
import fiona
import pandas as pd
from osgeo import gdal
from lxml import etree
import geopandas as gpd
from pathlib import Path
from zipfile import ZipFile
from typing import Any, Dict, List, Optional, Tuple, Union


gdal.UseExceptions()

DUMMY_BGT_GML = """
<?xml version="1.0" encoding="UTF-8"?>
<gml:FeatureCollection
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:imgeo-s="http://www.geostandaarden.nl/imgeo/2.1/simple/gml31"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:gml="http://www.opengis.net/gml" xsi:schemaLocation="http://www.geostandaarden.nl/imgeo/2.1/simple/gml31 imgeo-simple-2.1-gml31.xsd http://www.opengis.net/gml http://schemas.opengis.net/gml/3.1.1/base/gml.xsd">
  <gml:featureMember>
    <imgeo-s:{layer_name} gml:id="Dummy">
      <imgeo-s:objectBeginTijd xsi:nil="true"/>
      <imgeo-s:identificatie.namespace xsi:nil="true"/>
      <imgeo-s:identificatie.lokaalID xsi:nil="true"/>
      <imgeo-s:tijdstipRegistratie xsi:nil="true"/>
      <imgeo-s:LV-publicatiedatum xsi:nil="true"/>
      <imgeo-s:bronhouder xsi:nil="true"/>
      <imgeo-s:inOnderzoek xsi:nil="true"/>
      <imgeo-s:relatieveHoogteligging xsi:nil="true"/>
      <imgeo-s:bgt-status xsi:nil="true"/>
      <imgeo-s:bgt-type xsi:nil="true"/>
      <imgeo-s:plus-type xsi:nil="true"/>
      <imgeo-s:geometrie2d>
        <gml:MultiPolygon xmlns:gml="http://www.opengis.net/gml">
          <gml:polygonMember>
            <gml:Polygon>
              <gml:exterior>
                <gml:LinearRing>
                  <gml:posList xsi:nil="true"/>
                </gml:LinearRing>
              </gml:exterior>
            </gml:Polygon>
          </gml:polygonMember>
        </gml:MultiPolygon>
      </imgeo-s:geometrie2d>
    </imgeo-s:{layer_name}>
  </gml:featureMember>
</gml:FeatureCollection>
"""


def multi_replace(text: str, phrase_map: Dict[str, str]):
    for old_phrase, new_phrase in phrase_map.items():
        text = re.sub(
            pattern=rf"\b{old_phrase}\b",
            repl=new_phrase,
            string=text,
            count=0,
            flags=0
        )
    return text


def convert_datetime(
        src_path: Union[str, Path],
        format_map: Dict[str, Dict[str, str]],  # {attr: {"format": "%Y-%m-%d", "tz": "Europe/Amsterdam"}}
        layer: Optional[Union[str, int]] = None,
        dst_path: Optional[Union[str, Path]] = None,  # None => Inplace
        dst_driver: Optional[str] = None,  # "FlatGeobuf"
):
    src_path = Path(src_path).expanduser().absolute()
    if dst_path is None:
        dst_path = src_path
    else:
        dst_path = Path(dst_path).expanduser().absolute()
    gdf = gpd.read_file(filename=src_path, layer=layer, )
    for attr, params in format_map.items():
        fmt = params.pop("format", None)
        gdf[attr] = pd.to_datetime(
            gdf[attr],
            format=fmt,
            errors='coerce'
        )
        if "tz" in params.keys():
            gdf[attr].dt.tz_localize(
                **params
                # tz=params["tz"],
                # ambiguous=params.get("ambiguous", "infer"),
                # nonexistent=params.get("nonexistent", "shift_forward")
            )
    gdf.to_file(filename=dst_path, layer=layer, driver=dst_driver)
    return dst_path


def vector_translate(
        src_path: Union[str, Path],
        dst_path: Union[str, Path],
        src_driver: Optional[Tuple[str]] = None,
        src_crs: Optional[str] = None,
        dst_crs: Optional[str] = None,
        dst_driver: Optional[str] = "GPKG",
        src_options: Optional[Union[Tuple[str], Dict[str, Any]]] = tuple(),
        convert_options: Optional[Dict[str, Any]] = None
):
    src_path = Path(src_path).expanduser().absolute()
    dst_path = Path(dst_path).expanduser().absolute()
    extra_opts = dict()
    if src_driver:
        extra_opts["allowed_drivers"] = src_driver
    if src_options:
        extra_opts["open_options"] = src_options
    with gdal.OpenEx(
        utf8_path=str(src_path),
        nOpenFlags=gdal.OF_VECTOR,
        **extra_opts
    ) as src:
        if src_crs is None:
            srs = src.GetLayer().GetSpatialRef()
            if srs is None:
                src_crs = None
            else:
                src_crs = f"EPSG:{srs.GetAuthorityCode(None)}"
        if dst_crs is None:
            dst_crs = src_crs
        reproject = (src_crs != dst_crs)

    translate_options = {
        "format": dst_driver,
        "srcSRS": src_crs,
        "dstSRS": dst_crs,
        "reproject": reproject,
    }
    if convert_options is not None:
        translate_options.update(convert_options)
    gdal.VectorTranslate(
        destNameOrDestDS=str(dst_path),
        srcDS=str(src_path.expanduser().absolute()),
        options=gdal.VectorTranslateOptions(
            **translate_options
        )
    )
    return dst_path


def bgt_convert(
        bgt_zip: Union[str, Path],
        dst_dir: Optional[Union[str, Path]] = None,
        dst_driver: Optional[str] = "GPKG",
        fix_schema_source: Optional[bool] = True,
        src_crs: Optional[str] = "EPSG:28992",
        dst_crs: Optional[str] = None,
        clip_src: Optional[str] = None,  # WKT string (POLYGON or MULTIPOLYGON)
        clean_up: Optional[bool] = True,
        use_dummy: Optional[bool] = True
) -> List[Path]:
    """
    Convert BGT data to a different format.
    Args:
        bgt_zip: Path to the BGT zip file.
        dst_dir: Path to the directory to save the converted file(s).
        dst_driver: Driver to use for writing the converted format.
        fix_schema_source: Whether to fix the known schema issue in BGT GML files.
        src_crs: Source CRS of the BGT data.
        dst_crs: Destination CRS of the converted data.
        clip_src: Optional WKT string (POLYGON or MULTIPOLYGON) to clip the data.
        clean_up: Whether to clean up the extracted files and delete the BGT zip file.
        use_dummy: Whether to use a dummy GML file in case empty GML file is empty.

    Returns:

    """

    xml_namespaces = {
        "xsi": "http://www.w3.org/2001/XMLSchema-instance"
    }
    schema_urls = {
        "imgeo.xsd": "https://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo.xsd",
        "imgeo-simple-2.1-gml31.xsd": "https://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd"
    }

    # gdal.VectorTranslate
    bgt_zip = Path(bgt_zip).expanduser().absolute()
    with ZipFile(file=bgt_zip, mode="r") as zfp:
        if dst_dir is None:
            dst_dir = bgt_zip.parent
        dst_dir = Path(dst_dir).expanduser().absolute()
        dst_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        dst_map = dict()
        for file_name in zfp.namelist():
            src_path = Path(
                zfp.extract(member=file_name, path=dst_dir)
            ).expanduser().absolute()
            layer_name = src_path.stem.split(
                sep="_",
                maxsplit=1
            )[-1]
            if fix_schema_source:
                tree = etree.parse(source=src_path)
                schema_locations = tree.getroot().xpath(
                    "//*[@xsi:schemaLocation]",
                    namespaces=xml_namespaces
                )
                for element in schema_locations:
                    key = f"{{{xml_namespaces['xsi']}}}schemaLocation"
                    element.set(
                        key,
                        multi_replace(
                            text=element.get(key),
                            phrase_map=schema_urls
                        )
                    )
                tree.write(
                    src_path,
                    pretty_print=False,
                    xml_declaration=True,
                    encoding='utf-8'
                )

            with gdal.OpenEx(
                utf8_path=str(src_path),
                nOpenFlags=gdal.OF_VECTOR,
                allowed_drivers=["GML"],
                open_options=[
                    "WRITE_GFS=NO",
                    "FORCE_SRS_DETECTION=NO",
                    "EMPTY_AS_NULL=YES",
                    "SWAP_COORDINATES=AUTO",
                    "READ_MODE=AUTO",
                    "CONSIDER_EPSG_AS_URN=AUTO",
                    "EXPOSE_FID=AUTO",
                    "DOWNLOAD_SCHEMA=NO"
                ]
            ) as src:
                flag = use_dummy & (src.GetLayerCount() == 0)
            if flag:
                with open(file=src_path, mode="w", encoding="utf-8") as fp:
                    fp.write(
                        DUMMY_BGT_GML.format(
                            layer_name=layer_name.title()
                        ).strip()
                    )

            with gdal.OpenEx(
                utf8_path=str(src_path),
                nOpenFlags=gdal.OF_VECTOR,
                allowed_drivers=["GML"],
                open_options=[
                    "WRITE_GFS=NO",
                    "FORCE_SRS_DETECTION=NO",
                    "EMPTY_AS_NULL=YES",
                    "SWAP_COORDINATES=AUTO",
                    "READ_MODE=AUTO",
                    "CONSIDER_EPSG_AS_URN=AUTO",
                    "EXPOSE_FID=AUTO",
                    "DOWNLOAD_SCHEMA=NO"
                ]
            ) as src:
                if src_crs is None:
                    srs = src.GetLayer().GetSpatialRef()
                    if srs is None:
                        src_crs = None
                    else:
                        src_crs = f"EPSG:{srs.GetAuthorityCode(None)}"
                if dst_crs is None:
                    dst_crs = src_crs
                reproject = (src_crs != dst_crs)
                dst_path = src_path.with_suffix(
                    f".{dst_driver.lower()}"
                ).expanduser().absolute()
            gdal.VectorTranslate(
                destNameOrDestDS=str(dst_path),
                srcDS=str(src_path),
                options=gdal.VectorTranslateOptions(
                    format=dst_driver,
                    accessMode=None,
                    srcSRS=src_crs,
                    dstSRS=dst_crs,
                    reproject=reproject,
                    geometryType=("CONVERT_TO_LINEAR", 'PROMOTE_TO_MULTI'),
                    dim="XY",
                    clipSrc=clip_src,
                    makeValid=True,
                    skipFailures=False,
                    callback=None,
                )
            )
            for layer in fiona.listlayers(src_path):
                convert_datetime(
                    src_path=dst_path,
                    layer=layer,
                    format_map={
                        "objectBeginTijd": {
                            "format": None,
                            "tz": "Europe/Amsterdam",
                            "ambiguous": "infer",
                            "nonexistent": "shift_forward"
                        },
                        "objectEindTijd": {
                            "format": None,
                            "tz": "Europe/Amsterdam",
                            "ambiguous": "infer",
                            "nonexistent": "shift_forward"
                        },
                        "tijdstipRegistratie": {
                            "format": None,
                            "tz": "Europe/Amsterdam",
                            "ambiguous": "infer",
                            "nonexistent": "shift_forward"
                        },
                        "eindRegistratie": {
                            "format": None,
                            "tz": "Europe/Amsterdam",
                            "ambiguous": "infer",
                            "nonexistent": "shift_forward"
                        },
                        "LV-publicatiedatum": {
                            "format": None,
                            "tz": "Europe/Amsterdam",
                            "ambiguous": "infer",
                            "nonexistent": "shift_forward"
                        }
                    },
                    dst_path=None,
                    dst_driver=dst_driver
                )
            dst_map[layer_name] = dst_path
            if clean_up:
                src_path.unlink()
                gfs_path = src_path.with_suffix(".gfs")
                if gfs_path.is_file():
                    gfs_path.unlink()
        return dst_map
