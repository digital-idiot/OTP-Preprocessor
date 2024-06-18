import fiona
from osgeo import gdal
from lxml import etree
from pathlib import Path
from warnings import warn
from zipfile import ZipFile
from osgeo.gdal import ogr as ogr
from typing import Optional, Union
from fiona.errors import UnsupportedGeometryTypeError


gdal.UseExceptions()


# def convert_bgt(
#         bgt_zip: Union[str, Path],
#         dst_file: Optional[Union[str, Path]] = None,
#         marker: Optional[str] = "marker",
#         driver: Optional[str] = "GPKG"
# ) -> None:
#     """
#     Convert a BGT ZIP file to a GeoPackage file.
#
#     Args:
#         bgt_zip (Union[str, Path]): Path to the BGT ZIP file.
#         dst_file (Optional[Union[str, Path]]): Path to the destination file.
#         marker (Optional[str]): Attribute to use for identifying the layer.
#         driver (Optional[str]): Driver to use for writing the destination file.
#              Note: The driver must support append mode.
#     Returns:
#         None
#     """
#     bgt_zip = Path(bgt_zip)
#     with ZipFile(file=bgt_zip, mode="r") as zfp:
#         if dst_file is None:
#             dst_file = bgt_zip.with_suffix(f".{driver.lower()}")
#         # mode: str = 'w'
#         # start_index: int = 1
#         layers = dict()
#         for file_name in zfp.namelist():
#             try:
#                 with zfp.open(file_name) as fp:
#                     # gdf = gpd.read_file(filename=fp, srsDimension=2)
#                     with fiona.open(fp, mode='r') as src:
#                         schema = src.schema.copy()
#                         for feature in src:
#                             geom_type = feature["geometry"].type
#                             if geom_type not in layers:
#                                 schema["geometry"] = geom_type
#                                 layers[geom_type] = fiona.open(
#                                     fp=dst_file,
#                                     mode='w',
#                                     driver=driver,
#                                     schema=schema,
#                                     crs=src.crs,
#                                     layer=geom_type
#                                 )
#                             layers[geom_type].write(feature)
#                             #     mode = 'w'
#                             # else:
#                             #     mode = 'a'
#                             # with fiona.open(
#                             #     fp=dst_file,
#                             #     mode=mode,
#                             #     driver=driver,
#                             #     schema=schema,
#                             #     crs=src.crs,
#                             #     layer=geom_type
#                             # ) as dst:
#                             #     dst.write(feature)
#
#             except Exception as exc:
#                 warn(f"{file_name} => {exc}")
#
#             #     gdf = None
#             # if gdf is not None:
#             #     feature_count = len(gdf)
#             #     try:
#             #         gdf.index = gpd.pd.RangeIndex(
#             #             start=start_index,
#             #             stop=feature_count+1,
#             #             step=1
#             #         )
#             #         gdf[marker] = str(Path(file_name).stem)
#             #         gdf.to_file(filename=dst_file, driver=driver, mode=mode)
#             #         start_index += feature_count
#             #         mode = 'a'
#             #     except Exception as exc:
#             #         print(feature_count)
#             #         # print(gdf)
#             #         raise exc
#
#
# def convert_old(
#         bgt_zip: Union[str, Path],
#         dst_file: Optional[Union[str, Path]] = None,
#         driver: Optional[str] = "GPKG"
# ) -> None:
#     """
#     Convert a BGT ZIP file to a GeoPackage file.
#
#     Args:
#         bgt_zip (Union[str, Path]): Path to the BGT ZIP file.
#         dst_file (Optional[Union[str, Path]]): Path to the destination file.
#         driver (Optional[str]): Driver to use for writing the destination file.
#              Note: The driver must support append mode.
#     Returns:
#         None
#     """
#     bgt_zip = Path(bgt_zip)
#     with ZipFile(file=bgt_zip, mode="r") as zfp:
#         if dst_file is None:
#             dst_file = bgt_zip.with_suffix(f".{driver.lower()}")
#         mode: str = 'w'
#         for file_name in zfp.namelist():
#             try:
#                 with zfp.open(file_name) as fp:
#                     layers = fiona.listlayers(fp)
#                     fp.seek(0)
#                     if len(layers) > 0:
#                         for layer in layers:
#                             with fiona.open(
#                                 fp,
#                                 mode='r',
#                                 driver="GMLAS",
#                                 layer=layer,
#                                 GML_SKIP_CORRUPTED_FEATURES=True
#                             ) as src:
#                                 if len(src) > 0:
#                                     # try:
#                                     #     schema = src.schema.copy()
#                                     # except Exception as exc:
#                                     #     warn(f"{exc}")
#                                     schema = src.schema.copy()
#                                     with fiona.open(
#                                             dst_file,
#                                             mode=mode,
#                                             driver=driver,
#                                             schema=schema,
#                                             crs=src.crs,
#                                             layer=Path(file_name).stem
#                                     ) as dst:
#                                         for feature in src:
#                                             dst.write(feature)
#                                 else:
#                                     warn(
#                                         f"{file_name}::{layer} is Empty. Skipping..."
#                                     )
#                                     # try:
#                                     #     if src.schema:
#                                     #         with fiona.open(
#                                     #                 dst_file,
#                                     #                 mode=mode,
#                                     #                 driver=driver,
#                                     #                 schema=schema,
#                                     #                 crs=src.crs,
#                                     #                 layer=Path(file_name).stem
#                                     #         ) as dst:
#                                     #             for feature in src:
#                                     #                 dst.write(feature)
#                                     # except UnsupportedGeometryTypeError as geo_err:
#                                     #     warn(f"{file_name}\n{schema}\n{geo_err}")
#                     else:
#                         warn(f"{file_name} is Empty. Skipping...")
#
#                 mode = 'a'
#             # except ValueError as val_err:
#             #     warn(f"{file_name} => {val_err}")
#             except UnsupportedGeometryTypeError as geo_err:
#                 warn(f"Invalid Geometry: {file_name}::{layer}\n geo_err")
#             except Exception as exc:
#                 warn(f"{file_name} => {exc}")


def convert(
        bgt_zip: Union[str, Path],
        dst_dir: Optional[Union[str, Path]] = None,
        fix_schema_source: Optional[bool] = False,
) -> None:
    driver = "GPKG"
    # attr_map = {
    #     "_ogr_fields_metadata": "fields_metadata",
    #     "_ogr_layers_metadata": "layers_metadata",
    #     "_ogr_layer_relationships": "layer_relationships",
    #     "_ogr_other_metadata": "other_metadata"
    # }
    xml_namespaces = {
        "xsi": "http://www.w3.org/2001/XMLSchema-instance"
    }
    schema_urls = {
        "imgeo.xsd": "https://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo.xsd",
        "imgeo-simple-2.1-gml31.xsd": "https://register.geostandaarden.nl/gmlapplicatieschema/imgeo/2.1.1/imgeo-simple.xsd"
    }
    # gdal.VectorTranslate
    bgt_zip = Path(bgt_zip)
    with ZipFile(file=bgt_zip, mode="r") as zfp:
        if dst_dir is None:
            dst_dir = bgt_zip.parent
        dst_dir = Path(dst_dir)
        dst_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        for file_name in zfp.namelist():
            src_path = Path(zfp.extract(member=file_name, path=dst_dir))
            if fix_schema_source:
                tree = etree.parse(source=src_path)
                schema_locations = tree.getroot().xpath(
                    _path="//@xsi:schemaLocation",
                    namespaces=xml_namespaces
                )
                uri = str(element)
                for element in schema_locations:
                    element.attrib[
                        f"{{{xml_namespaces['xsi']}}}schemaLocation"
                    ] = schema_urls.get(uri, uri)
                tree.write(
                    file=src_path,
                    xml_declaration=True,
                    pretty_print=False,
                    encoding="utf-8"
                )

            with gdal.OpenEx(
                utf8_path=f"GMLAS:{src_path.absolute()}",
                nOpenFlags=gdal.OF_VECTOR,
                # allowed_drivers=["GMLAS"],
                open_options=[
                    "VALIDATE=YES",
                    "REMOVE_UNUSED_LAYERS=YES"
                    "FAIL_IF_VALIDATION_ERROR=YES",
                    "REMOVE_UNUSED_FIELDS=YES",
                    "HANDLE_MULTIPLE_IMPORTS=YES",
                    "SCHEMA_FULL_CHECKING=YES",
                    "EXPOSE_METADATA_LAYERS=YES"
                ]
            ) as src:
                dst_path = src_path.with_suffix(f".{driver.lower()}")
                gdal.VectorTranslate(
                    destNameOrDestDS=str(dst_path.absolute()),
                    srcDS=src,
                    options=gdal.VectorTranslateOptions(
                        format="",
                        srcSRS="EPSG:28992",
                        dstSRS="EPSG:4326",
                        reproject=True,
                        geometryType="CONVERT_TO_LINEAR",
                        dim="XY",
                        clipSrc=None,
                        clipDst=None,
                    )
                    # format='GeoJSON',
                    # dstSRS='EPSG:4326'  # Optional: Reproject to WGS84
                )
                # with gdal.GetDriverByName(driver).Create(
                #     utf8_path=dst_path,
                #     xsize=0,
                #     ysize=0,
                #     bands=0,
                #     eType=gdal.GDT_Unknown
                # ) as dst:
                #     for i in range(src.GetLayerCount()):
                #         src_layer = src.GetLayerByIndex(i)
                #         layer_name = attr_map.get(
                #             src_layer.GetName(),
                #             src_layer.GetName()
                #         )
                #         if src_layer.GetGeomType() == ogr.wkbNone:
                #             # Non-spatial layer
                #             dst_layer = dst.CreateLayer(
                #                 name=layer_name,
                #                 geom_type=ogr.wkbNone
                #             )
                #         else:
                #             # Spatial layer
                #             dst_layer = dst.CreateLayer(
                #                 name=layer_name,
                #                 srs=src_layer.GetSpatialRef(),
                #                 geom_type=src_layer.GetGeomType(),
                #                 options=['SPATIAL_INDEX=YES']
                #             )
                #         if dst_layer is None:
                #             warn(
                #                 "In the destination dataset failed to " +
                #                 f"create layer: {layer_name}"
                #             )
                #         else:
                #             dst_layer.StartTransaction()
                #             for feature in src_layer:
                #                 fid = feature.GetFID()
                #                 status = dst_layer.CreateFeature(feature)
                #                 if status != 0:
                #                     warn(
                #                         f"Failed to create feature fid:{fid} " +
                #                         f"layer: {layer_name}"
                #                     )
                #             dst_layer.CommitTransaction()
                #     dst.ExecuteSQL("VACUUM")
            src_path.unlink()
    bgt_zip.unlink()
