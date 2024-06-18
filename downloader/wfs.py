import fiona
import geopandas as gpd
from pathlib import Path
from pyproj.crs import CRS
from owslib.etree import etree
from xml.etree import ElementTree
from rich.progress import Progress
from owslib.wfs import WebFeatureService
from owslib.namespaces import Namespaces as OWSNamespace
from typing import Optional, Any, Tuple, Dict, Union, Iterable
from owslib.util import log, nspath, openURL, ServiceException


class WFS200(object):

    namespace = OWSNamespace()

    def __init__(
            self, url: str,
            xml: Optional[str] = None,
            version: Optional[str] = "2.0.0",
            parse_remote_metadata: Optional[bool] = False,
            timeout: Optional[int] = 30,
            username: Optional[str] = None,
            password: Optional[str] = None,
            headers: Optional[str] = None,
            auth: Optional[str] = None
    ) -> None:
        """
        Initialize a WFS 2.0.0 object.

        Args:
            url (str): URL of the WFS service.
            xml (Optional[str]): XML string with custom options.
            version (Optional[str]): Version of the WFS service.
            parse_remote_metadata (Optional[bool]): Whether to parse remote
                metadata.
            timeout (Optional[int]): Timeout for the request.
            username (Optional[str]): Username for the request.
            password (Optional[str]): Password for the request.
            headers (Optional[str]): Headers for the request.
            auth (Optional[str]): Authentication for the request.
        """
        if int(version[0]) != 2:
            raise NotImplementedError(
                f"Version: {version} is not implemented!" +
                "Currently supported version: 2.x.x"
            )
        self._wfs_src = WebFeatureService(
            url=url,
            version=version,
            xml=xml,
            timeout=timeout,
            parse_remote_metadata=parse_remote_metadata,
            username=username,
            password=password,
            headers=headers,
            auth=auth
        )
        self._ops = {
            op.name: op
            for op in self._wfs_src.operations
        }
        self._schema_cache = dict()

    def fetch_capabilities(
            self,
            parser: Optional[etree.XMLParser] = None
    ) -> etree.Element:
        """
        Fetch capabilities of the WFS service.
        Args:
            parser (Optional[etree.XMLParser]): XML Parser to use.

        Returns:
            etree.Element: XML Element Tree containing the capabilities of the
                WFS service.
        """
        element = etree.XML(
            text=self._wfs_src.getcapabilities().read(),
            parser=parser
        )
        return element

    def fetch_layers(self) -> Tuple[str]:
        return tuple(self._wfs_src.contents.keys())

    @property
    def output_formats(self) -> Iterable[str]:
        if "GetFeature" in self._ops.keys():
            return tuple(
                self._ops["GetFeature"].parameters["outputFormat"]["values"]
            )
        else:
            raise ValueError("`GetFeature` is not a supported capability!")

    def fetch_schema(self, typename: str) -> Dict[str, Any]:
        if typename not in self._schema_cache.keys():
            self._schema_cache[typename] = self._wfs_src.get_schema(
                typename=typename
            )
        return self._schema_cache[typename]

    def fetch_crs_options(self, type_name: str) -> Iterable[str]:
        return self._wfs_src.contents[type_name].crsOptions

    def get_hits(
            self,
            type_name: Optional[Tuple[str]] = None,
            filter_condition: Optional[str] = None,
            bbox: Optional[Tuple[int]] = None
    ):
        url, data = self._wfs_src.getPOSTGetFeatureRequest(
            typename=type_name,
            filter=filter_condition,
            bbox=bbox,
            featureid=None,
            featureversion=None,
            propertyname="*",
            maxfeatures=None,
            outputFormat=None,
            method='Post',
            startindex=None,
            sortby=None
        )
        root = ElementTree.fromstring(data)
        root.set("resultType", "hits")
        data = ElementTree.tostring(root, encoding='utf-8', method='xml')

        u = openURL(
            url_base=url,
            data=data,
            method="Post",
            timeout=self._wfs_src.timeout,
            headers=self._wfs_src.headers,
            auth=self._wfs_src.auth
        )

        if "Content-Length" in u.info():
            length = int(u.info()["Content-Length"])
            have_read = False
        else:
            data = u.read()
            have_read = True
            length = len(data)

        if length < 32000:
            if not have_read:
                data = u.read()
            try:
                tree = etree.fromstring(data)
            except Exception as exc:
                log.debug(f"Not XML:\n{data}\n\nException:\n{exc}")
            else:
                ogc_namespace = WFS200.namespace.get_namespace(key="ogc")
                if tree.tag == "{%s}ServiceExceptionReport" % ogc_namespace:
                    se = tree.find(
                        nspath("ServiceException", ogc_namespace)
                    )
                    raise ServiceException(str(se.text).strip())
        else:
            data = u.read()
        count = ElementTree.fromstring(data).attrib.get('numberMatched')
        try:
            return int(count)
        except Exception as exc:
            raise ValueError(f"Invaid integer: {count},\nException:\n{exc}")

    def get_feature(
            self,
            type_name: Optional[Tuple[str]] = None,
            filter_condition: Optional[str] = None,
            bbox: Optional[Tuple[int]] = None,
            feature_id: Optional[Tuple[str]] = None,
            feature_version: Optional[str] = None,
            property_name: Optional[Tuple[str]] = None,  # "*" gor method=GET
            max_features: Optional[int] = None,
            stored_query_id: Optional[str] = None,
            stored_query_params: Optional[Dict[str, Any]] = None,
            method: Optional[str] = "POST",
            output_format: Optional[str] = None,
            output_crs: Optional[Union[CRS, str, int]] = None,
            start_index: Optional[int] = None,
            sort_by: Optional[Tuple[str]] = None,
            schema: Optional[Dict[str, Any]] = None,
            src_crs: Optional[Union[CRS, str, int]] = None,
            output_driver: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        primary_key = self.fetch_schema(typename=type_name)["required"]
        response = self._wfs_src.getfeature(
            typename=type_name,
            filter=filter_condition,
            bbox=bbox,
            featureid=feature_id,
            featureversion=feature_version,
            propertyname=property_name,
            maxfeatures=max_features,
            storedQueryID=stored_query_id,
            storedQueryParams=stored_query_params,
            method=method,
            outputFormat=output_format,
            startindex=start_index,
            sortby=sort_by
        )
        gdf = gpd.read_file(
            filename=response,
            driver=output_driver,
            schema=schema
        )
        gdf = gdf.set_index(primary_key)
        if isinstance(src_crs, (str, int)):
            src_crs = CRS.from_string(str(src_crs))
        gdf.crs = src_crs
        if isinstance(output_crs, (str, int)):
            output_crs = CRS.from_string(str(output_crs))
        if output_crs is not None:
            gdf.to_crs(crs=output_crs, inplace=True)
        return gdf

    def stream_feature(
            self,
            dst_file: Union[str, Path],
            data_crs: Optional[Union[CRS, str, int]] = None,
            data_driver: Optional[str] = None,
            dst_driver: Optional[str] = "GPKG",
            dst_layer: Optional[str] = "Layer",
            dst_mode: Optional[str] = 'w',
            type_name: Optional[Tuple[str]] = None,
            filter_condition: Optional[str] = None,
            bbox: Optional[Tuple[int]] = None,
            feature_id: Optional[Tuple[str]] = None,
            feature_version: Optional[str] = None,
            property_name: Optional[Tuple[str]] = None,  # "*" if method=GET
            max_features: Optional[int] = None,
            stored_query_id: Optional[str] = None,
            stored_query_params: Optional[Dict[str, Any]] = None,
            method: Optional[str] = "POST",  # "Get"
            output_format: Optional[str] = None,
            output_crs: Optional[Union[CRS, str, int]] = None,
            start_index: Optional[int] = 0,
            sort_by: Optional[Tuple[str]] = None,
            fid_suffix: Optional[str] = f"_original",
            progress_handle: Optional[Progress] = None,
            clear_progressbar: Optional[bool] = True
    ) -> Path:
        total_hits = self.get_hits(
            type_name=type_name,
            filter_condition=filter_condition,
            bbox=bbox
        )
        markers = tuple(range(start_index, total_hits, max_features))

        task = None
        if progress_handle:
            task = progress_handle.add_task(
                description="[orange]Current Feature:",
                total=len(markers)
            )
        schema = None
        for marker in markers:
            gdf = self.get_feature(
                type_name=type_name,
                filter_condition=filter_condition,
                bbox=bbox,
                feature_id=feature_id,
                feature_version=feature_version,
                property_name=property_name,
                max_features=max_features,
                stored_query_id=stored_query_id,
                stored_query_params=stored_query_params,
                output_format=output_format,
                output_crs=output_crs,
                method=method,
                start_index=marker,
                sort_by=sort_by,
                schema=schema,
                output_driver=data_driver,
                src_crs=data_crs
            )
            gdf.reset_index(inplace=True)
            gdf.index = range(marker, (marker + gdf.shape[0]))
            column_mappings = {
                "fid": f"fid{fid_suffix}",
                "FID": f"FID{fid_suffix.upper()}"
            }
            for attr_name in column_mappings.values():
                if attr_name in gdf.columns:
                    raise ValueError(
                        f"Attribute `{attr_name}` already exists!\n" +
                        "Expected a collison free attribute suffix"
                    )
            gdf.rename(
                columns=column_mappings,
                inplace=True
            )
            gdf.to_file(
                filename=dst_file,
                driver=dst_driver,
                layer=dst_layer,
                mode=dst_mode
            )
            dst_mode = 'a'
            if schema is None:
                with fiona.open(dst_file, mode='r') as dst:
                    schema = dst.schema
            if task:
                progress_handle.update(task_id=task, advance=1)
        if task and clear_progressbar:
            progress_handle.remove_task(task)
        return dst_file
