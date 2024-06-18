import json
import time
import requests
from pyproj import CRS
from pathlib import Path
from warnings import warn
from json import JSONDecodeError
from rich.progress import Progress
from shapely import Polygon, to_wkt
from typing import Dict, Literal, Optional, Tuple, Union


class BGTDownloader:
    """
    Class to download BGT data from PDOK API.
    """
    __crs = CRS.from_epsg(28992)
    __base_url = "https://api.pdok.nl"

    def __init__(self) -> "BGTDownloader":
        """
        Initialize BGTDownloader class.
        """
        self.__api_url = f"{self.__base_url}/lv/bgt/download/v1_0/full/custom"
        self.__headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    @property
    def api_base_url(self) -> str:
        """
        Get base URL of PDOK API.

        Returns:
            str: Base URL of PDOK API.
        """
        return self.__base_url

    @property
    def crs(self) -> CRS:
        """
        Get CRS of BGT data.

        Returns:
            CRS: CRS of BGT data.
        """
        return self.__crs

    def fetch(
        self,
        geo_filter: Polygon,
        feature_types: Optional[
            Tuple[
                Literal[
                    "bak",
                    "begroeidterreindeel",
                    "bord",
                    "buurt",
                    "functioneelgebied",
                    "gebouwinstallatie",
                    "installatie",
                    "kast",
                    "kunstwerkdeel",
                    "mast",
                    "onbegroeidterreindeel",
                    "ondersteunendwaterdeel",
                    "ondersteunendwegdeel",
                    "ongeclassificeerdobject",
                    "openbareruimte",
                    "openbareruimtelabel",
                    "overbruggingsdeel",
                    "overigbouwwerk",
                    "overigescheiding",
                    "paal",
                    "pand",
                    "plaatsbepalingspunt",
                    "put",
                    "scheiding",
                    "sensor",
                    "spoor",
                    "stadsdeel",
                    "straatmeubilair",
                    "tunneldeel",
                    "vegetatieobject",
                    "waterdeel",
                    "waterinrichtingselement",
                    "waterschap",
                    "wegdeel",
                    "weginrichtingselement",
                    "wijk"
                ]
            ]
        ] = (
            "bak",
            "begroeidterreindeel",
            "bord",
            "buurt",
            "functioneelgebied",
            "gebouwinstallatie",
            "installatie",
            "kast",
            "kunstwerkdeel",
            "mast",
            "onbegroeidterreindeel",
            "ondersteunendwaterdeel",
            "ondersteunendwegdeel",
            "ongeclassificeerdobject",
            "openbareruimte",
            "openbareruimtelabel",
            "overbruggingsdeel",
            "overigbouwwerk",
            "overigescheiding",
            "paal",
            "pand",
            "plaatsbepalingspunt",
            "put",
            "scheiding",
            "sensor",
            "spoor",
            "stadsdeel",
            "straatmeubilair",
            "tunneldeel",
            "vegetatieobject",
            "waterdeel",
            "waterinrichtingselement",
            "waterschap",
            "wegdeel",
            "weginrichtingselement",
            "wijk"
        ),
        format_type: Optional[
            Literal[
                "citygml",
                "gmllight",
                "stufgeo"
            ]
        ] = "citygml"
    ) -> Dict[str, str]:
        """
        Probe BGT data from PDOK API.

        Args:
            geo_filter (Polygon): Polygon to filter the data.
            feature_types (Tuple[Literal]): Feature types to download.
            format_type (Literal): Format of the data.

        Returns:
            Dict[str, str]: Metadata with ID of the data to be downloaded.
        """
        payload = json.dumps(
            obj={
                "featuretypes": feature_types,
                "format": format_type,
                "geofilter": to_wkt(geo_filter)
            },
            indent=None,
            separators=(",", ":")
        )

        with requests.post(
            self.__api_url, data=payload, headers=self.__headers
        ) as response:
            if response.status_code == 202:
                try:
                    response_dict = response.json()
                except JSONDecodeError:
                    raise ValueError("Response is not JSON")
                return {
                    "downloadRequestId": response_dict["downloadRequestId"],
                    "downloadEndPoint": (
                        f"{self.api_base_url}" +
                        f"{response_dict['_links']['status']['href']}"
                    )
                }
            else:
                response.raise_for_status()

    def download(
        self,
        geo_filter: Polygon,
        dst_filepath: Union[str, Path],
        feature_types: Optional[
            Tuple[
                Literal[
                    "bak",
                    "begroeidterreindeel",
                    "bord",
                    "buurt",
                    "functioneelgebied",
                    "gebouwinstallatie",
                    "installatie",
                    "kast",
                    "kunstwerkdeel",
                    "mast",
                    "onbegroeidterreindeel",
                    "ondersteunendwaterdeel",
                    "ondersteunendwegdeel",
                    "ongeclassificeerdobject",
                    "openbareruimte",
                    "openbareruimtelabel",
                    "overbruggingsdeel",
                    "overigbouwwerk",
                    "overigescheiding",
                    "paal",
                    "pand",
                    "plaatsbepalingspunt",
                    "put",
                    "scheiding",
                    "sensor",
                    "spoor",
                    "stadsdeel",
                    "straatmeubilair",
                    "tunneldeel",
                    "vegetatieobject",
                    "waterdeel",
                    "waterinrichtingselement",
                    "waterschap",
                    "wegdeel",
                    "weginrichtingselement",
                    "wijk"
                ]
            ]
        ] = (
            "bak",  # "bin"
            "begroeidterreindeel",  # "vegetated area part"
            "bord",  # "plate"
            "buurt",  # "neighbourhood"
            "functioneelgebied",  # "functional area"
            "gebouwinstallatie",  # "building installation"
            "installatie",  # "installation"
            "kast",  # "closet"
            "kunstwerkdeel",  # "artwork part"
            "mast",  # "mast"
            "onbegroeidterreindeel",  # "bare area part"
            "ondersteunendwaterdeel",  # "supporting water part"
            "ondersteunendwegdeel",  # "supporting road section"
            "ongeclassificeerdobject",  # "unclassifiedobject"
            "openbareruimte",  # "public space"
            "openbareruimtelabel",  # "public space label"
            "overbruggingsdeel",  # "bridging part"
            "overigbouwwerk",  # "other construction work"
            "overigescheiding",  # "other separation"
            "paal",  # "pole"
            "pand",  # "pledge"
            "plaatsbepalingspunt",  # "locating point"
            "put",  # "well"
            "scheiding",  # "parting"
            "sensor",  # "sensor"
            "spoor",  # "track"
            "stadsdeel",  # "borough"
            "straatmeubilair",  # "street furniture"
            "tunneldeel",  # "tunnel part"
            "vegetatieobject",  # "vegetation object"
            "waterdeel",  # "water part"
            "waterinrichtingselement",  # "water design element"
            "waterschap",  # "water Authority"
            "wegdeel",  # "way part"
            "weginrichtingselement",  # "road design element"
            "wijk",  # "neighbourhood"
        ),
        format_type: Optional[
            Literal[
                "citygml",
                "gmllight",
                "stufgeo"
            ]
        ] = "citygml",
        progress_host: Optional[Progress] = None,
        checking_interval: Optional[float] = 0.2,
        chunk_size: Optional[int] = 1024
    ) -> None:
        """
        Download BGT data from PDOK API.

        Args:
            geo_filter (Polygon): Polygon to filter the data.
            dst_filepath (Union[str, Path]): Destination dst path.
            feature_types (Tuple[Literal]): Feature types to download.
            format_type (Literal): Format of the data to be fetched.
            progress_host (Optional[Progress]): Object to hook progress bars.
                Example: Progress(
                    SpinnerColumn(spinner_name="Earth", style="earth"),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
                )
            checking_interval (Optional[float]): Interval to check the data
                preparation progress at the host.
            chunk_size (Optional[int]): Size of the download chunks.
        Returns:
            None: None
        """
        meta = self.fetch(
            geo_filter=geo_filter,
            feature_types=feature_types,
            format_type=format_type
        )
        response = requests.get(
            meta["downloadEndPoint"],
            headers=self.__headers
        )
        if progress_host:
            preparartion_task = progress_host.add_task(
                description="Preparing Data:",
                total=100,
                completed=0
            )
        else:
            preparartion_task = None
        while response.status_code == 200:
            if preparartion_task:
                progress_host.update(
                    task_id=preparartion_task,
                    completed=float(response.json()["progress"])
                )
            time.sleep(checking_interval)
            response = requests.get(
                meta["downloadEndPoint"],
                headers=self.__headers
            )
        if response.status_code == 201:
            response_dict = response.json()
            if preparartion_task is not None:
                progress_host.update(
                    task_id=preparartion_task,
                    completed=float(response.json()["progress"])
                )
                if progress_host.tasks[preparartion_task].finished:
                    progress_host.update(
                        task_id=preparartion_task,
                        visible=False
                    )
            download_url = (
                f"{self.api_base_url}" +
                f"{response_dict['_links']['download']['href']}"
            )
            with requests.get(download_url, stream=True) as download_response:
                download_response.raise_for_status()
                with open(dst_filepath, "wb") as dst:
                    total_length = response.headers.get('content-length')
                    try:
                        total_length = int(total_length)
                    except TypeError:
                        warn(f"Unable to infer Content-Length: {total_length}")
                        total_length = None
                    if progress_host:
                        download_task = progress_host.add_task(
                            description="Downloading Data:",
                            total=total_length,
                            completed=0
                        )
                    else:
                        download_task = None
                    for chunk in download_response.iter_content(
                        chunk_size=chunk_size
                    ):
                        dst.write(chunk)
                        dst.flush()
                        if download_task is not None:
                            progress_host.update(
                                task_id=download_task,
                                advance=len(chunk)
                            )
                if download_task is not None:
                    progress_host.update(
                        task_id=download_task,
                        description=(
                            "[green]:white_check_mark:[/green] " +
                            progress_host.tasks[download_task].description
                        )
                    )
        else:
            response.raise_for_status()
