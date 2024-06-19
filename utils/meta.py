import pandas as pd
from enum import Enum
from osgeo import gdal, gdalconst
from typing import Iterable, Literal, Optional, Sequence, Tuple, Union


class RATConst(Enum):
    Integer = gdalconst.GFT_Integer
    Real = gdalconst.GFT_Real
    String = gdalconst.GFT_String
    Alpha = gdalconst.GFU_Alpha
    Blue = gdalconst.GFU_Blue
    Generic = gdalconst.GFU_Generic
    GreenMin = gdalconst.GFU_GreenMin
    Min = gdalconst.GFU_Min
    PixelCount = gdalconst.GFU_PixelCount
    RedMin = gdalconst.GFU_RedMin
    AlphaMax = gdalconst.GFU_AlphaMax
    BlueMax = gdalconst.GFU_BlueMax
    Green = gdalconst.GFU_Green
    Max = gdalconst.GFU_Max
    MinMax = gdalconst.GFU_MinMax
    Red = gdalconst.GFU_Red
    AlphaMin = gdalconst.GFU_AlphaMin
    BlueMin = gdalconst.GFU_BlueMin
    GreenMax = gdalconst.GFU_GreenMax
    MaxCount = gdalconst.GFU_MaxCount
    Name = gdalconst.GFU_Name
    RedMax = gdalconst.GFU_RedMax
    THEMATIC = gdalconst.GRTT_THEMATIC
    ATHEMATIC = gdalconst.GRTT_ATHEMATIC


RATConstAlias = {
    name: member.value for name, member in RATConst.__members__.items()
}


class RAT(object):
    dtype_mapping = {
        RATConst.Integer: int,
        RATConst.Real: float,
        RATConst.String: str,
        0: int,
        1: float,
        2: str

    }

    def __init__(
            self,
            schema: Iterable[
                Tuple[
                    str,
                    Literal[
                        RATConst.Integer,
                        RATConst.Real,
                        RATConst.String
                    ],
                    Literal[
                        RATConst.Alpha,
                        RATConst.Blue,
                        RATConst.Generic,
                        RATConst.GreenMin,
                        RATConst.Min,
                        RATConst.PixelCount,
                        RATConst.RedMin,
                        RATConst.AlphaMax,
                        RATConst.BlueMax,
                        RATConst.Green,
                        RATConst.Max,
                        RATConst.MinMax,
                        RATConst.Red,
                        RATConst.AlphaMin,
                        RATConst.BlueMin,
                        RATConst.GreenMax,
                        RATConst.MaxCount,
                        RATConst.Name,
                        RATConst.RedMax,
                    ]
                ]
            ],
            table_type: Optional[
                Literal[
                    RATConst.THEMATIC,
                    RATConst.ATHEMATIC
                ]
            ] = RATConst.THEMATIC
    ):
        self._schema = pd.DataFrame(
            data=schema,
            columns=["field_name", "field_type", "field_usage"],
            index=None
        )
        # self._schema.set_index("field_name")
        if self._schema["field_name"].is_unique:
            self._schema["dtype"] = self._schema["field_type"].map(
                RAT.dtype_mapping
            )
            self._rat = gdal.RasterAttributeTable()
            self._rat.SetTableType(table_type)
            self._rat.SetTableType(table_type)
            for name, field_type, usage in schema:
                self._rat.CreateColumn(name, field_type, usage)
        else:
            raise ValueError("Schema contains duplicate field names!")

    def __call__(self) -> gdal.RasterAttributeTable:
        return self._rat.Clone()

    def reset(self):
        self._rat.SetRowCount(0)

    @property
    def schema(self) -> pd.DataFrame:
        return self._schema.copy()

    @property
    def table_type(self) -> int:
        return self._rat.GetTableType()

    @property
    def row_count(self) -> int:
        return self._rat.GetRowCount()

    @property
    def colum_count(self) -> int:
        return self._rat.GetColumnCount()

    def populate(self, df: pd.DataFrame) -> None:
        delta_rc = df.shape[0]
        row_count = self.row_count
        self._rat.SetRowCount(self.row_count + delta_rc)
        for idx, row in self.schema[["dtype"]].iterrows():
            self._rat.WriteArray(
                array=df.iloc[:, idx].values.astype(row["dtype"]),
                field=idx,
                start=row_count
            )


def gdal_extensions(driver: Union[str, gdal.Driver]) -> Sequence[str]:
    if isinstance(driver, str):
        driver = gdal.GetDriverByName(driver)
    if not isinstance(driver, gdal.Driver):
        raise ValueError("Driver is invalid!")

    exts = driver.GetMetadataItem("DMD_EXTENSIONS")
    # noinspection PyPropertyAccess
    return exts.split(" ") if exts else [
        driver.ShortName.replace(' ', '.').lower()
    ]
