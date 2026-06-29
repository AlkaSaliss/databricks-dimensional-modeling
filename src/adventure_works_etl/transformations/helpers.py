import base64
import struct

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType


def record_hash(*cols, namespace=None):
    hash_cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    if namespace is not None:
        hash_cols = [F.lit(namespace), *hash_cols]

    return F.sha2(F.concat_ws("|", *hash_cols), 256)


def individual_survey_field(xml_col, field_name):
    value = F.trim(
        F.xpath_string(
            xml_col,
            F.lit(f"/*[local-name()='IndividualSurvey']/*[local-name()='{field_name}']/text()"),
        )
    )
    return F.when(F.length(value) > 0, value)


spatial_schema = StructType(
    [
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)


@F.udf(spatial_schema)
def parse_spatial_location(value):
    if value is None:
        return None

    try:
        b = base64.b64decode(value)

        if len(b) < 22:
            return None

        srid = struct.unpack("<I", b[0:4])[0]

        if srid != 4326:
            return None

        lat, lon = struct.unpack("<dd", b[6:22])

        return {
            "latitude": float(lat),
            "longitude": float(lon),
        }

    except Exception:
        return None
