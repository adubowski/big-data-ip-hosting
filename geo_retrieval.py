import esutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

# Build a Spark session.
spark = SparkSession.builder.getOrCreate()

# Define the schema for the UDF function.
schema = StructType([
    StructField("city", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("country_iso_code", StringType(), True),
    StructField("subdivision", StringType(), True),
    StructField("subdivision_iso_code", StringType(), True)
])


def ip_string_to_location(ip_string):
    """
    This function uses GeoIP2 to retrieve location data for a given IP. The ip should be given as a string, for example
    '127.0.0.1'. It can also be an IPv6 address. No guarantees are made by this function about the output data, any
    value in the output tuple could be None.
    """
    from geoip2 import database as gd
    from geoip2.errors import AddressNotFoundError
    city = None
    longitude = None
    latitude = None
    postal_code = None
    country = None
    country_iso = None
    subdivision = None
    subdivision_iso = None

    # The GitHub getting started guide of geoip2 was used as a reference: https://github.com/maxmind/GeoIP2-python
    with gd.Reader("ipv4.mmdb") as reader:
        try:
            response = reader.city(ip_string)
            if response.city is not None and response.city.name is not None:
                city = response.city.name.encode("utf-8").strip()
            if response.location is not None:
                longitude = response.location.longitude
                latitude = response.location.latitude
            if response.postal is not None:
                postal_code = response.postal.code
            if response.country is not None and response.country.name is not None:
                country = response.country.name.encode("utf-8").strip()
                country_iso = response.country.iso_code
            if response.subdivisions is not None and response.subdivisions.most_specific.name is not None:
                subdivision = response.subdivisions.most_specific.name.encode("utf-8").strip()
                subdivision_iso = response.subdivisions.most_specific.iso_code
        except AddressNotFoundError:
            pass

    # Return all data, this tuple will automatically be packed in the schema (defined above).
    return city, longitude, latitude, postal_code, country, country_iso, subdivision, subdivision_iso


# Make a Spark UDF with the above function.
udf_ip_string_to_location = udf(ip_string_to_location, schema)

# 1. Retrieve all CSV files.
files = esutil.hdfs.ls("/user/s2017873/url_ip/*/*.csv")
# 2. Read them.
df = spark.read.csv(files)
# 3. Rename columns
df = df.withColumnRenamed("_c0", "domain")
df = df.withColumnRenamed("_c1", "ip")
# 4. Select, including the UDF above applied on the IP.
df = df.select(
    df.domain,
    df.ip,
    udf_ip_string_to_location(df.ip).alias("location")
)

# 5. Filter out all rows that contain NULL for the location data.
df = df.where(col("location.city").isNotNull())

# 6. Flatten the schema, so we can write it to a CSV.
df = df.select(
    df.domain,
    df.ip,
    df.location.city,
    df.location.latitude,
    df.location.longitude,
    df.location.postal_code,
    df.location.country,
    df.location.country_iso_code,
    df.location.subdivision,
    df.location.subdivision_iso_code
)

# Output all data to CSV files, to be downloaded or used by some other person or system.
df.write.mode("overwrite").csv("/user/s1932195/locations")
