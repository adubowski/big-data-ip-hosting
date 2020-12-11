"""
This script ...

To execute on a machine:
 time spark-submit template.py 2> /dev/null
"""

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode, desc, col, lit
from pyspark.sql import SQLContext

sc = SparkContext(appName="template")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

# Read the data
df = spark.read.json("/data/doina/Twitter-Archive.org")
