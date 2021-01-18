
import pyspark
from warcio.archiveiterator import ArchiveIterator
import warc
import esutil
import json
from pyspark.sql import SparkSession
from pyspark import SparkFiles, SparkConf


conf = SparkConf()
conf.appName = 'master-yarn'
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
# m1_df = spark.read.csv('/user/s2017873/top-1m.csv.gz')
wat_files = esutil.hdfs.ls('/user/s2017873/wat_files/', False, False)

def get_records_for_path(path):
	result = []
	with esutil.hdfs.opent('hdfs:' + path) as stream:
		for record in ArchiveIterator(stream):
			if record.rec_type =='metadata':
				# wat_json = spark.read.json(record.content_stream().read()).asDict()
				wat_json = json.loads(record.content_stream().read())
				if "WARC-IP-Address" in wat_json["Envelope"]["WARC-Header-Metadata"]:
					ip = wat_json["Envelope"]["WARC-Header-Metadata"]["WARC-IP-Address"]
					url = wat_json["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
					no_https = url.split("//")[1]
					if not ('/' in no_https):
						result.append((no_https, ip))
	return result

rdd = sc.parallelize(wat_files)
new_rdd = rdd.flatMap(lambda path: get_records_for_path(path))

# m1_rdd = m1_df.rdd.map(lambda (rank, url) : (url, rank))
#with open("/home/s2017873/log4.txt", "w+") as f:
#	f.write(str(new_rdd.take(1)))
# final_rdd = m1_rdd.leftOuterJoin(new_rdd)
# joined_rdd = m1_rdd.join(new_rdd).take(1)
#with open("/home/s2017873/log4.txt", 'w+') as f:
#	first = final_rdd.take(1)
#	f.write(str(first))
new_rdd.saveAsTextFile("hdfs:///user/s2017873/FINAL")

# joined_rdd.saveAsTextFile("output_folder")
# final_rdd.saveAsTextFile("url_rank_ip.csv")
