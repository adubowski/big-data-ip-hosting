from pyspark.sql import SparkSession
from warcio.archiveiterator import ArchiveIterator
import json
import sys
import subprocess
import esutil
import multiprocessing as mp
from urlparse import urlparse

# Put your starting and ending index here
if len(sys.argv) <= 3 :
	start = 0
	end = 0
else:
	start = int(sys.argv[1])
	end = int(sys.argv[2])
print("Start has been set to {0}, end has been set to {1}".format(start, end))

# Do not touch the rest
wat_files = esutil.hdfs.ls("/user/s2017873/wat_files")
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

def get_dicts_for_path(path, queue):
	result = set()
	cat = subprocess.Popen(["hdfs", "dfs", "-cat", path], stdout=subprocess.PIPE)
	for record in ArchiveIterator(cat.stdout):
		 if record.rec_type =='metadata':
	 		# wat_json = spark.read.json(record.content_stream().read()).asDict()
			wat_json = json.loads(record.content_stream().read())
			payload = wat_json["Envelope"]["WARC-Header-Metadata"]
			if "WARC-IP-Address" in payload:
				url = payload["WARC-Target-URI"]
				ip = payload["WARC-IP-Address"]
				parsed_uri = urlparse(url)
				result.add((parsed_uri.netloc, ip))
	queue.put((path, list(result)))

def write_list_to_csv(queue, subset):
	for i in range(len(subset)):
		path, result = queue.get()
		df = sc.parallelize(list(result)).toDF()
		df = df.withColumnRenamed("_1", "url")\
	         	.withColumnRenamed("_2", "ip")
		dir_name = path.split("/")[-1].split(".")[0]
		file_path = "/user/s2017873/url_ip/" + dir_name
		df.write.csv(file_path)
		print("{0} / {1}".format(i, len(subset)))

subset = wat_files[start:end]
processes = []
q = mp.Queue()
writer = mp.Process(target=write_list_to_csv, args=(q, subset, ))
writer.start()
print("Started")
for path in subset:
	p = mp.Process(target=get_dicts_for_path, args = (path, q, ))
	processes.append(p)
	p.start()
writer.join()
