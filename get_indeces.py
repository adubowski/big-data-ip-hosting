import pickle
import requests
import json
import csv
import time
import multiprocessing as mp
url_list = []
cc = "http://index.commoncrawl.org/CC-MAIN-2020-50-index"
with open("/home/s2017873/top-1m.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    i = 0
    for row in csv_reader:
            i += 1
            url_list.append(row[1])

def get_indeces_from_url(tup):
    url, queue = tup
    get_string = cc + "/?url=" + url + "/&output=json"
    code = 0
    while code != 200:
            r = requests.get(get_string)
            code = r.status_code
            if r.status_code == 200:
                    json_obj_list = r.text.split("\n")
                    json_obj_list.pop()
                    l = set()
                    for json_obj in json_obj_list:
                            j = json.loads(json_obj)
                            if '/warc/' in j['filename']:
                                    l.add(j['filename'])
                    queue.put(l)
                    break
            elif r.status_code == 404:
                    queue.put(set())
                    break
            elif r.status_code == 503:
                    time.sleep(0.5)
            else :
                    print(r.status_code)
            time.sleep(0.5)
# Partition urls to 1000 workers that each do 1000 urls

def write_to_urls(queue):
    url_set = set()
    for i in range(top_k):
            new_urls = queue.get()
            print("{0} / {1}".format(i, top_k))
            to_write = new_urls.difference(url_set)
            url_set = url_set.union(new_urls)
            with open("url_file.txt", "a") as f:
                    for url in to_write:
                            f.write(url + "\n")
    print("Done")
p = mp.Pool(4)
m = mp.Manager()
q = m.Queue()
top_k = 100
url_list = url_list[:top_k]
url_list = map(lambda url : (url, q), url_list)
writer = mp.Process(target=write_to_urls, args=(q, ))
writer.start()
p.map(get_indeces_from_url, url_list)
p.join()
writer.join()
