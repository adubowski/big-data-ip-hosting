import pickle
import subprocess
import os
import multiprocessing as mp
download_prefix = "https://commoncrawl.s3.amazonaws.com/"
existing_files = os.listdir('wat_files')

def change_filename(path):
    path = path.replace("/warc/", "/wat/")
    path = path.replace("warc.gz", "warc.wat.gz")
    filename = path.split("/")[-1]
    return filename, path
url_index = []
with open('url_file.txt') as f:
    text = f.read()
    url_index = text.split("\n")

url_index = list(map(change_filename, url_index))
url_index = [path for (filename, path) in url_index if filename not in existing_files]
with open("to_download.txt", "w+") as f:
    for index in url_index:
            f.write(download_prefix + index + "\n")
command_arr = ['wget', '-P', 'wat_files', '-i', 'to_download.txt']
subprocess.run(command_arr)


