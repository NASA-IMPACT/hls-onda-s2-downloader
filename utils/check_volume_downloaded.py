import json
import requests


with open("params.json", "r") as f:
    params = json.load(f)

with open(params["original_list"], "r") as f:
    old_list = [x.strip("\n") for x in f.readlines()]

with open(params["new_list"], "r") as f:
    new_list = [x.strip("\n") for x in f.readlines()]

for file in new_list:
    old_list.remove(file)

print(len(old_list))
base_url = "https://catalogue.onda-dias.eu/dias-catalogue/Products"
granule_url = base_url + '?$search="name:{}.zip"'

total_size = 0

for file in old_list:
    query_url = granule_url.format(file)
    r = requests.get(query_url, stream=False)
    results = json.loads(r.content)["value"][0]

    total_size += results["size"]

total_size_in_gb = total_size/(1024*1024*1024)


print(f"Total donwloaded volume: {total_size_in_gb} GB")
