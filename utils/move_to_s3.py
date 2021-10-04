import boto3
import datetime
import glob
import json
import os
import requests

with open("../query_params.json", "r") as f:
    params = json.load(f)

aws_config = params["aws"]
session = boto3.Session()#profile_name=aws_config["aws_profile"])
client = session.client("sts")
roleArn = aws_config["upload_role_arn"]
roleSessionName = aws_config["upload_role_name"]
creds = client.assume_role(
                           RoleArn=roleArn,
                           RoleSessionName=roleSessionName
                           )
creds = creds["Credentials"]
s3 = boto3.client("s3",
                  aws_access_key_id=creds["AccessKeyId"],
                  aws_secret_access_key=creds["SecretAccessKey"],
                  aws_session_token=creds["SessionToken"]
                  )
target_bucket = aws_config["target_s3_bucket"]

downloaded = glob.glob(f"../{params['output_directory']}/*.zip")
base_url = "https://catalogue.onda-dias.eu/dias-catalogue/Products"
granule_url = base_url + '?$search="name:{}"'

with open("params.json", "r") as f:
    list_params = json.load(f)

with open(list_params["new_list"], "r") as f:
    filelist = sorted([x.strip("\n") for x in f.readlines()])

print(len(filelist))

for dl in downloaded:
    fname = dl.split("/")[-1]
    query_url = granule_url.format(fname)
    r = requests.get(query_url)
    results = json.loads(r.content)["value"][0]
    expected_size = results["size"]/(1024*1024)
    actual_size = os.stat(dl).st_size/(1024*1024)
    if expected_size == actual_size:
        data_date = datetime.datetime.strptime(fname.split("_")[2],
                                               "%Y%m%dT%H%M%S"
                                               )
        key = f"{data_date:%m-%d-%Y}/{fname}"
        print(" ".join([f"local file: {dl} is being uploaded to:"
                        f" {target_bucket}/{key}"
                        ]
                       )
              )
        s3.upload_file(dl, target_bucket, key)
        if fname.strip(".zip") in filelist:
            filelist.remove(fname.strip(".zip"))
        else:
            print(f"{fname.strip('.zip')} not in input file list")
        os.remove(dl)
    else:
        print(" ".join([f"File: {results['name']}",
                        f"Expected Size: {expected_size}",
                        f"Downloaded Size: {actual_size}",
                        "do not agree.",
                        "Deleting the partial download."
                        ]
                       )
              )
print(len(filelist))

with open(list_params["new_list"], "w") as f:
    f.writelines("%s\n" % file for file in filelist)
