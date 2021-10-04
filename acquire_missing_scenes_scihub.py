import boto3
import datetime
import json
import os
import requests
import time
import xmltodict

from multiprocessing import Manager, Pool


class queryOnda():

    def __init__(self):
        with open("query_params_scihub.json", "r") as f:
            self.params = json.load(f)
        username = self.params["auth"]["username"]
        password = self.params["auth"]["password"]
        threads = self.params["threads"]
        self.auth = (username, password)
        self.local_dir = self.params['output_directory']
        self.get_filelist()
        self.configure_url()
        self.get_fileinfo()
        self.downloaded = Manager().list()
        pool = Pool(threads)
        progress = pool.map_async(self.download_granule, self.fileinfo.keys())
        while not progress.ready():
            if len(self.downloaded) > 0:
                self.movetoS3()
            time.sleep(30)

    def get_filelist(self):
        with open(self.params["data_path"], "r") as f:
            self.filelist = sorted([x.strip("\n") for x in f.readlines()])
            self.filelist = Manager().list(self.filelist)

    def configure_url(self):
        base_url = "https://scihub.copernicus.eu/dhus/"
        if "MSIL1C" in self.filelist[0]:
            self.granule_url = base_url + 'search?q=identifier:{}'

    def get_fileinfo(self):
        self.count = 0
        self.pids = []
        self.fileinfo = {}
        for file in self.filelist:
            query_url = self.granule_url.format(file)
            r = requests.get(query_url,auth=self.auth, timeout=5)
            response = xmltodict.parse(r.text)["feed"]["entry"]
            expected_size = float(response["summary"].split(":")[-1].strip(" MB"))
            download_url = response["link"][0]["@href"]
            filename = response["title"]
            self.fileinfo[filename] = {"size": expected_size, "url": download_url}

    def download_granule(self,file):
        expected_size = self.fileinfo[file]["size"]
        data_url = self.fileinfo[file]["url"]
        r = requests.get(data_url,
                         auth=self.auth, stream=True,
                         timeout=5
                         )
        print(f"Downloading {file}, Expected Size: {expected_size} - Current Time: {datetime.datetime.now()}")
        local_filename = f"{self.local_dir}/{file}.zip"
        with open(local_filename, "wb") as fd:
            for chunk in r.iter_content(chunk_size=1024):
                fd.write(chunk)
        download_size = os.stat(local_filename).st_size/(1024*1024)
        if abs(download_size - expected_size) < 1:
            self.downloaded.append(local_filename)
        else:
            print(" ".join([f"File: {file}.zip",
                            f"Expected Size: {expected_size}",
                            f"Downloaded Size: {download_size}",
                            "do not agree. Deleting the partial download."
                            ]
                           )
                  )
            os.remove(local_filename)
        print(f"Finished Download of {file}.zip: {datetime.datetime.now()}")
        
    def update_database(self):
        with open(self.params["data_path"], "w") as f:
            f.writelines("%s\n" % file for file in self.filelist)

    def movetoS3(self):
        aws_config = self.params["aws"]
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

        for dl in self.downloaded:
            fname = dl.split("/")[-1]
            data_date = datetime.datetime.strptime(fname.split("_")[2],
                                                   "%Y%m%dT%H%M%S"
                                                   )
            key = f"{data_date:%m-%d-%Y}/{fname}"
            print(" ".join([f"local file: {dl} is being uploaded to:"
                            f" {target_bucket}/{key}"
                            ]
                           )
                 )
            s3.upload_file(dl, target_bucket, key,
                    ExtraArgs={'ACL': 'bucket-owner-full-control'})
            os.remove(dl)
            self.filelist.remove(fname.strip(".zip"))
            self.downloaded.remove(dl)
            self.update_database()


if __name__ == "__main__":
    queryOnda()
