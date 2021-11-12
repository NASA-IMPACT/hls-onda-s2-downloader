import boto3
import datetime
import json
import os
import requests
import time

from multiprocessing import Manager, Pool


class queryOnda():

    def __init__(self):
        with open("query_params.json", "r") as f:
            self.params = json.load(f)
        username = self.params["auth"]["username"]
        password = self.params["auth"]["password"]
        self.auth = (username, password)
        self.local_dir = self.params['output_directory']
        self.get_filelist()
        self.configure_url()
        for key in self.files:
            print(f"Downloading {len(self.files[key])} files for {key}")
            self.filelist = self.files[key]
            while len(self.files[key]) > 0:
                self.query_api()
                self.files[key] = self.filelist
                self.update_database()
                self.query_api()


    def get_filelist(self):
        with open(self.params["data_path"],"r") as f:
            self.files = json.load(f)

    def configure_url(self):
        base_url = "https://catalogue.onda-dias.eu/dias-catalogue/Products"
        self.order_url = base_url + '({})'
        self.granule_url = base_url + '?$search="name:{}.zip"'

    def query_api(self):
        self.count = 0
        self.pids = []
        self.downloaded = Manager().list()
        for file in self.filelist:
            if self.count < self.params["max_requests"]:
                query_url = self.granule_url.format(file)
                r = requests.get(query_url)
                if len(json.loads(r.content)["value"]) == 0:
                    continue
                else:
                    results = json.loads(r.content)["value"][0]
                self.pid = results["id"]
                local_filename = f"{self.local_dir}/{results['name']}"
                if os.path.exists(local_filename):
                    expected_size = results["size"]/(1024*1024)
                    actual_size = os.stat(local_filename).st_size/(1024*1024)
                    if actual_size == expected_size:
                        print(" ".join([f"{results['name']} has already",
                                        "been downloaded! Starting next file."
                                        ]
                                       )
                              )
                        self.downloaded.append(local_filename)
                    else:
                        print(" ".join([f"File: {results['name']}",
                                        f"Expected Size: {expected_size}",
                                        f"Downloaded Size: {actual_size}",
                                        "do not agree.",
                                        "Deleting the partial download."
                                        ]
                                       )
                              )
                        os.remove(local_filename)
                elif results["downloadable"]:
                    print(" ".join([f"Granule: {results['name']}",
                                    f"Status: {results['downloadable']}"
                                    ]
                                   )
                          )
                    self.pids.append(self.pid)
                    if len(self.pids) == 50:
                        self.downloaded = Manager().list()
                        self.request_manager()
                        self.update_database()
                        self.pids = []

                if file == self.filelist[-1]:
                    self.request_manager()
            else:
                self.request_manager()

    def request_manager(self):
        now = datetime.datetime.now()
        print(f"Downloading {len(self.pids)} restored granules.")
        print(len(self.filelist))
        if len(self.pids) > 0:
            with Pool(len(self.pids)) as p:
                p.map(self.download_granule, self.pids)
        if self.params["push_to_s3"]:
            self.movetoS3()

    def download_granule(self, pid):
        data_url = self.order_url.format(pid) + "/$value"
        r = requests.get(self.order_url.format(pid))
        results = json.loads(r.content)
        r = requests.get(data_url,
                         auth=self.auth, stream=True
                         )
        print(f"Downloading {results['name']}: {datetime.datetime.now()}")
        expected_size = results['size']/(1024*1024)
        local_filename = f"{self.local_dir}/{results['name']}"
        with open(local_filename, "wb") as fd:
            for chunk in r.iter_content(chunk_size=1024):
                fd.write(chunk)
        download_size = os.stat(local_filename).st_size/(1024*1024)
        if download_size == expected_size:
            self.downloaded.append(local_filename)
        else:
            print(" ".join([f"File: {results['name']}",
                            f"Expected Size: {expected_size}",
                            f"Downloaded Size: {download_size}",
                            "do not agree. Deleting the partial download."
                            ]
                           )
                  )
            os.remove(local_filename)
        print(f"Finished Download of {results['name']}: {datetime.datetime.now()}")

        with open(self.params["data_path"], "w") as f:
            json.dump(self.files,f)

    def movetoS3(self):
        session = boto3.Session()
        s3 = boto3.client("s3")
        aws_config = self.params["aws"]
        target_bucket = aws_config["target_s3_bucket"]

        for dl in self.downloaded:
            fname = dl.split("/")[-1]
            key = fname
            print(" ".join([f"local file: {dl} is being uploaded to:"
                            f" {target_bucket}/{key}"
                            ]
                           )
                 )
            s3.upload_file(dl, target_bucket, key,
                    ExtraArgs={'ACL': 'bucket-owner-full-control'})
            os.remove(dl)
            self.filelist.remove(fname.strip(".zip"))


    def update_database(self):
        with open(self.params["data_path"],"w") as f:
            json.dump(self.files,f)

if __name__ == "__main__":
    queryOnda()
