import boto3
import datetime
import json
import os
import requests
import time

from multiprocessing import Pool


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
        self.query_api()

    def get_filelist(self):
        with open(self.params["data_path"], "r") as f:
            self.filelist = sorted([x.strip("\n") for x in f.readlines()])

    def configure_url(self):
        base_url = "https://catalogue.onda-dias.eu/dias-catalogue/Products"
        self.order_url = base_url + '({})'
        if "MSIL1C" in self.filelist[0]:
            self.granule_url = base_url + '?$search="name:{}.zip"'
        else:
            self.granule_url = self.order_url

    def query_api(self):
        self.count = 0
        self.pids = []
        self.downloaded = []
        for file in self.filelist:
            self.count += 1
            if self.count <= self.params["max_requests"]:
                query_url = self.granule_url.format(file)
                r = requests.get(query_url)
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
                        self.filelist.remove(file)
                        self.downloaded.append(local_filename)
                        self.count -= 1
                    else:
                        print(" ".join([f"File: {results['name']}",
                                        f"Expected Size: {expected_size}",
                                        f"Downloaded Size: {actual_size}",
                                        "do not agree.",
                                        "Deleting the partial download."
                                        ]
                                       )
                              )
                elif results["downloadable"]:
                    print(" ".join([f"Granule: {results['name']}",
                                    f"Status: {results['downloadable']}"
                                    ]
                                   )
                          )
                    self.download_granule(self.pid)
                    self.count -= 1
                elif not results["downloadable"]:
                    print(" ".join([f"Granule: {results['name']}",
                                    f"Status: {results['downloadable']}"
                                    ]
                                   )
                          )
                    self.restore_granule()
            else:
                self.request_manager()

    def request_manager(self):
        now = datetime.datetime.now()
        end_time = now + datetime.timedelta(minutes=60)
        print(" ".join([f"{self.params['max_requests']}",
                        "requests have been submitted. Waiting",
                        f"{self.params['time_lag_in_minutes']}",
                        "Minutes to download restored scenes"
                        ]
                       )
              )
        print(f"Current Time: {datetime.datetime.now()}")
        time.sleep(self.params["time_lag_in_minutes"]*60)
        print(f"Downloading {len(self.pids)} restored granules.")
        print(len(self.filelist))
        if len(self.pids) > 0:
            with Pool(len(self.pids)) as p:
                p.map(self.download_granule, self.pids)

        print(" ".join(["Successfully restored all of the scenes.",
                        f"Waiting until {end_time}",
                        "to query the API again"
                        ]
                       )
              )
        print(f"Current Time: {datetime.datetime.now()}")
        print(len(self.filelist))
        self.update_database()
        if self.params["push_to_s3"]:
            self.movetoS3()

        while datetime.datetime.now() < end_time:
            time.sleep(60)
        else:
            self.query_api()

    def download_granule(self, pid):
        data_url = self.order_url.format(pid) + "/$value"
        r = requests.get(self.order_url.format(pid))
        results = json.loads(r.content)
        r = requests.get(data_url,
                         auth=self.auth, stream=True
                         )
        print(f"Downloading {results['name']}")
        expected_size = results['size']/(1024*1024)
        local_filename = f"{self.local_dir}/{results['name']}"
        with open(local_filename, "wb") as fd:
            for chunk in r.iter_content(chunk_size=1024):
                fd.write(chunk)
        download_size = os.stat(local_filename).st_size/(1024*1024)
        if download_size == expected_size:
            self.filelist.remove(results["name"].strip(".zip"))
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

    def restore_granule(self):
        self.pids.append(self.pid)
        restore_url = self.order_url.format(self.pid) + "/Ens.Order"
        r = requests.post(restore_url,
                          auth=self.auth
                          )
        results = json.loads(r.content)
        print(" ".join([
                        f"Status: {results['Status']}",
                        f"Message: {results['StatusMessage']}",
                        f"Estimated Restored Time: {results['EstimatedTime']}"
                        ]
                       )
              )

    def update_database(self):
        with open(self.params["data_path"], "w") as f:
            f.writelines("%s\n" % file for file in self.filelist)

    def movetoS3(self):
        aws_config = self.params["aws"]
        session = boto3.Session(profile_name=aws_config["aws_profile"])
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
            s3.upload_file(dl, target_bucket, key)
            os.remove(dl)


if __name__ == "__main__":
    queryOnda()
