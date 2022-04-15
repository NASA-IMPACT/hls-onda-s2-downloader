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
        print(f"{len(self.filelist)} files to download")
        self.configure_url()
        self.query_api()

    def get_filelist(self):
        with open(self.params["data_path"],"r") as f:
            self.files = json.load(f)
        self.filelist = []
        for key in self.files:
            #print(f"Downloading {len(self.files[key])} files for {key}")
            if len(self.files[key]) > 0:
                self.filelist.extend(self.files[key])

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
                print(file)
                r = requests.get(query_url)
                if len(json.loads(r.content)["value"]) == 0:
                    print(f"{file} is not available via OndaDIAS")
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
                    if len(self.pids) == 50 or file == self.filelist[-1]:
                        self.downloaded = Manager().list()
                        with Pool(len(self.pids)) as p:
                            p.map(self.download_granule, self.pids)
                        self.movetoS3()
                        self.update_database()
                        self.pids = []
                elif not results["downloadable"]:
                    print(" ".join([f"Granule: {results['name']}",
                                    f"Status: {results['downloadable']}"
                                    ]
                                   )
                          )
                    status = self.restore_granule()
                    if status == "Error":
                        self.count = 20
                    else:
                        self.count += 1

                if file == self.filelist[-1]:
                    self.request_manager()
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
        if self.params["push_to_s3"]:
            self.movetoS3()

        print(" ".join([f"Current Time: {datetime.datetime.now()}",
                        f"Next Query Time: {end_time}"
                        ]
                        )
              )
        self.update_database()
        print(len(self.filelist))
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
    def restore_granule(self):
        self.pids.append(self.pid)
        restore_url = self.order_url.format(self.pid) + "/Ens.Order"
        r = requests.post(restore_url,
                          auth=self.auth
                          )
        try:
            results = json.loads(r.content)
            return " ".join([
                            f"Status: {results['Status']}",
                            f"Message: {results['StatusMessage']}",
                            f"Estimated Restored Time: {results['EstimatedTime']}"
                            ]
                           )
        except json.decoder.JSONDecodeError:
            print(r.content)
            return "Error"
        except:
            print("Something happened. Exiting.")
            exit()


    def update_database(self):

        with open(self.params["data_path"], "w") as f:
            json.dump(self.files,f)

    def get_archive_S3key(self,filename):
        file_comp = filename.split("_")
        self.date = datetime.datetime.strptime(file_comp[2],"%Y%m%dT%H%M%S")
        utm_info = file_comp[5]
        utm_zone = utm_info[1:3]
        lat_band = utm_info[3]
        square = utm_info[4:]
        new_key = f"{utm_zone}/{lat_band}/{square}/{self.date:%Y/%-m/%-d}/{filename}"
        return new_key

    def movetoS3(self):
        aws_config = self.params["aws"]
        session = boto3.Session()#profile_name=aws_config["aws_profile"])
        s3 = boto3.client("s3")
        target_bucket = aws_config["target_s3_bucket"]
        archive_bucket = aws_config.get("archive_bucket")

        for dl in set(self.downloaded):
            fname = dl.split("/")[-1]
            key = f"{fname}"
            s3.upload_file(dl, target_bucket, key,
                    ExtraArgs={'ACL': 'bucket-owner-full-control'})
            if archive_bucket is not None:
                archive_key = self.get_archive_S3key(fname)
                print(" ".join([f"local file: {dl} is being uploaded to:"
                                f" {archive_bucket}/{archive_key}"
                                ]
                               )
                     )
                s3.upload_file(dl, archive_bucket, archive_key,
                    ExtraArgs={'ACL': 'bucket-owner-full-control'})

            os.remove(dl)
            self.files[f'{self.date:%Y-%m-%d}'].remove(fname.strip(".zip"))
        self.update_database()
        self.get_filelist()

if __name__ == "__main__":
    queryOnda()
