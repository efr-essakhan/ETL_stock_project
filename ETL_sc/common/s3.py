"""Connector and methods accessing S3"""

import os
import boto3

class S3BucketConnector():
    """
    Class for interacting with a specified AWS S3 bucket
    """

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket_name: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3 
        :param bucket_name: S3 bucket name to connect to
        """

        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                        aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url) #single underline means protected variable, double underline means private variable. _s3 <- protected var data members of a class that can be accessed within the class and the classes derived from that class.
        self._bucket = self._s3.Bucket(bucket_name)


        def list_files_in_prefix(self):
            pass

        def read_csv_as_df(self):
            pass

        def write_df_to_s3(self):
            pass

        



