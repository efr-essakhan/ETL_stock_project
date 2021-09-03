"""Connector and methods accessing S3"""

import os
import logging
import boto3
import io
import pandas as pd

from botocore.vendored.six import StringIO

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
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3',
                                         endpoint_url=endpoint_url) #single underline means protected variable, double underline means private variable. _s3 <- protected var data members of a class that can be accessed within the class and the classes derived from that class.
        self._bucket = self._s3.Bucket(bucket_name)


    def list_files_in_prefix(self, prefix: str):
        """listing all files/objects in an S3 bucket with a specific prefix.

        Args:
            prefix (str): prefix on the S3 bucket that should be filtered with.

        Returns:
            list: list of all the file/object names containing the prefix in their key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files #files names in reality

    def read_csv_as_df(self, key: str, encoding: str = 'utf-8', sep = ','):
        """Reading the csv file from the S3 bucket and returning the file as a dataframe

        Args:
            key (str): key of the file that should be read
            encoding (str, optional): encoding of the data inside the csv file. Defaults to 'utf-8'.
            sep (str, optional): seperator of the csv file. Defaults to ','.

        Returns:
            data_frame: Pandas dataframe containing the data of the CSV file.
        """
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)

        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding) #Get the specified object using its key from the bucket.
        data = io.StringIO(csv_obj) #Convert the CSV object into string format into memory - so that it can be used without saving into hdd (its the alternative accpeted by pandas)
        data_frame = pd.read_csv(data, sep=sep)
        return data_frame

    def write_df_to_s3(self):
        pass