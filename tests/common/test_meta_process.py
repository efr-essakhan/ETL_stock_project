"""Test MetaProcess methods"""

from ETL_sc.common.constants import MetaProcessFormat
from datetime import date, timedelta
from ETL_sc.common.custom_exceptions import *
from ETL_sc.common.s3 import S3BucketConnector
from ETL_sc.common.meta_process import MetaProcess
from io import StringIO


import pandas as pd
from datetime import datetime, timedelta
import os

import unittest
import boto3
from moto import mock_s3


class TestMetaProcessMethods(unittest.TestCase):
    """Testing MetaProcess class"""

    def setUp(self):
        """
        Setting up the environment
        """

        #mocking S3 connections start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        #Defining the class arguments for the mock S3
        self.s3_access_key = 'AWS_ACCESS_KEY_ID' #can be any string really
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY' #can be any string really
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating S3 mock access-keys as environment variabels.
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating a bucket on the mocked S3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-central-1'
                              })
        #Create bucket instance
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        #Creating a connection (that connects to mock S3 bucket) as testing instance (what will be tested)
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key, self.s3_secret_key,
                                                self.s3_endpoint_url, self.s3_bucket_name)

        #List comprehension to get last 7 days (from today) as a list
        self.dates = [(datetime.today().date() - timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]

        def tearDown(self):
            # Mocking S3 connection stop
            self.mock_s3.stop()




    def test_update_meta_file_no_meta_file(self):
        """Tests the update_meta_file when there is no meta file present in source bucket.
            Expected:
           1. Pass new list of dates (assumed to have been processed) into method
           2. S3 bucket contains no Meta-file and thus will create new meta-file; storing list of dates & corresponding (todays) processed dates
           3. In test we check the new file created in (2) for correctness against
        """

        #Expected result
        date_list_exp = ['2021-04-16', '2021-04-17'] #Expect this to be returned
        proccesed_date_list_exp = [datetime.today().date()] *2 #If we have no meta_file we give date_list_exp as an argument, and a meta-file should be created and it should be added with these two processed dates.
        #Test init
        meta_key = 'meta.csv'
        #Method execution
        MetaProcess.update_meta_file(date_list_exp, meta_key, self.s3_bucket_conn)

        #Read meta file
        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proccesed_date_list_result = list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value])\
                .dt.date
        )
        #Test after method exection
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proccesed_date_list_exp, proccesed_date_list_result)
        #Clean-up after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]

            }
        )

if __name__ == "__main__":
    unittest.main()


