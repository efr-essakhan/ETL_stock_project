"""Test S3bucketConnector methods"""

import os
import unittest

import boto3

from moto import mock_s3

from ETL_sc.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """Testing the S3BucketConnector class

    Args:
        unittest ([type]): [description]
    """

    def setUp(self):
        """Method used to initilize our test (ran each time)
        """

        #mocking S3 connections start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        #Defining the class arguments for the mock S3 buckets
        self.s3_access_key = 'AWS_ACCESS_KEY_ID' #can be any string really
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY' #can be any string really
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating S3 mock access-keys as environment variabels.
        os.environ[self.s3_access_key] = 'KEY1' # TODO: This is a real env variable, but we remove it later i guess.
        os.environ[self.s3_secret_key] = 'KEY2'
        #Creating a bucket on the mocked S3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocalConstraint': 'eu-central-1'
                              })
        #Create bucket instance
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        #Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key, self.s3_secret_key,
                                                self.s3_endpoint_url, self.s3_bucket_name)





    def tearDown(self):
        """Ran each time after the unit tests have been done."""

        # mocking s3 connection stop
        self.mock_s3.stop()
        pass

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting 2 files/objects keys
        as returned list, from the mocked s3 bucket.
        """
        pass
    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method in case of a wrong or not-existing prefix.
        """
        pass


if __name__ == "__main__":
    unittest.main();


