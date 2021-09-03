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
                                  'LocationConstraint': 'eu-central-1'
                              })
        #Create bucket instance
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        #Creating a connection (that connects to mock bucket) as testing instance (what will be tested)
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key, self.s3_secret_key,
                                                self.s3_endpoint_url, self.s3_bucket_name)





    def tearDown(self):
        """Ran each time after the unit tests have been done."""

        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting 2 files/objects keys
        as (returned as) list, from the mocked s3 bucket.
        """
        #Test init/setup
        prefix = 'prefix/'
        key1_exp = f'{prefix}test1.csv'
        key2_exp = f'{prefix}test2.csv'
        csv_content =  """"col1,col2
        valA,valB"""
        ##Putting 2 new object with the same prefix in the mock bucket
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        #Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix)
        #Test after method execution
        ##Check if returned list has 2 items/objects names
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        #Cleanup after test
        self.s3_bucket.delete_objects( #This operation enables you to delete multiple objects from a bucket using a single HTTP request (provide details in XML format). You may specify up to 1000 keys. https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method in case of a wrong or not-existing prefix.
        """
        #Test init/setup
        prefix_expected = 'no-prefix/'
        #Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_expected)
        #Test after method execution
        self.assertTrue(not list_result) #checks if list_result is empty



if __name__ == "__main__":
    unittest.main(); #Calls Setup> all tests > tear-down

    #If you just want one test to be executed
    # testIns = TestS3BucketConnectorMethods()
    # testIns.setUp()
    # testIns.test_list_files_in_prefix_ok() #add a print statement to the method
    # testIns.tearDown()


