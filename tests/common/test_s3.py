"""Test S3bucketConnector methods"""

from io import StringIO, BytesIO
import os
import unittest

import boto3
import pandas as pd

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

    def test_read_csv_to_df_ok(self):
        """Tests the read_csv_as_df method for
        reading one .csv file from the mocked s3 bucket - checking if retrieved file matches to the
        Dataframe formulated from it.
        """

        #Expected results
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        #Test init/Setup
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp) #upload csv content into the mock S3
        # Method execution
        with self.assertLogs() as logm: #picks up any loggings
            df_result = self.s3_bucket_conn.read_csv_as_df(key_exp)
            # Logging test after method execution
            self.assertIn(log_exp, logm.output[0]) #output holds the logs printed in order

        #Test after method execution
        self.assertEqual(df_result.shape[0], 1)  #df_result.shape = (row, col) = (1,2)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp, df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])
        #Clean-up after test
        self.s3_bucket.delete_objects( #deleting objects from mock
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_empty(self):
        """Tests the write_df_to_s3 method with an empty Dataframe as input
        """
        #Expected results
        return_exp = None
        log_exp = 'The dataframe is empty! No file will be written!'
        #Test init
        df_empty = pd.DataFrame()
        key = 'key.csv'
        file_format = 'csv'
        #Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_empty, key, file_format)
            #Log test
            self.assertIn(log_exp, logm.output[0])

        #Test after method execution
        self.assertEqual(return_exp, result)

    def test_write_df_to_s3_csv(self):
        """Tests the write_df_to_s3 method if writing csv is successfull
        """

        #Expected result
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.csv'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        #Test init
        file_format = 'csv'
        #Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format) #storing the data to S3
            #Logging
            #self.assertIn(log_exp, logm[0])

        #Retrieving the data back for testing
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)

        #Test after method execution
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        #Clean-up after test
        self.s3_bucket.delete_objects( #deleting objects from mock
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )




    def test_write_df_to_s3_parquet(self):
        """Tests the write_df_to_s3 method if writing to parquet is successfull
        """
        #Expected result
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.parquet'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        #Test init
        file_format = 'parquet'
        #Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format) #storing the data to S3
            #Logging
            #self.assertIn(log_exp, logm[0])

        #Retrieving the data back for testing
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)

        #Test after method execution
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        #Clean-up after test
        self.s3_bucket.delete_objects( #deleting objects from mock
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )






if __name__ == "__main__":
    unittest.main(); #Calls Setup> all tests > tear-down

    #If you just want one test to be executed
    # testIns = TestS3BucketConnectorMethods()
    # testIns.setUp()
    # testIns.test_list_files_in_prefix_ok() #add a print statement to the method
    # testIns.tearDown()


