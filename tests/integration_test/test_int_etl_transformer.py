"""Integration Testing StockETL Methods"""

import unittest
import boto3
import os
import pandas as pd
from io import BytesIO#
from datetime import datetime, timedelta

from ETL_sc.common.s3 import S3BucketConnector
from ETL_sc.transformers.etl_transformer import EtlSourceConfig, EtlTargetConfig, StockETL
from ETL_sc.common.meta_process import MetaProcess
from ETL_sc.common.constants import MetaProcessFormat


class IntTestStockETLMethods(unittest.TestCase):
    """
        Integration Testing the StockETL class.
    """

    def setUp(self):
        """
        Setting up the environment
        """

        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name_src = 'stock-integration-test-src'
        self.s3_bucket_name_trg = 'stock-integration-test-trg'
        self.meta_key = 'meta_file.csv'

        # Creating the source and target bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)

        self.src_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket = self.s3.Bucket(self.s3_bucket_name_trg)

        # Creating S3BucketConnector testing instances#
        self.s3_bucket_src = S3BucketConnector(self.s3_access_key,
                                            self.s3_secret_key,
                                            self.s3_endpoint_url,
                                            self.s3_bucket_name_src)
        self.s3_bucket_trg = S3BucketConnector(self.s3_access_key,
                                            self.s3_secret_key,
                                            self.s3_endpoint_url,
                                            self.s3_bucket_name_trg)
        # Creating a list of dates
        self.dates = [(datetime.today().date() - timedelta(days=day))\
            .strftime(MetaProcessFormat.META_FILE_DATE_FORMAT.value) for day in range(8)]

        # Creating source and target configuration
        conf_dict_src = {
            'src_first_extract_date': self.dates[3],
            'src_columns': ['ISIN', 'Mnemonic', 'Date', 'Time',
            'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }
        conf_dict_trg = {
            'trg_col_isin': 'isin',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price_eur',
            'trg_col_clos_price': 'closing_price_eur',
            'trg_col_min_price': 'minimum_price_eur',
            'trg_col_max_price': 'maximum_price_eur',
            'trg_col_dail_trade_vol': 'daily_traded_volume',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_key': 'report1/xetra_daily_report1_',
            'trg_key_date_format': '%Y%m%d_%H%M%S',
            'trg_format': 'parquet'
        }

        self.source_config = EtlSourceConfig(**conf_dict_src)
        self.target_config = EtlTargetConfig(**conf_dict_trg)

        # Creating mock source files on mocked s3 - this is easier to transform rather then getting real data from Xetra
        columns_src = ['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice',
        'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
        data = [['AT0000A0E9W5', 'SANT', self.dates[5], '12:00', 20.19, 18.45, 18.20, 20.33, 877],
                ['AT0000A0E9W5', 'SANT', self.dates[4], '15:00', 18.27, 21.19, 18.27, 21.34, 987],
                ['AT0000A0E9W5', 'SANT', self.dates[3], '13:00', 20.21, 18.27, 18.21, 20.42, 633],
                ['AT0000A0E9W5', 'SANT', self.dates[3], '14:00', 18.27, 21.19, 18.27, 21.34, 455],
                ['AT0000A0E9W5', 'SANT', self.dates[2], '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
                ['AT0000A0E9W5', 'SANT', self.dates[2], '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
                ['AT0000A0E9W5', 'SANT', self.dates[1], '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
                ['AT0000A0E9W5', 'SANT', self.dates[1], '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
                ['AT0000A0E9W5', 'SANT', self.dates[1], '09:00', 24.22, 22.21, 22.21, 25.01, 1523]]
        self.df_src = pd.DataFrame(data, columns=columns_src)
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[0:0], #[start row, end row] thus this: first row
        f'{self.dates[5]}/{self.dates[5]}_BINS_XETR12.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[1:1], #second row
        f'{self.dates[4]}/{self.dates[4]}_BINS_XETR15.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[2:2],
        f'{self.dates[3]}/{self.dates[3]}_BINS_XETR13.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[3:3],
        f'{self.dates[3]}/{self.dates[3]}_BINS_XETR14.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[4:4],
        f'{self.dates[2]}/{self.dates[2]}_BINS_XETR07.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[5:5],
        f'{self.dates[2]}/{self.dates[2]}_BINS_XETR08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[6:6],
        f'{self.dates[1]}/{self.dates[1]}_BINS_XETR07.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[7:7],
        f'{self.dates[1]}/{self.dates[1]}_BINS_XETR08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[8:8],
        f'{self.dates[1]}/{self.dates[1]}_BINS_XETR09.csv','csv')

         #report to be produced
        columns_report = ['ISIN', 'Date', 'opening_price_eur', 'closing_price_eur',
        'minimum_price_eur', 'maximum_price_eur', 'daily_traded_volume', 'change_prev_closing_%']
        data_report = [['AT0000A0E9W5', f'{self.dates[3]}', 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
                       ['AT0000A0E9W5', f'{self.dates[2]}', 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
                       ['AT0000A0E9W5', f'{self.dates[1]}', 23.58, 24.22, 22.21, 25.01, 3586, 14.58]]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self):
        #clean bucket from setUP stuff.

        for file in self.src_bucket.objects.all():
            file.delete()
        for file in self.trg_bucket.objects.all():
            file.delete()

    #TODO: do more integration tests, even if u think the unittests are enough.
    def test_integration_etl_report1_no_metafile(self):
        """
        Integration Test for the etl_report1 method with no meta-file present in S3
        """
        # Expected results
        df_exp = self.df_report
        meta_exp = [self.dates[3], self.dates[2], self.dates[1], self.dates[0]]

        # Method execution
        xetra_etl = StockETL(self.s3_bucket_src, self.s3_bucket_trg,
        self.meta_key, self.source_config, self.target_config)
        xetra_etl.etl_report1()

        # Test after method execution
        trg_file = self.s3_bucket_trg.list_files_in_prefix(self.target_config.trg_key)[0]
        data = self.trg_bucket.Object(key=trg_file).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)

        self.assertTrue(df_exp.equals(df_result))

        meta_file = self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_as_df(meta_file)
        self.assertEqual(list(df_meta_result[MetaProcessFormat.META_FILE_DATE_COL.value]), meta_exp)


if __name__ == "__main__":
    unittest.main(); #Calls Setup> all tests > tear-down
