"""ETL component"""

import logging
from datetime import datetime
from typing import NamedTuple

import pandas as pd

from ETL_sc.common.meta_process import MetaProcess
from ETL_sc.common.s3 import S3BucketConnector



class EtlSourceConfig(NamedTuple):
    """
    Class for source (table) configuration data - these parameters should be configurable for the ETL job

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for ISIN in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volume in source

    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str

class EtlTargetConfig(NamedTuple):
    """
    Class for target configuration data - these parameters should be configurable for the ETL job

    trg_col_isin: column name for ISIN in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_dail_trade_vol: column name for daily traded volume in target
    trg_col_ch_prev_clos: column name for change to previous day's closing price in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file
    trg_format: file format of the target file

    """

    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trade_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class StockETL():
    "The ETL job. Reads the stock data, transforms and writes the transformed data to target."

    def __init__(self, s3_bucket_src: S3BucketConnector, s3_bucket_trg: S3BucketConnector,
                meta_key: str, src_args: EtlSourceConfig, trg_args: EtlTargetConfig):
        """
        Constructor

        Args:
            s3_bucket_src (S3BucketConnector): connection to source S3 bucket
            s3_bucket_trg (S3BucketConnector): connection to target S3 bucket
            meta_key (str): key of meta file in S3 bucket
            src_args (EtlSourceConfig): Namedtuple class with source configuration data
            trg_args (EtlTargetConfig): Namedtuple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args

        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.src_args.src_first_extract_date, self.meta_key, self.s3_bucket_trg)

        self.meta_update_list = [date for date in self.extract_date_list\
            if date >= self.extract_date] #TODO: futile?

    def extract(self):
        """

        Read the source files as per the dates in self.extract_date_list needed,
        and concatenates them to one Pandas DataFrame

        Returns:
             data_frame: Pandas DataFrame with the extracted data to transform, from source
        """
        self._logger.info('Extracting Stock-data (Xetra) source files started...')
        files = [key for date in self.extract_date_list\
                     for key in self.s3_bucket_src.list_files_in_prefix(date)] #for each hour there is a seperate file in bucket.
        if not files: #checking if list empty
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_as_df(file)\
                for file in files], ignore_index=True)
        self._logger.info('Extracting Stock-data (Xetra) source files finished.')
        return data_frame

    def transform_report1(self, data_frame: pd.DataFrame):
        """Applies the necessary transformation to create report 1

        Args:
            data_frame (pd.DataFrame): Pandas DataFrame as Input

        Returns:
            pd.DataFrame: Pandas DataFrame as Input
        """


        if data_frame.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return data_frame

        self._logger.info('Applying transformations to Xetra source data for report 1 started...')

        # Filtering necessary source columns
        data_frame = data_frame.loc[:, self.src_args.src_columns]

        # Removing rows with missing values
        data_frame.dropna(inplace=True)

         # Calculating opening price per ISIN and day
        data_frame[self.trg_args.trg_col_op_price] = data_frame\
            .sort_values(by=[self.src_args.src_col_time])\
                .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price]\
                    .transform('first')

         # Calculating closing price per ISIN and day
        data_frame[self.trg_args.trg_col_clos_price] = data_frame\
            .sort_values(by=[self.src_args.src_col_time])\
                .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price]\
                        .transform('last')

        # Renaming columns
        data_frame.rename(columns={
            self.src_args.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_args.src_col_max_price: self.trg_args.trg_col_max_price,
            self.src_args.src_col_traded_vol: self.trg_args.trg_col_dail_trade_vol
            }, inplace=True)

        # Aggregating per ISIN and day -> opening price, closing price,
        # minimum price, maximum price, traded volume
        data_frame = data_frame.groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date], as_index=False)\
                .agg({
                    self.trg_args.trg_col_op_price: 'min',
                    self.trg_args.trg_col_clos_price: 'min',
                    self.trg_args.trg_col_min_price: 'min',
                    self.trg_args.trg_col_max_price: 'max',
                    self.trg_args.trg_col_dail_trade_vol: 'sum'})

        # Change of current day's closing price compared to the
        # previous trading day's closing price in %
        data_frame[self.trg_args.trg_col_ch_prev_clos] = data_frame\
            .sort_values(by=[self.src_args.src_col_date])\
                .groupby([self.src_args.src_col_isin])[self.trg_args.trg_col_op_price]\
                    .shift(1)
        data_frame[self.trg_args.trg_col_ch_prev_clos] = (
            data_frame[self.trg_args.trg_col_op_price] \
            - data_frame[self.trg_args.trg_col_ch_prev_clos]
            ) / data_frame[self.trg_args.trg_col_ch_prev_clos ] * 100

        # Rounding to 2 decimals
        data_frame = data_frame.round(decimals=2)

        # Removing the day before extract_date
        data_frame = data_frame[data_frame.Date >= self.extract_date].reset_index(drop=True)
        self._logger.info('Applying transformations to Xetra source data finished...')
        return data_frame

    def load(self):
        pass

    def etl_report1(self):
        pass





