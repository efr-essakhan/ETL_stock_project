"""
Methods for processing the meta file

The meta-file purpose is to hold checkpoint data of what day reports have been done already and at what time the processing of the stock-data was done.
Purpose is so that it can be used to auto-generate weekly
report based on this data, without redundancy - dates that have already been done are not included in the report.

"""
from datetime import datetime
from ETL_sc.common.s3 import S3BucketConnector
from ETL_sc.common.constants import MetaProcessFormat
import pandas as pd
import collections
from ETL_sc.common.custom_exceptions import *

class MetaProcess():
    """Class for woking with the meta file"""

    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_conn: S3BucketConnector):
        """
        Updating the meta-file with the processed Stock-data dates, and todays-date as prodcessed-date.

        Args:
            extract_date_list (list): A list of dates that are extracted from the source
            meta_key (str): key/name of the meta-file on the S3 bucket
            s3_bucket_conn (S3BucketConnector): S3BucketConnector for the bucket with the meta-file

        Raises:
            WrongMetaFileException: Raise when the meta-file format is not correct.

        Returns:
            Boolean: Return True if the update is successfull
        """

        #Preparing new data to add to meta_file
        #Creating an empty DataFrame using the meta-file column names.
        df_new = pd.DataFrame(columns=[
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value])
        #Filling the data columns with values in extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        #Filling the processed column with today's date
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = \
            datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)

        try:
            # Attempting to retrieve old meta-file
            df_old = s3_bucket_conn.read_csv_as_df(meta_key) #retrive metafile - Can throw exception: NoSuchKey ,which is caught

            #If retrieved -> check if in correct format
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns): #checking if same columns
                raise WrongMetaFileException
            else:
                #If meta file exists & in correct formar -> then union/appnend DataFrame of old and the new meta-data created.
                df_all = pd.concat([df_old, df_new])

        except s3_bucket_conn.session.client('s3').exceptions.NoSuchKey:
            # No meta-file exists -> then only the new data is used to create new meta-file
            df_all = df_new

        #Writing new/updated meta-file to S3
        s3_bucket_conn.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True





    @staticmethod
    def return_date_list():
        pass