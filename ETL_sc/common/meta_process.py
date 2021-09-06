"""
Methods for processing the meta file

The meta-file purpose is to hold checkpoint data of what day reports have been done already and at what time the processing of the stock-data was done.
Purpose is so that it can be used to auto-generate weekly
report based on this data, without redundancy - dates that have already been done are not included in the report.

"""
from datetime import datetime, timedelta
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

    #Try to understand and create flowchart for this
    @staticmethod
    def return_date_list(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Creating a list of dates (to be used to for extracting of data int the ETL pipeline). List is based on the
        the input first_date and the already processed dates in the meta_file

        Args:
            first_date (str): The earliest date Stock date (Xetra) should be processed
            meta_key (str): key of the meta_file on the S3 bucket
            s3_bucket_meta (S3BucketConnector): S3BucketConnector for the bucket with the meta file

        Returns:
            return_min_date: first date that should be processed
            return_date_list: list of all dates from first_day-1 till today
        """

        #We need one day before the first_date to do the transformation during E'T'L
        start_date = datetime.strftime(first_date,
                                  MetaProcessFormat.META_DATE_FORMAT.value)\
                                      .date() - timedelta(days=1) #Start_date = first_Date - 1

        today = datetime.today().date()

        try:
            #If meta file exists in S3 bucket -> create return_date_list using the content of the meta-file
            #Reading meta-file
            df_meta = s3_bucket_meta.read_csv_as_df(meta_key) #Would throw exception if non-existance

            #Creating a set of all dates in meta-file
            src_dates = set(pd.to_datetime(
                df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]
            ).dt.date)

            #Creating a list of dates from start_date until today
            dates = [start_date + timedelta(days=x) for x in range(0, \
                (today-start_date).days+1)] #using start_date instead of first_date as it is already a datetime obj.

            #We dont want to process for the ETL report any duplicate dates, thus do new_dates-old_dates = unprocessed_dates
            dates_missing = set(dates[1:]) - src_dates #[1:] because we want to skip the start_date

            if dates_missing: #if set has values.
                #Determining the earliest date that should be selected
                min_date = min(dates_missing) - timedelta(days=1)
                #TODO: APPEARS FUTILE
                return_min_date = (min_date + timedelta(days=1)) \
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)

                #Creating a list of dates from min_date until today
                return_dates = [
                    date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                        for date in dates if date >= min_date
                ]
            else:
                #Setting values for the earliest date and the list of dates
                return_dates = []
                return_min_date = datetime(2200,1,1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)

        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            #No meta-file found -> creating a date list from first_Date-1 day untiltoday
            return_min_date = first_date
            return_dates = [\
                (start_date + timedelta(day=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) for x in range(0, (today-start_date).days+1)
            ]

        return return_min_date, return_dates












