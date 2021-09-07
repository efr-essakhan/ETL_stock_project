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
            extract_date_list (list): A list of dates that are extracted from the source - these are meant to be processed and used to make-up the ETL stock-data report.
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

    #TODO: May have to simplify to bs to be a straightforwards, get last 7 days kind of thing.
    @staticmethod
    def return_date_list(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Creating a list of dates (to be used to for extracting of correctly dated data into the ETL pipeline). List is based on the
        the input first_date and the already processed dates in the meta_file.

        Note: The upper-bound date is always today's date. But the lower-bound/minimum date is figured out utlizing meta-file.

        1)

        Args:
            first_date (str): The desired earliest date Stock date (Xetra) should be processed
            meta_key (str): key of the meta_file on the S3 bucket
            s3_bucket_meta (S3BucketConnector): S3BucketConnector for the bucket with the meta file

        Returns:
            return_min_date (str): first date that should be processed
            return_date_list: list of all dates from first_day-1 till today that have not been processed previously.
        """

        #We need one day before the first_date to do the transformation during E'T'L - this value could be discarded and not used if it aleady exists in meta-file
        first_date_minus1 = datetime.strptime(first_date,
                                  MetaProcessFormat.META_DATE_FORMAT.value)\
                                      .date() - timedelta(days=1) #Convert first_date string into datetime obj and get previous day date

        #This will be the upper limit of the dates.
        today = datetime.today().date()

        try: # If meta file exists in S3 bucket -> create return_date_list utilizing the content of the meta-file

            #Reading meta-file
            df_meta = s3_bucket_meta.read_csv_as_df(meta_key) #Would throw exception if non-existance of meta_file

            #Create a set out of a list of datetime's taken from the 'source_date' column of df_meta
            meta_dates_set = set(pd.to_datetime(
                df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]
            ).dt.date)

            #Creating a list of dates from first_date_minus1 until today
            dates = [first_date_minus1 + timedelta(days=x) for x in range(0, \
                (today-first_date_minus1).days+1)] #Have to add +1 because range's final output is one less then upper bound. Thus here the list produced goes from Todays date -> first_date_minus1 , else:  Todays date -> first_date

            #We dont want to process for the ETL report any duplicate dates, thus do new_dates-old_dates = dates_not_in_meta (unprocessed dates)
            dates_not_in_meta  = set(dates[1:]) - meta_dates_set #[1:] because we don't want to remove the date's upper limit i.e. todays date

            if len(dates_not_in_meta) != 0: #I.e. there are unique dates to process


                return_min_date = min(dates_not_in_meta) \
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)

                #Determining the earliest date that should be selected (lower limit)
                min_date_minus1 = min(dates_not_in_meta) - timedelta(days=1)  #We need one day before the return_min_date to do the transformation during E'T'L

                #Creating a list of dates from min_date_minus1 until today
                return_dates = [
                    date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                        for date in dates if date >= min_date_minus1
                ]

            else: #There are no unqiue dates to process
                #Setting these dummy values to later handle as exceptions
                return_dates = []
                return_min_date = datetime(2200,1,1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)

        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            #No meta-file found -> creating a date list from first_Date-1 day untiltoday
            return_min_date = first_date
            return_dates = [\
                (first_date_minus1 + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) for x in range(0, (today-first_date_minus1).days+1)
            ]

        return return_min_date, return_dates

     @staticmethod
    def return2_date_list(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):

        #We need one day before the first_date to do the transformation during E'T'L - this value could be discarded and not used if it aleady exists in meta-file
        first_date_minus1 = datetime.strptime(first_date,
                                  MetaProcessFormat.META_DATE_FORMAT.value)\
                                      .date() - timedelta(days=1) #Convert first_date string into datetime obj and get previous day date

        #This will be the upper limit of the dates.
        today = datetime.today().date()

        try:
             #Reading meta-file
            df_meta = s3_bucket_meta.read_csv_as_df(meta_key) #Would throw exception if non-existance of meta_file

            #Create a set out of a list of datetime's taken from the 'source_date' column of df_meta
            meta_dates_set = set(pd.to_datetime(
                df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]
            ).dt.date)


        except:













