"""File to store constants"""

from enum import Enum

class S3FileTypes(Enum):
    """Supported file types for S3BucketConnector"""

    CSV = 'csv'
    PARQUET = 'parquet'



class MetaProcessFormat(Enum):
    """These constants are used in the formation of MetaProcess Class in meta_process.py"""
    META_FILE_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESSED_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_FILE_DATE_COL = 'source_file_date'
    META_PROCESSED_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'
