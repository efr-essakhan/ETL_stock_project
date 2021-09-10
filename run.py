"""Entry point for running the Stock data ETL application"""

import argparse
import logging
import logging.config
from os import access

import yaml

from ETL_sc.common.s3 import S3BucketConnector
from ETL_sc.transformers.etl_transformer import StockETL, EtlSourceConfig, EtlTargetConfig


def main():
    """
    Entry point to run the Stock-data ETL job
    """

    #Parsing the YAML (configuration) file.

    #Submit the path of config file as an argument
    parser = argparse.ArgumentParser(description="Run the stock-data ETL job") #parser reads launch.json
    parser.add_argument('config', help='A path to configuration file, in YAML format')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))


    #Congifure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config) #loading the config as a dictionary
    logger = logging.getLogger(__name__)

    #Reading our S3 configuration from YAML
    s3_config = config['s3']

    # creating the S3BucketConnector class instances for source and target
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['src_endpoint_url'],
                                      bucket_name=s3_config['src_bucket'])

    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket_name=s3_config['trg_bucket'])

    # reading source configuration
    source_config = EtlSourceConfig(**config['source']) #** allows dictionaries to be submitted as keyword arguments.
    # reading target configuration
    target_config = EtlTargetConfig(**config['target'])
    # reading meta file configuration
    meta_config = config['meta']

    # creating StockETL class instance
    logger.info('Xetra ETL job started')
    stock_etl = StockETL(s3_bucket_src, s3_bucket_trg,
                         meta_config['meta_key'], source_config, target_config)
    # running etl job for xetra report1
    stock_etl.etl_report1()
    logger.info('Xetra ETL job finished.')










if __name__ == '__main__':
    main()