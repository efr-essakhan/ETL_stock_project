"""Entry point for running the Stock data ETL application"""

import logging
import logging.config

import yaml


def main():
    """
    Entry point to run the Stock-data ETL job
    """
    #Parsing the YAML (configuration) file.
    config_path = r'C:\Users\Essa\Documents\GitHub\ETL_Stock_project\ETL_stock_project\configs\etl_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    print(config)

    #Congifure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config) #loading the config as a dictionary
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")






if __name__ == '__main__':
    main()