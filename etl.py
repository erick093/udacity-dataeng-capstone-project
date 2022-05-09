import configparser
import os
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from helpers import *
from etl_helpers import *

config = configparser.ConfigParser()
config.read('config.cfg', encoding='utf-8')

os.environ['AWS_ACCESS_KEY_ID'] = config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark session that is able to read from S3
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data):
    """
    Process the Immigration Dataset
    :param spark: Spark session
    :param input_data: Input data directory
    :param output_data: Output data directory
    """
    # define the immigration data folder
    immigration_data = input_data + "i94_apr16_sub.sas7bdat"

    # read the SAS file in spark
    immigration_df = spark.read.format("com.github.saurfang.sas.spark").load(immigration_data)

    # clean the immigration df
    clean_immigration_df = clean_dataframe_in_spark(df=immigration_df, columns_to_drop=['occup', 'entdepu', 'insnum',
                                                                                        'dtadfile', 'dtaddto',
                                                                                        'visapost'])

    # create immigration fact table
    immigration_df = create_immigration_fact_table(spark=spark, immigration_df=clean_immigration_df,
                                                   output_data=output_data)

    # create calendar dim table
    create_calendar_dim_table(spark=spark, immigration_df=immigration_df, output_data=output_data)


def process_immigration_labels_data(spark, input_data, output_data):
    """
    Process the immigration labels data
    :param spark: Spark session
    :param input_data: Input data directory
    :param output_data: Output data directory
    """
    # define the immigration labels data folder
    immigration_labels = input_data + "I94_SAS_Labels_Descriptions.SAS"

    # create visa dim table
    create_visa_dim_table(spark=spark, sas_filename=immigration_labels, output_data=output_data)

    # create country dim table
    create_country_dim_table(spark=spark, sas_filename=immigration_labels, output_data=output_data)

    # create ports dim table
    create_ports_dim_table(spark=spark, sas_filename=immigration_labels, output_data=output_data)

    # create us states dim table
    create_us_states_dim_table(spark=spark, sas_filename=immigration_labels, output_data=output_data)


def process_demographics_data(spark, input_data, output_data):
    """
    Process demographics data
    :param spark: Spark session
    :param input_data: Input data directory
    :param output_data: Output data directory
    """
    # define the demographics' data folder
    demographics_data = input_data + "us-cities-demographics.csv"

    # read the demographics data in spark
    demographics_df = spark.read.csv(demographics_data, header=True, inferSchema=True, sep=";")

    # clean the demographics data
    clean_demographics_df = clean_dataframe_in_spark(df=demographics_df, columns_to_drop=[])

    # create demographics table
    create_demographics_dim_table(spark=spark, demographics_df=clean_demographics_df, output_data=output_data)


def process_airport_data(spark, input_data, output_data):
    """
    Process Aiport Codes data
    :param spark: Spark session
    :param input_data: Input data directory
    :param output_data: Output data directory
    """
    # define the airport codes data folder
    airport_codes = input_data + "airport-codes.csv"

    # read the airport codes data in spark
    airport_codes_df = spark.read.csv(airport_codes, header=True, inferSchema=True)

    # clean the airport codes data
    clean_airport_codes_df = clean_dataframe_in_spark(df=airport_codes_df, columns_to_drop=[])

    # create airport codes table
    create_us_airports_dim_table(spark=spark, us_airports_df=clean_airport_codes_df, output_data=output_data)


def perform_quality_checks(spark, output_data):
    """
    Perform quality checks
    :param spark: Spark session
    :param output_data: Output data directory
    """
    # define the dataframes to check
    dataframes_to_check = ["calendar_dim", "country_dim", "demographics_dim", "immigration_fact", "ports_dim",
                           "us_airports_dim", "us_states_dim", "visa_dim"]

    # read the parquet dataframe and check
    for df_name in dataframes_to_check:
        df = spark.read.parquet(f"{output_data}/{df_name}.parquet")
        df_count = df.count()
        if df_count == 0:
            raise ValueError(f"quality-check-count not passed!: {df_name} is empty")
        else:
            log_info(f"quality-check-count passed!: {df_name} has {df_count} rows")
        # delete the dataframe from memory
        df.unpersist()


def main():
    """
    Main function
    """
    # Create a Spark session
    log_info('Creating a Spark Session')
    spark = create_spark_session()

    # define the input and output paths
    input_data = "datasets/"
    output_data = "tables_/"
    # input_data = "s3a://dateng-capstone-datasets/"
    # output_data = "s3a://dataeng-capstone-tables/"

    log_info('Processing immigration labels')
    process_immigration_labels_data(spark=spark, input_data=input_data, output_data=output_data)

    log_info("Processing immigration dataset")
    process_immigration_data(spark=spark, input_data=input_data, output_data=output_data)

    log_info("Processing airport codes dataset")
    process_airport_data(spark=spark, input_data=input_data, output_data=output_data)

    log_info("Processing demographics dataset")
    process_demographics_data(spark=spark, input_data=input_data, output_data=output_data)

    log_info("Performing quality checks")
    perform_quality_checks(spark=spark, output_data=output_data)

    log_info("Closing the Spark Session")
    spark.stop()


if __name__ == '__main__':
    main()
    # x = input("Press enter to terminate")
