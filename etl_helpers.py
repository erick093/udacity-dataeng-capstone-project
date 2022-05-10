import pyspark.sql.functions as F
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, dayofweek, dayofmonth, month, year, weekofyear, \
    monotonically_increasing_id
from datetime import datetime, timedelta
from helpers import log, log_info, log_error, read_sas_labels_file


def create_immigration_fact_table(spark, immigration_df, output_data):
    """
    Creates the Immigration table
    :param output_data: output data folder
    :param spark: spark session
    :param immigration_df: Immigration data dataframe
    """
    # Define the UDF to convert SAS date format to Datetime format
    convert_to_datetime = F.udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(int(x))).isoformat() if x else None)

    # Apply the UDF to the dataframe
    log_info("Converting the SAS date format to Datetime format")
    immigration_df = immigration_df.withColumn("arrdate", convert_to_datetime(immigration_df.arrdate))
    immigration_df = immigration_df.withColumn("depdate", convert_to_datetime(immigration_df.depdate))

    # Cast columns to integers
    columns_to_cast_to_int = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94visa', 'i94mode', 'biryear',
                              'i94bir', 'admnum', 'i94mode', 'i94bir', 'count']
    for column in columns_to_cast_to_int:
        log_info(f"Casting {column} to integer")
        immigration_df = immigration_df.withColumn(column, immigration_df[column].cast(IntegerType()))

    # rename the columns
    immigration_df = immigration_df.withColumnRenamed('cicid', 'record_id') \
        .withColumnRenamed('i94yr', 'entry_year') \
        .withColumnRenamed('i94mon', 'entry_month') \
        .withColumnRenamed('i94cit', 'country_citizenship') \
        .withColumnRenamed('i94res', 'country_residence') \
        .withColumnRenamed('arrdate', 'arrival_date') \
        .withColumnRenamed('depdate', 'departure_date') \
        .withColumnRenamed('i94visa', 'visa_category') \
        .withColumnRenamed('i94mode', 'mode_of_entry') \
        .withColumnRenamed('biryear', 'birth_year') \
        .withColumnRenamed('i94addr', 'state_code') \
        .withColumnRenamed('i94port', 'port_of_entry') \
        .withColumnRenamed('i94bir', 'age') \
        .withColumnRenamed('admnum', 'admission_number') \
        .withColumnRenamed('fltno', 'flight_number') \
        .withColumnRenamed('visatype', 'visa_type') \
        .withColumnRenamed('entdepa', 'arrival_flag') \
        .withColumnRenamed('entdepd', 'departure_flag') \
        .withColumnRenamed('matflag', 'match_flag') \

    # Write dataframe to Parquet
    immigration_df.write.partitionBy("visa_category").parquet(output_data + "immigration_fact.parquet", mode="overwrite")

    return immigration_df


def create_visa_dim_table(spark, sas_filename, output_data):
    """
    Create the visa dimensional table
    :param spark: spark session
    :param sas_filename: sas file name
    :param output_data: output data directory
    """
    # read the specific labels information from the SAS descriptions file
    visa_tuple_data = read_sas_labels_file(sas_filename=sas_filename,
                                           label="I94VISA")

    # create the schema of the visa dim table
    schema = StructType(
        [
            StructField("visa_category_id", StringType()),
            StructField("visa_category_name", StringType()),
        ]
    )

    # create the visa dim table
    visa_dim_df = spark.createDataFrame(data=visa_tuple_data,
                                        schema=schema)

    # cast the visa_category_id to integer
    visa_dim_df = visa_dim_df.withColumn("visa_category_id", visa_dim_df["visa_category_id"].cast(IntegerType()))

    # write the visa dim table to parquet
    visa_dim_df.write.parquet(output_data + "visa_dim.parquet", mode="overwrite")


def create_country_dim_table(spark, sas_filename, output_data):
    """
    Create the country dim table
    :param spark: spark session
    :param sas_filename: sas file name
    :param output_data: output data directory
    """
    # read the specific labels information from the SAS descriptions file
    country_tuple_data = read_sas_labels_file(sas_filename=sas_filename,
                                              label="I94CIT")

    # create the schema of the country dim table
    schema = StructType(
        [
            StructField("country_id", StringType()),
            StructField("country_name", StringType()),
        ]
    )

    # create the country dim table
    country_dim_df = spark.createDataFrame(data=country_tuple_data,
                                           schema=schema)

    # cast the country_id to integer
    country_dim_df = country_dim_df.withColumn("country_id", country_dim_df["country_id"].cast(IntegerType()))

    # write the country dim table to parquet
    country_dim_df.write.parquet(output_data + "country_dim.parquet", mode="overwrite")


def create_ports_dim_table(spark, sas_filename, output_data):
    """
    Create the ports dim table
    :param spark: spark session
    :param sas_filename: sas file name
    :param output_data: output data directory
    """
    # read the specific labels information from the SAS descriptions file
    ports_tuple_data = read_sas_labels_file(sas_filename=sas_filename,
                                            label="I94PORT")

    # create the schema of the ports dim table
    schema = StructType(
        [
            StructField("port_id", StringType()),
            StructField("port_name", StringType()),
        ]
    )

    # create the ports dim table
    ports_dim_df = spark.createDataFrame(data=ports_tuple_data,
                                         schema=schema)

    # write the ports dim table to parquet
    ports_dim_df.write.parquet(output_data + "ports_dim.parquet", mode="overwrite")


def create_us_states_dim_table(spark, sas_filename, output_data):
    """
    Create the us states dim table
    :param spark: spark session
    :param sas_filename: sas file name
    :param output_data: output data directory
    """
    # read the specific labels information from the SAS descriptions file
    us_states_tuple_data = read_sas_labels_file(sas_filename=sas_filename,
                                                label="I94ADDR")

    # create the schema of the US states dim table
    schema = StructType(
        [
            StructField("us_state_id", StringType()),
            StructField("us_state_name", StringType()),
        ]
    )

    # create the US states dim table
    us_states_dim_df = spark.createDataFrame(data=us_states_tuple_data,
                                             schema=schema)

    # write the US states dim table to parquet
    us_states_dim_df.write.parquet(output_data + "us_states_dim.parquet", mode="overwrite")


def create_us_airports_dim_table(spark, us_airports_df, output_data):
    """
    Create the us airports dim table
    :param spark: spark session
    :param us_airports_df: us airports dataframe
    :param output_data: output data directory
    """
    # filter the dataframe to only include airports in the US
    us_airports_df = us_airports_df.where("iso_country = 'US' and iata_code is not null")

    # extrac the state code from the iso_region_code
    us_airports_df = us_airports_df.withColumn("state_code", regexp_replace('iso_region', 'US-', ''))

    # drop the iso_region column
    us_airports_df = us_airports_df.drop("iso_region")

    # write the us airports dim table to parquet
    us_airports_df.write.parquet(output_data + "us_airports_dim.parquet", mode="overwrite")


def create_calendar_dim_table(spark, immigration_df, output_data):
    """
    Creates a calendar dim table
    :param spark: Spark session
    :param immigration_df: immigration dataframe
    :param output_data: output data directory
    """
    # select the distinct arrival and departure date
    calendar_df_arrival = immigration_df.select("arrival_date").distinct()
    calendar_df_departure = immigration_df.select("departure_date").distinct()

    # Merge the two dataframes
    calendar_df = calendar_df_arrival.union(calendar_df_departure)

    # Rename the column
    calendar_df = calendar_df.withColumnRenamed('arrival_date', 'calendar_date')

    # create the day, week, month, year and weekday columns
    calendar_df = calendar_df.withColumn('arrival_day', dayofmonth('calendar_date'))
    calendar_df = calendar_df.withColumn('arrival_week', weekofyear('calendar_date'))
    calendar_df = calendar_df.withColumn('arrival_month', month('calendar_date'))
    calendar_df = calendar_df.withColumn('arrival_year', year('calendar_date'))
    calendar_df = calendar_df.withColumn('arrival_weekday', dayofweek('calendar_date'))

    # create an id column
    calendar_df = calendar_df.withColumn('calendar_id', monotonically_increasing_id())

    # write the calendar dim table to parquet
    calendar_df.write.parquet(output_data + "calendar_dim.parquet", mode="overwrite")


def create_demographics_dim_table(spark, demographics_df, output_data):
    """
    Creates a demographics dim table
    :param spark: Spark session
    :param demographics_df: demographics dataframe
    :param output_data: output data directory
    """
    # rename the columns
    demographics_df = demographics_df.withColumnRenamed('City', 'city') \
        .withColumnRenamed('State', 'state') \
        .withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Average Household Size', 'average_household_size') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('State Code', 'state_code') \
        .withColumnRenamed('Race', 'race') \
        .withColumnRenamed('Count', 'count') \

    # Cast columns to integers
    columns_to_cast_to_int = ['male_population', 'female_population', 'number_veterans', 'foreign_born']
    for column in columns_to_cast_to_int:
        log_info(f"Casting {column} to integer")
        demographics_df = demographics_df.withColumn(column, demographics_df[column].cast(IntegerType()))

    # Create an id column
    demographics_df = demographics_df.withColumn('record_id', monotonically_increasing_id())

    # write the demographics dim table to parquet
    demographics_df.write.parquet(output_data + "demographics_dim.parquet", mode="overwrite")







