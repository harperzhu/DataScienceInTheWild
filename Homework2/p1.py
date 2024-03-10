import sys
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,DoubleType, IntegerType
from pyspark.sql.functions import when, col, count,lit

def create_dataframe(filepath, format, spark):
    """
    Create a spark df given a filepath and format.

    :param filepath: <str>, the filepath
    :param format: <str>, the file format (e.g. "csv" or "json")
    :param spark: <str> the spark session

    :return: the spark df uploaded
    """   

    spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()
    spark_df = spark.read.format(format).load(filepath)
    if format.lower() == 'csv':
        spark_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(filepath)
    elif format.lower() == 'json':
        spark_df = spark.read.json(filepath)

    return spark_df



def transform_nhis_data(nhis_df):
    """
    Transform df elements

    :param nhis_df: spark df
    :return: spark df, transformed df
    """
    
    nhis_df = nhis_df.withColumn("AGE_P", map_age_udf(col("AGE_P")))
    # Rename the columns to match the BRFSS dataset
    nhis_df = nhis_df.withColumnRenamed("AGE_P", "_AGEG5YR")\
                     .withColumnRenamed("MRACBPI2", "_IMPRACE")
    
    nhis_df = nhis_df.dropna(subset=["_AGEG5YR", "SEX", "_IMPRACE", "DIBEV1"])
    # Drop rows with any null values in the key columns
    return nhis_df

def map_age_to_brfss_category(age):
    if age >= 80:
        return 13  # Age 80 or older
    elif age >= 75:
        return 12  # Age 75 to 79
    elif age >= 70:
        return 11  # Age 70 to 74
    elif age >= 65:
        return 10  # Age 65 to 69
    elif age >= 60:
        return 9   # Age 60 to 64
    elif age >= 55:
        return 8   # Age 55 to 59
    elif age >= 50:
        return 7   # Age 50 to 54
    elif age >= 45:
        return 6   # Age 45 to 49
    elif age >= 40:
        return 5   # Age 40 to 44
    elif age >= 35:
        return 4   # Age 35 to 39
    elif age >= 30:
        return 3   # Age 30 to 34
    elif age >= 25:
        return 2   # Age 25 to 29
    elif age >= 18:
        return 1   # Age 18 to 24
    else:
        return 14  # Don't know/Refused/Missing

map_age_udf = udf(map_age_to_brfss_category, IntegerType())

def report_summary_stats(joined_df):
    """
    Calculate prevalence statistics

    :param joined_df: the joined df

    :return: None
    # """

    # try:
    #     joined_df.show(truncate=False)
    #     print('I AM RIGHT HERE')
    # except Exception as e:
    #     print(f"An error occurred: {e}")
        
    # joined_df.show(truncate=False) 
    #Calculate disease prevalence by race
    # race_prevalence = joined_df.groupBy('_IMPRACE').agg(
    #     count(when(col('DIBEV1') == 1, True)).alias('Disease_Prevalence'),
    #     count(lit(1)).alias('Total_Count')
    # ).withColumn('Prevalence_Rate', (col('Disease_Prevalence') / col('Total_Count')) * 100)
    
    # Calculate disease prevalence by gender
    gender_prevalence = joined_df.groupBy('SEX').agg(
        count(when(col('DIBEV1') == 1, True)).alias('Disease_Prevalence'),
        count(lit(1)).alias('Total_Count')
    ).withColumn('Prevalence_Rate', (col('Disease_Prevalence') / col('Total_Count')) * 100)
    
    # # Calculate disease prevalence by BRFSS categorical age
    # age_prevalence = joined_df.groupBy('_AGEG5YR').agg(
    #     count(when(col('DIBEV1') == 1, True)).alias('Disease_Prevalence'),
    #     count(lit(1)).alias('Total_Count')
    # ).withColumn('Prevalence_Rate', (col('Disease_Prevalence') / col('Total_Count')) * 100)

    # # Print the statistics for disease prevalence
    # # debug this
    # print("Disease Prevalence by Race and Ethnic Background:")
    # race_prevalence.show(truncate=False)
    
    #debug this
    print("Disease Prevalence by Gender:")
    gender_prevalence.show(truncate=False)
    
    # print("Disease Prevalence by BRFSS Categorical Age:")
    # age_prevalence.show(truncate=False)

    pass

def join_data(brfss_df, nhis_df):
    """
    Join dataframes

    :param brfss_df: spark df
    :param nhis_df: spark df after transformation
    :return: the joined df

    """
    
    joined_df = brfss_df.join(nhis_df, on=['_AGEG5YR', '_IMPRACE', 'SEX'], how='inner')
    joined_df = joined_df.select("SEX", "_IMPRACE", "_AGEG5YR", "_LLCPWT", "DIBEV1")
    
    
    #drop na

    
    
    return joined_df

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('nhis', type=str, default=None, help="brfss filename")
    arg_parser.add_argument('brfss', type=str, default=None, help="nhis filename")
    arg_parser.add_argument('-o', '--output', type=str, default=None, help="output path(optional)")

    #parse args
    args = arg_parser.parse_args()
    if not args.nhis or not args.brfss:
        arg_parser.usage = arg_parser.format_help()
        arg_parser.print_usage()
    else:
        brfss_filename = args.nhis
        nhis_filename = args.brfss

        # Start spark session
        spark = SparkSession.builder.getOrCreate()

        # load dataframes
        brfss_df = create_dataframe(brfss_filename, 'json', spark)
        nhis_df = create_dataframe(nhis_filename, 'csv', spark)
        # Perform mapping on nhis dataframe
        nhis_df_transformed = transform_nhis_data(nhis_df)
        # Join brfss and nhis df
        joined_df = join_data(brfss_df, nhis_df_transformed)
        # Calculate statistics
        # print(joined_df)
        report_summary_stats(joined_df)

        # # Save
        # if args.output:
        #     joined_df.write.csv(args.output, mode='overwrite', header=True)


        # Stop spark session 
        spark.stop()