import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType , StructType,StructField
from pyspark.sql.window import Window
import pyspark.sql.functions as fn

config = configparser.ConfigParser()
config.read('../config/etl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['S3']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['S3']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: This function creates the spark session which will be used in teh rest of the script.

        Arguments:
            None
        Returns:
            None
    """

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.executor.heartbeatInterval","3600s") \
        .config("spark.files.fetchTimeout", "3600s") \
        .config("spark.network.timeout", "4600s") \
        .config("spark.storage.blockManagerSlaveTimeoutMs","3600s") \
        .getOrCreate()


    return spark


def createAggTables(spark, input_data, output_data):
    """
        Description: This function processes the main google app csv file

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.
            output_data: S3 Output bucket which will be used to save the parquet files for each table

        Returns:
            None
    """
    df_app_fact = createAppFactTabe(spark,input_data)

    df_app_fact.show(truncate=False)

def createAppFactTabe(spark,input_data):
    """
        Description: This function processes the main google app csv file

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.
            output_data: S3 Output bucket which will be used to save the parquet files for each table

        Returns:
            None
    """
    spark = create_spark_session()
    df_app = spark.read.parquet(input_data + config['DL_TABLES']['APP_TBL'])
    df_appPerm = spark.read.parquet(input_data + config['DL_TABLES']['APP_PERMISSION_TBL'])
    df_perm = spark.read.parquet(input_data + config['DL_TABLES']['PERMISSION_TBL'])

    df_app.createOrReplaceTempView("APP")
    df_appPerm.createOrReplaceTempView("APP_PERMISSION")
    df_perm.createOrReplaceTempView("PERMISSION")

    df_agg = spark.sql("""
        SELECT Category_Id,Currency_Type_Id,Developer_Id,EXTRACT(YEAR FROM Release_Dt) YEAR, 
               EXTRACT(MONTH FROM Release_Dt) MONTH,Cont_Rating_Id,Permission_Type_Id
        FROM APP AP
        INNER JOIN APP_PERMISSION AP_PER
        ON AP.APP_ID = AP_PER.APP_ID
        INNER JOIN PERMISSION PER
        ON AP_PER.Permission_Id = PER.Permission_Id
    """)

    return df_agg


def main():
    spark = create_spark_session()
    if config['GENERAL']['DEBUG'] == "1":
        input_data = "C:/Downloads/Courses/Udacity_Data_Engineering/Data_Engineering_Capstone/"
        output_data = "C:/Downloads/Courses/Udacity_Data_Engineering/Data_Engineering_Capstone/"
    else:
        input_data = config['S3']['TARGET_BUCKET']
        output_data = config['S3']['TARGET_BUCKET']

    createAggTables(spark,input_data,output_data)


if __name__ == "__main__":
    main()
