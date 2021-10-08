import configparser
import os
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('config/etl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_google_apps_data(spark, input_data, output_data):
    """
        Description: This funciton processes the songs files on S3 and populate SONGS and ARTISTS folders

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.
            output:data: S3 Output bucket which will be used to save the output folders ( SONGS, ARTISTS)

        Returns:
            None
    """
    pass


def process_google_perm_data(spark, input_data, output_data):
    """
        Description: This funciton processes the songs files on S3 and populate SONGS and ARTISTS folders

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.
            output:data: S3 Output bucket which will be used to save the output folders ( SONGS, ARTISTS)

        Returns:
            None
    """
    pass

def main():
    spark = create_spark_session()
    input_data = config['AWS']['SOURCE_BUCKET']
    output_data = config['AWS']['TARGET_BUCKET']


    process_google_apps_data(spark, input_data, output_data)
    process_google_perm_data(spark, input_data, output_data)



if __name__ == "__main__":
    main()