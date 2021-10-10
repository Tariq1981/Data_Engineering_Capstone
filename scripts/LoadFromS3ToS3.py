import configparser
import os
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('../config/etl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['S3']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['S3']['AWS_SECRET_ACCESS_KEY']
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
    schema = "App_Name STRING,App_Id STRING,Category STRING,Rating DOUBLE,Rating_Count INT,Installs STRING," \
             "Minimum_Installs INT,Maximum_Installs INT,Free STRING,Price DOUBLE,Currency STRING,Size STRING," \
             "Minimum_Android STRING,Developer_Id STRING,Developer_Website STRING,Developer_Email STRING," \
             "Released DATE,Last_Updated DATE,Content_Rating STRING,Privacy_Policy STRING,Ad_Supported STRING," \
             "In_App_Purchases STRING,Editors_Choice STRING,Scraped_Time TIMESTAMP"

    googleapp_data=input_data+config['S3']['GOOGLE_APPS_DATA']
    df = spark.read.schema(schema).option("header","true")\
        .option("dateFormat","MMM d, yyyy")\
        .option("timestampFormat", "yyyy-MM-dd hh:mm:ss").csv(googleapp_data)

    df_cat=getCategoryTable(spark,df,output_data)
    df.show()


def getCategoryTable(spark,df,output_data):
    """
    Read cateogry and merge with what in te df after getting max ID and increase

    :param spark:
    :param df:
    :param output_data:
    :return:
    """
    cat_df = spark.read.parquet(output_data+config["DL_TABLES"]["APP_CATEGORY_TBL"])
    return cat_df


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
    if config['GENERAL']['DEBUG'] == "1":
        input_data = "C:/Downloads/Courses/Udacity_Data_Engineering/Data_Engineering_Capstone/"
        output_data = "C:/Downloads/Courses/Udacity_Data_Engineering/Data_Engineering_Capstone/"
    else:
        input_data = config['S3']['SOURCE_BUCKET']
        output_data = config['S3']['TARGET_BUCKET']


    process_google_apps_data(spark, input_data, output_data)
    #process_google_perm_data(spark, input_data, output_data)



if __name__ == "__main__":
    main()