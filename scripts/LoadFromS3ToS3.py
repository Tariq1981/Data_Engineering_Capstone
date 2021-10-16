import configparser
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType
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
        .master("local[1]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.executor.heartbeatInterval","3600s") \
        .config("spark.files.fetchTimeout", "3600s") \
        .config("spark.network.timeout", "4600s") \
        .config("spark.storage.blockManagerSlaveTimeoutMs","3600s") \
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
    # schema = "App_Name STRING,App_Id STRING,Category STRING,Rating DOUBLE,Rating_Count INT,Installs STRING," \
    #          "Minimum_Installs INT,Maximum_Installs INT,Free STRING,Price DOUBLE,Currency STRING,Size STRING," \
    #          "Minimum_Android STRING,Developer_Id STRING,Developer_Website STRING,Developer_Email STRING," \
    #          "Released DATE,Last_Updated DATE,Content_Rating STRING,Privacy_Policy STRING,Ad_Supported STRING," \
    #          "In_App_Purchases STRING,Editors_Choice STRING,Scraped_Time TIMESTAMP"

    df = readGoogleAppFile(spark, input_data)
    # df.filter(df["Currency"] == "9126997").show(truncate=False)
    #df.show()

    # Create App_Category Table
    df_cat = getLookupTable(spark, df, "Category", "Category_Id", "Category_Desc", output_data,
                            config["DL_TABLES"]["APP_CATEGORY_TBL"])
    df_cat.cache()
    df_cat.show(n=1)
    #print(df_cat.count())
    #df_cat.createOrReplaceTempView(config["DL_TABLES"]["APP_CATEGORY_TBL"])
    #spark.sql("REFRESH TABLE "+config["DL_TABLES"]["APP_CATEGORY_TBL"])
    df_cat.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["APP_CATEGORY_TBL"])

    # Create Content_Rating Table
    df_cntRat = getLookupTable(spark, df, "Content_Rating", "Cont_Rating_Id", "Cont_Rating_Desc", output_data,
                               config["DL_TABLES"]["CONTENT_RATING_TBL"])
    df_cntRat.cache()
    df_cntRat.show(n=1)
    #df_cntRat.createOrReplaceTempView(config["DL_TABLES"]["CONTENT_RATING_TBL"])
    #spark.sql("REFRESH TABLE "+config["DL_TABLES"]["CONTENT_RATING_TBL"])
    df_cntRat.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["CONTENT_RATING_TBL"])

    # Create CURRENCY_TYPE Table
    df_curr = getLookupTable(spark, df, "Currency", "Currency_Type_Id", "Currency_Type_Desc", output_data,
                             config["DL_TABLES"]["CURRENCY_TYPE_TBL"])
    df_curr.cache()
    df_curr.show(n=100)
    #df_curr.createOrReplaceTempView(config["DL_TABLES"]["CURRENCY_TYPE_TBL"])
    #spark.sql("REFRESH TABLE " + config["DL_TABLES"]["CURRENCY_TYPE_TBL"])
    df_curr.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["CURRENCY_TYPE_TBL"])


def readGoogleAppFile(spark,input_data):
    """
    Read cateogry and merge with what in te df after getting max ID and increase

    :param spark:
    :param df:
    :param output_data:
    :return:
    """
    pattern = '("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*)'
    colNames = ["App_Name","App_Id","Category","Rating DOUBLE","Rating_Count","Installs","Minimum_Installs","Maximum_Installs","Free","Price DOUBLE","Currency","Size","Minimum_Android","Developer_Id","Developer_Website","Developer_Email","Released","Last_Updated","Content_Rating","Privacy_Policy","Ad_Supported","In_App_Purchases","Editors_Choice","Scraped_Time"]

    def trimQutes(strVal):
        return strVal.strip('"')


    googleapp_data = input_data + config['S3']['GOOGLE_APPS_DATA']
    df = spark.read.text(googleapp_data).filter("value not like 'App Name,App Id%'")
    udfStrip = fn.udf(trimQutes,StringType())
    for i in range(0,len(colNames)):
        df = df.withColumn(colNames[i],fn.regexp_extract(df["value"],pattern,i+1))
        df = df.withColumn(colNames[i],udfStrip(df[colNames[i]]))

    df = df.withColumn("Released",fn.to_date(df["Released"], "MMM d, yyyy"))
    df = df.withColumn("Last_Updated",fn.to_date(df["Last_Updated"], "MMM d, yyyy"))
    df = df.withColumn("Scraped_Time",fn.to_timestamp("Scraped_Time", "yyyy-MM-dd HH:mm:ss"))
    df.drop("value")
    return df



def getLookupTable(spark, df, srcColumn, tgtIdColumn, tgtColumn, output_data, tblName):
    """
    Read cateogry and merge with what in te df after getting max ID and increase

    :param spark:
    :param df:
    :param output_data:
    :return:
    """
    idCol = Window.orderBy(srcColumn)

    try:
        look_df = spark.read.parquet(output_data + tblName)
        new_lookup_df = df.select([srcColumn]).distinct().filter(df[srcColumn].isNotNull() & (df[srcColumn] != ""))
        look_max = look_df.agg(fn.max(tgtIdColumn).alias("max_Id"))
        result = look_df.join(new_lookup_df, look_df[tgtColumn] == new_lookup_df[srcColumn], "left") \
            .crossJoin(look_max)
        result = result.filter(result[tgtColumn].isNull()).withColumn("Id", fn.row_number().over(idCol)) \
            .select(["Id", srcColumn, "max_Id"])
        result = result.withColumn(tgtIdColumn, result.Id + result.max_Id) \
            .withColumnRenamed(srcColumn, tgtColumn).select([tgtIdColumn, tgtColumn])

        new_lookup_df = look_df.unionAll(result)
    except Exception as e:
        print(e)
        new_lookup_df = df.select([srcColumn]).distinct().filter(df[srcColumn].isNotNull()) \
            .withColumn(tgtIdColumn, fn.row_number().over(idCol)) \
            .withColumnRenamed(srcColumn, tgtColumn)

    return new_lookup_df


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
    # process_google_perm_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
