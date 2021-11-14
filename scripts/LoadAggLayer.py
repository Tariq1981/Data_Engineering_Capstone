import configparser
import os
from Utitlity import replaceTable
from Utitlity import create_spark_session
from Utitlity import getConfig
from pyspark.sql.window import Window
import pyspark.sql.functions as fn


def createAggTables(config,spark, input_data, output_data):
    """
        Description: This function call the functions which create the aggregate tables.

        Arguments:
            spark: spark session object
            input_data: S3 Input bucket which will be used
            output_data: S3 Output bucket which will be used

        Returns:
            None
    """
    if config["GENERAL"]["DEBUG"] == "1":
        in_data_full = input_data
        out_data_full = output_data
    else:
        in_data_full = "s3a://" + input_data + "/"
        out_data_full = "s3a://" + output_data + "/"
    df_app_fact = createAppFactTabe(config,spark,in_data_full)
    df_app_fact.write.mode("overwrite").parquet(out_data_full + config["DWH_TABLES"]["APP_FACT_FT"] + "_TEMP")
    replaceTable(config,spark, output_data, config["DWH_TABLES"]["APP_FACT_FT"])

def createAppFactTabe(config,spark,input_data):
    """
        Description: This function create the APP_FACT table

        Arguments:
            spark: spark session object
            input_data: S3 Input bucket which will be used

        Returns:
            df_agg: The app aggregate dataframe
    """

    df_app = spark.read.parquet(input_data + config['DL_TABLES']['APP_TBL'])
    df_appPerm = spark.read.parquet(input_data + config['DL_TABLES']['APP_PERMISSION_TBL'])
    df_perm = spark.read.parquet(input_data + config['DL_TABLES']['PERMISSION_TBL'])

    df_app.createOrReplaceTempView("APP")
    df_appPerm.createOrReplaceTempView("APP_PERMISSION")
    df_perm.createOrReplaceTempView("PERMISSION")
    df_agg = spark.sql("""
        SELECT Category_Id,Currency_Type_Id,Developer_Id,
               COALESCE(EXTRACT(YEAR FROM Release_Dt),EXTRACT(YEAR FROM Last_Update_Dt)) Release_Year, 
               COALESCE(EXTRACT(MONTH FROM Release_Dt),EXTRACT(MONTH FROM Last_Update_Dt)) Release_Month,
               Cont_Rating_Id,Permission_Type_Id,
               COUNT(DISTINCT AP_PER.Permission_Id) Total_Num_Permissions,COUNT(DISTINCT AP.App_Id) Count_Of_Apps,
               SUM(AP.Rating)/COUNT(DISTINCT AP.App_Id) Average_Rating,SUM(Rating_Num) Total_Rating_Num,
               SUM(Maximum_Installs) Total_Installs,COUNT(DISTINCT CASE WHEN Is_Free = 'Y' THEN AP.APP_ID END) Count_Of_Free,
               COUNT(DISTINCT CASE WHEN Is_Free = 'N' THEN AP.APP_ID END) Count_Of_Paid,
               SUM(AP.Price) Total_Price,SUM(Size_In_MB) Total_Size_In_MB,
               COUNT(DISTINCT CASE WHEN Is_Ad_Supported = 'Y' THEN AP.APP_ID END) Count_Ad_Supported,
               COUNT(DISTINCT CASE WHEN Is_In_App_Purchase = 'Y' THEN AP.APP_ID END) Count_In_App_Purchase,
               COUNT(DISTINCT CASE WHEN Is_Editor_Choice = 'Y' THEN AP.APP_ID END) Count_Of_Editor_Choice
        FROM APP AP
        INNER JOIN APP_PERMISSION AP_PER
        ON AP.APP_ID = AP_PER.APP_ID
        INNER JOIN PERMISSION PER
        ON AP_PER.Permission_Id = PER.Permission_Id
        GROuP BY Category_Id,Currency_Type_Id,Developer_Id,
                 COALESCE(EXTRACT(YEAR FROM Release_Dt),EXTRACT(YEAR FROM Last_Update_Dt)),
                 COALESCE(EXTRACT(MONTH FROM Release_Dt),EXTRACT(MONTH FROM Last_Update_Dt)),
                 Cont_Rating_Id,Permission_Type_Id
    """)
    wid = Window.orderBy("Release_Year","Release_Month","Developer_Id")
    df_agg = df_agg.withColumn("Auto_App_Id",fn.row_number().over(wid))
    df_agg = df_agg.select(["Auto_App_Id","Category_Id","Currency_Type_Id","Developer_Id",
                            "Release_Year","Release_Month","Cont_Rating_Id","Permission_Type_Id",
                            "Total_Num_Permissions","Count_Of_Apps","Average_Rating","Total_Rating_Num",
                            "Total_Installs","Count_Of_Free","Count_Of_Paid","Total_Price","Total_Size_In_MB",
                            "Count_Ad_Supported","Count_In_App_Purchase","Count_Of_Editor_Choice"])

    return df_agg


def main():
    spark = create_spark_session()
    config = getConfig(spark)
    os.environ['AWS_ACCESS_KEY_ID'] = config['S3']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['S3']['AWS_SECRET_ACCESS_KEY']

    if config['GENERAL']['DEBUG'] == "1":
        input_data = "C:/Downloads/Courses/Udacity_Data_Engineering/Data_Engineering_Capstone/"
        output_data = "C:/Downloads/Courses/Udacity_Data_Engineering/Data_Engineering_Capstone/"
    else:
        input_data = config['S3']['TARGET_BUCKET']
        output_data = config['S3']['TARGET_BUCKET']

    createAggTables(config,spark,input_data,output_data)


if __name__ == "__main__":
    main()
