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


def process_google_apps_data(spark, input_data, output_data):
    """
        Description: This function processes the main google app csv file

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.
            output_data: S3 Output bucket which will be used to save the parquet files for each table

        Returns:
            None
    """
    df = readGoogleAppFile(spark, input_data)
    # df.filter(df["Currency"] == "9126997").show(truncate=False)
    #df.show()

    # Create Developer Table
    df_dev = getDeveloperTable(spark,df,output_data,config["DL_TABLES"]["DEVELOPER_TBL"])
    df_dev.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["DEVELOPER_TBL"]+"_TEMP")
    replaceTable(spark,output_data,config["DL_TABLES"]["DEVELOPER_TBL"])

    # Create App_Category Table
    df_cat = getLookupTable(spark, df, "Category", "Category_Id", "Category_Desc", output_data,
                            config["DL_TABLES"]["APP_CATEGORY_TBL"])
    df_cat.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["APP_CATEGORY_TBL"]+"_TEMP")
    replaceTable(spark, output_data, config["DL_TABLES"]["APP_CATEGORY_TBL"])

    # Create Content_Rating Table
    df_cntRat = getLookupTable(spark, df, "Content_Rating", "Cont_Rating_Id", "Cont_Rating_Desc", output_data,
                               config["DL_TABLES"]["CONTENT_RATING_TBL"])
    df_cntRat.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["CONTENT_RATING_TBL"]+"_TEMP")
    replaceTable(spark, output_data, config["DL_TABLES"]["CONTENT_RATING_TBL"])

    # Create CURRENCY_TYPE Table
    df_curr = getLookupTable(spark, df, "Currency", "Currency_Type_Id", "Currency_Type_Desc", output_data,
                             config["DL_TABLES"]["CURRENCY_TYPE_TBL"])
    df_curr.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["CURRENCY_TYPE_TBL"]+"_TEMP")
    replaceTable(spark, output_data, config["DL_TABLES"]["CURRENCY_TYPE_TBL"])

    #Create APP Table
    df_app = getAppTable(spark,df,config["DL_TABLES"]["APP_CATEGORY_TBL"],config["DL_TABLES"]["CONTENT_RATING_TBL"],
                         config["DL_TABLES"]["CURRENCY_TYPE_TBL"],config["DL_TABLES"]["DEVELOPER_TBL"],
                         output_data,config["DL_TABLES"]["APP_TBL"])
    df_app.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["APP_TBL"]+"_TEMP")
    replaceTable(spark, output_data, config["DL_TABLES"]["APP_TBL"])



def readGoogleAppFile(spark,input_data):
    """
        Description: This function read the google app csv file. It uses noraml text read which read the whole line
        as one string. This is due to quotes and comma characters are included in the name field.

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.

        Returns:
            df: Spark DataFrame which contains the data.
    """

    pattern = '("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*),("(?:[^"]|"")*"|[^,]*)'
    colNames = ["App_Name","App_Id","Category","Rating","Rating_Count","Installs","Minimum_Installs","Maximum_Installs","Free","Price","Currency","Size","Minimum_Android","Developer_Id","Developer_Website","Developer_Email","Released","Last_Updated","Content_Rating","Privacy_Policy","Ad_Supported","In_App_Purchases","Editors_Choice","Scraped_Time"]

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
    df = df.drop("value")
    return df


def getAppTable(spark,df,catTableName,contTableName,curTableName,devTableName,output_data,tblName):
    """
        Description: This function creates the APP table.

        Arguments:
            spark: spark session object
            df: The dataframe which holds the data read from google apps csv file
            catTableName: The table name for the CATEGORY table
            contTableName: The table name for the CONTENT_RATING table
            curTableName: The table name for the CURRENCY table
            devTableName: The table name for the DEVELOPER table
            output_data: S3 Output bucket which will be used to save the parquet files for APP table
            tblName: The table name for the APP table.
        Returns:
            new_app_df: Spark DataFrame which contains the data for the APP table.
    """

    try:
        #Read Lookup tables
        df_cat = spark.read.parquet(output_data + catTableName)
        df_cont = spark.read.parquet(output_data + contTableName)
        df_curr = spark.read.parquet(output_data + curTableName)
        df_dev = spark.read.parquet(output_data + devTableName)

        new_app_df = df.join(df_cat,df["Category"] == df_cat["Category_Desc"],"left")
        new_app_df = new_app_df.withColumn("Category_Id",fn.coalesce(new_app_df["Category_Id"],fn.lit(-1)))

        new_app_df = new_app_df.join(df_cont, new_app_df["Content_Rating"] == df_cont["Cont_Rating_Desc"], "left")
        new_app_df = new_app_df.withColumn("Cont_Rating_Id", fn.coalesce(new_app_df["Cont_Rating_Id"], fn.lit(-1)))

        new_app_df = new_app_df.join(df_curr, new_app_df["Currency"] == df_curr["Currency_Type_Desc"], "left")
        new_app_df = new_app_df.withColumn("Currency_Type_Id", fn.coalesce(new_app_df["Currency_Type_Id"], fn.lit(-1)))

        new_app_df = new_app_df.withColumnRenamed("Developer_Id","Src_Developer_Name")
        new_app_df = new_app_df.join(df_dev, new_app_df["Src_Developer_Name"] == df_dev["Developer_Name"], "left")
        new_app_df = new_app_df.withColumn("Developer_Id", fn.coalesce(new_app_df["Developer_Id"], fn.lit(-1)))

        ##### Apply transformation to columns like Size and other to match table schema.
        ##### read parquet and get new apps to insert and apps to be updated and smae as it is all union all and overwrite
        new_app_df = new_app_df.withColumnRenamed("Rating_Count","Rating_Num")
        new_app_df = new_app_df.withColumn("Minimum_Installs",fn.expr("CAST(Minimum_Installs AS INTEGER)"))
        new_app_df = new_app_df.withColumn("Maximum_Installs", fn.expr("CAST(Maximum_Installs AS INTEGER)"))
        new_app_df = new_app_df.withColumn("Is_Free",
                                           fn.when(new_app_df["Free"] == "True",fn.lit("Y")).otherwise(
                                               fn.lit("N"))).drop("Free")
        new_app_df = new_app_df.withColumn("Size_In_MB",fn.expr("CAST(substring(Size,1,length(Size)-1) AS FLOAT)")).drop("Size")
        new_app_df = new_app_df.withColumnRenamed("Minimum_Android","Supp_OS_Version")
        new_app_df = new_app_df.withColumnRenamed("Released","Release_Dt")
        new_app_df = new_app_df.withColumnRenamed("Last_Updated","Last_Update_Dt")
        new_app_df = new_app_df.withColumn("Is_Ad_Supported",
                                           fn.when(new_app_df["Ad_Supported"] == "True", fn.lit("Y")).otherwise(
                                               fn.lit("N"))).drop("Ad_Supported")
        new_app_df = new_app_df.withColumn("Is_In_App_Purchase",
                                           fn.when(new_app_df["In_App_Purchases"] == "True", fn.lit("Y")).otherwise(
                                               fn.lit("N"))).drop("In_App_Purchases")
        new_app_df = new_app_df.withColumn("Is_Editor_Choice",
                                           fn.when(new_app_df["Editors_Choice"] == "True", fn.lit("Y")).otherwise(
                                               fn.lit("N"))).drop("Editors_Choice")
        new_app_df = new_app_df.withColumnRenamed("Scraped_Time","Scrapped_Dttm")
        new_app_df = new_app_df.select(
            ["App_Id", "App_Name", "Category_Id", "Rating", "Rating_Num", "Minimum_Installs",
             "Maximum_Installs", "Is_Free", "Price", "Currency_Type_Id", "Size_In_MB", "Supp_OS_Version",
             "Developer_Id", "Release_Dt", "Last_Update_Dt", "Cont_Rating_Id", "Privacy_Policy",
             "Is_Ad_Supported", "Is_In_App_Purchase", "Is_Editor_Choice", "Scrapped_Dttm"])

        app_df = spark.read.parquet(output_data + tblName)

        new_app_df.createOrReplaceTempView("APP_DF_SRC")
        app_df.createOrReplaceTempView("APP_DF_TGT")
        new_app_df_Ins_Upd = spark.sql("""
        SELECT SRC.*
        FROM APP_DF_SRC SRC
        LEFT OUTER JOIN APP_DF_TGT TGT
        ON SRC.App_ID = TGT.App_ID
        WHERE (TGT.App_ID IS NULL) OR (TGT.App_ID IS NOT NULL AND 
               (TGT.App_Name <> SRC.App_Name OR TGT.Rating <> SRC.Rating OR TGT.Rating_Num <> SRC.Rating_Num OR 
                TGT.Minimum_Installs <> SRC.Minimum_Installs OR TGT.Maximum_Installs <> SRC.Maximum_Installs OR 
                TGT.Is_Free <> SRC.Is_Free OR TGT.Price <> SRC.Price OR TGT.Currency_Type_Id <> SRC.Currency_Type_Id OR
                COALESCE(TGT.Size_In_MB,-1) <> COALESCE(SRC.Size_In_MB,-1) OR TGT.Supp_OS_Version <> SRC.Supp_OS_Version OR 
                TGT.Developer_Id <> SRC.Developer_Id OR TGT.Last_Update_Dt <> SRC.Last_Update_Dt OR 
                TGT.Cont_Rating_Id <> SRC.Cont_Rating_Id OR TGT.Privacy_Policy <> SRC.Privacy_Policy OR 
                TGT.Is_Ad_Supported <> SRC.Is_Ad_Supported OR TGT.Is_In_App_Purchase <> SRC.Is_In_App_Purchase OR
                TGT.Is_Editor_Choice <> SRC.Is_Editor_Choice
                ))
        """)
        new_app_df_Ins_Upd.createOrReplaceTempView("APP_NEW_UPDATE")

        new_app_df_rest = spark.sql("""
        SELECT TGT.*
        FROM APP_DF_TGT TGT
        LEFT OUTER JOIN APP_NEW_UPDATE RES 
        ON TGT.App_ID = RES.App_ID
        WHERE RES.App_ID IS NULL
        """)

        new_app_df = new_app_df_rest.unionAll(new_app_df_Ins_Upd)


    except Exception as e:
        print(e)

    return new_app_df


def getDeveloperTable(spark, df,output_data,tblName):
    """
        Description: This function creates the DEVELOPER table.

        Arguments:
            spark: spark session object
            df: The dataframe which holds the data read from google apps csv file
            output_data: S3 Output bucket which will be used to save the parquet files for DEVELOPER table
            tblName: The table name for the DEVELOPER table.
        Returns:
            new_app_df: Spark DataFrame which contains the data for the DEVELOPER table.
    """


    try:
        idCol = Window.orderBy("Src_Developer_Name")
        look_df = spark.read.parquet(output_data + tblName)
        new_lookup_df = df.select(["Developer_Id","Developer_Website","Developer_Email"]).distinct() \
            .withColumnRenamed("Developer_Id","Src_Developer_Name") \
            .withColumnRenamed("Developer_Website", "Src_Developer_Website") \
            .withColumnRenamed("Developer_Email", "Src_Developer_Email") \
            .filter(df["Developer_Id"].isNotNull() & (df["Developer_Id"] != ""))

        look_max = look_df.agg(fn.max("Developer_Id").alias("max_Id"))
        resultAll = new_lookup_df.join(look_df, look_df["Developer_Name"] == new_lookup_df["Src_Developer_Name"], "left") \
            .crossJoin(look_max)

        resultIns = resultAll.filter(resultAll["Developer_Id"].isNull()).withColumn("Id", fn.row_number().over(idCol)) \
            .select(["Id","Src_Developer_Name","Src_Developer_Website","Src_Developer_Email","max_Id"])

        resultIns = resultIns.withColumn("Developer_Id", resultIns.Id + resultIns.max_Id) \
            .withColumnRenamed("Src_Developer_Name", "Developer_Name") \
            .withColumnRenamed("Src_Developer_Website", "Developer_Website") \
            .withColumnRenamed("Src_Developer_Email", "Developer_Email") \
            .select(["Developer_Id","Developer_Name","Developer_Website","Developer_Email"])

        resultUpd = resultAll.filter(resultAll["Developer_Id"].isNotNull() &
                                     ((resultAll["Src_Developer_Website"] != resultAll["Developer_Website"]) |
                                     (resultAll["Src_Developer_Email"] != resultAll["Developer_Email"]))) \
            .select(["Developer_Id","Developer_Name","Src_Developer_Website","Src_Developer_Email"]) \
            .withColumnRenamed("Developer_Id","Upd_Developer_Id") \
            .withColumnRenamed("Developer_Name", "Upd_Developer_Name")
        restRows = look_df.join(resultUpd,resultUpd["Upd_Developer_Id"] == look_df["Developer_Id"],"left") \
            .filter("Upd_Developer_Id IS NULL") \
            .select(["Developer_Id","Developer_Name","Developer_Website","Developer_Email"])

        resultUpd.withColumnRenamed("Upd_Developer_Id","Developer_Id") \
            .withColumnRenamed("Upd_Developer_Name", "Developer_Name") \
            .withColumnRenamed("Src_Developer_Website","Developer_Website") \
            .withColumnRenamed("Src_Developer_Email","Developer_Email")

        new_lookup_df = look_df.unionAll(resultIns).unionAll(resultUpd)
    except Exception as e:
        print(e)
        idCol = Window.orderBy("Developer_Name")
        new_lookup_df = df.select(["Developer_Id", "Developer_Website", "Developer_Email"]).distinct() \
            .withColumnRenamed("Developer_Id", "Developer_Name")

        new_lookup_df = new_lookup_df.filter(new_lookup_df["Developer_Name"].isNotNull()) \
            .withColumn("Developer_Id", fn.row_number().over(idCol)) \
            .select(["Developer_Id","Developer_Name","Developer_Website","Developer_Email"])


    return new_lookup_df

def getLookupTable(spark, df, srcColumn, tgtIdColumn, tgtColumn, output_data, tblName):
    """
        Description: This function is used to create lookup tables like CATEGORY,...etc.

        Arguments:
            spark: spark session object
            df: The dataframe which holds the data read from google apps csv file
            srcColumn: The source column which will be used in generating Id for each distinct value in the lookup
            tgtIdColumn: The name of the column which will hold the Id column in the target table
            tgtColumn: The name of column which will hold the valye from the srcColumn in the target table.
            output_data: S3 Output bucket which will be used to save the parquet files for DEVELOPER table
            tblName: The table name for the lookup table.
        Returns:
            new_lookup_df: Spark DataFrame which contains the data for the lookup table.
    """

    idCol = Window.orderBy(srcColumn)

    try:
        look_df = spark.read.parquet(output_data + tblName)
        new_lookup_df = df.select([srcColumn]).distinct().filter(df[srcColumn].isNotNull() & (df[srcColumn] != ""))
        look_max = look_df.agg(fn.max(tgtIdColumn).alias("max_Id"))
        result = new_lookup_df.join(look_df, look_df[tgtColumn] == new_lookup_df[srcColumn], "left") \
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
        Description: This funciton processes the google app permission json file

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.
            output:data: S3 Output bucket which will be used to save the output folders

        Returns:
            None
    """

    # Read json file create lookup using thte generic function then use_df_app in creating the relation app and permission
    df = readGoogleJsonFile(spark,input_data)
    df.printSchema()
    df.show(truncate=False)

    # Create Permisison Type lookup
    df_permType = getLookupTable(spark, df, "type", "Permission_Type_Id", "Permission_Type_Desc", output_data,
                            config["DL_TABLES"]["PERMISSION_TYPE_TBL"])
    df_permType.cache()
    #df_permType.show(n=10)
    df_permType.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["PERMISSION_TYPE_TBL"]+"_TEMP")
    replaceTable(spark,output_data,config["DL_TABLES"]["PERMISSION_TYPE_TBL"])

    # Create Permission Table lookup
    df_perm = getPermisisonTable(spark,df,config["DL_TABLES"]["PERMISSION_TYPE_TBL"],
                                 output_data,config["DL_TABLES"]["PERMISSION_TBL"])
    df_perm.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["PERMISSION_TBL"]+"_TEMP")
    replaceTable(spark, output_data, config["DL_TABLES"]["PERMISSION_TBL"])

    #Create APP_PERMISSION Table
    df_appPerm = getAppPermissionTable(spark,df,config["DL_TABLES"]["PERMISSION_TBL"],output_data,
                                       config["DL_TABLES"]["APP_PERMISSION_TBL"])
    df_appPerm.write.mode("overwrite").parquet(output_data + config["DL_TABLES"]["APP_PERMISSION_TBL"]+"_TEMP")
    replaceTable(spark, output_data, config["DL_TABLES"]["APP_PERMISSION_TBL"])


def getAppPermissionTable(spark,df,permTableName,output_data,tblName):
    """
        Description: This function is used to create APP_PERMISSION table.

        Arguments:
            spark: spark session object
            df: The dataframe which holds the data read from google apps permissions json file
            permTableName: Table name for PERMISSION table.
            output_data: S3 Output bucket which will be used to save the parquet files for APP_PERMISSION table
            tblName: The table name for the APP_PERMISSION table.
        Returns:
            df_new_appPerm: Spark DataFrame which contains the data for the APP_PERMISSION table.
    """
    df_perm = spark.read.parquet(output_data + permTableName)
    df.createOrReplaceTempView("SrcAppPermTable")
    df_perm.createOrReplaceTempView("PermTable")
    df_new_appPerm = spark.sql("""
            SELECT SRC.appId,COALESCE(PT.Permission_Id,-1) Permission_Id
            FROM SrcAppPermTable SRC
            LEFT OUTER JOIN PermTable PT
            ON SRC.permission = PT.Permisison_Desc
    """)
    try:
        df_appPerm = spark.read.parquet(output_data + tblName)
        df_new_appPerm = df_appPerm.union(df_new_appPerm)
    except Exception as e:
        print(e)

    return df_new_appPerm

def getPermisisonTable(spark,df,permTypeTableName,output_data,tblName):
    """
        Description: This function is used to create PERMISSION table.

        Arguments:
            spark: spark session object
            df: The dataframe which holds the data read from google apps permissions json file
            permTypeTableName: Table name for PERMISSION_TYPE table.
            output_data: S3 Output bucket which will be used to save the parquet files for APP_PERMISSION table
            tblName: The table name for the PERMISSION table.
        Returns:
            df_perm_new: Spark DataFrame which contains the data for the PERMISSION table.
    """

    df.createOrReplaceTempView("DF_SRC")
    df_permType = spark.read.parquet(output_data + permTypeTableName)
    df_permType.createOrReplaceTempView("DF_PERMTYPE")
    df_perm_new = spark.sql("""
            SELECT /*+ BROADCASTJOIN(DF_PERMTYPE) */ ROW_NUMBER() OVER(ORDER BY permission) Permission_Id,permission Permisison_Desc,
                    COALESCE(Permission_Type_Id,-1) Permission_Type_Id
            FROM 
            (
                SELECT permission,type 
                FROM DF_SRC
                GROUP BY permission,type
            ) X
            LEFT OUTER JOIN DF_PERMTYPE PT
            ON PT.Permission_Type_Desc = X.type
            """)
    try:
        df_perm = spark.read.parquet(output_data + tblName)
        df_perm_new.createOrReplaceTempView("DF_NEW_PERM")
        df_perm.createOrReplaceTempView("DF_TGT_PERM")
        df_perm_new = spark.sql("""
                SELECT SRC.Permission_Id + max_Id Permission_Id, SRC.Permisison_Desc, SRC.Permission_Type_Id
                FROM DF_NEW_PERM SRC
                CROSS JOIN 
                (
                    SELECT max(Permission_Id) max_ID
                    FROM DF_TGT_PERM
                )
        """)
    except Exception as e:
        print(e)


    return df_perm_new


def readGoogleJsonFile(spark,input_data):
    """
        Description: This function read the google app permission json file.

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.

        Returns:
            df: Spark DataFrame which contains the data.
    """

    schema = StructType([StructField("appId", StringType()),
                         StructField("appName", StringType()),
                         StructField("allPermissions", ArrayType(
                             StructType([
                                 StructField("permission", StringType()),
                                 StructField("type", StringType())
                             ])
                         ))])
    permission_file = input_data + config['S3']['GOOGLE_PERM_DATA']
    df = spark.read.schema(schema).option("multiline", "true").json(permission_file)
    df = df.withColumn("permissionSt",fn.explode("allPermissions"))
    df = df.withColumn("permission",df["permissionSt"]["permission"]) \
        .withColumn("type",df["permissionSt"]["type"]).drop("allPermissions","permissionSt")
    return df

def replaceTable(spark,output_data,tblName):
    """
        Description: This function is used to replace the old data with new one.

        Arguments:
            spark: spark session object
            output_data: S3 Output bucket which will be used
            tblName: The table name tobe replaced.

        Returns:
            None
    """

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    # list files in the directory
    list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(output_data))
    for file in list_status:
        fileName = file.getPath().getName()
        print(fileName)
        if fileName == tblName:
            print("Got it !!!!!!")
            fs.delete(spark._jvm.org.apache.hadoop.fs.Path(output_data + fileName), True)
            break
    fs.rename(spark._jvm.org.apache.hadoop.fs.Path(output_data + tblName + "_TEMP"),
              spark._jvm.org.apache.hadoop.fs.Path(output_data + tblName))

def main():
    spark = create_spark_session()
    if config['GENERAL']['DEBUG'] == "1":
        input_data = "C:/Downloads/Courses/Udacity_Data_Engineering/Data_Engineering_Capstone/"
        output_data = "C:/Downloads/Courses/Udacity_Data_Engineering/Data_Engineering_Capstone/"
    else:
        input_data = config['S3']['SOURCE_BUCKET']
        output_data = config['S3']['TARGET_BUCKET']

    process_google_apps_data(spark, input_data, output_data)
    process_google_perm_data(spark,input_data, output_data)


if __name__ == "__main__":
    main()
