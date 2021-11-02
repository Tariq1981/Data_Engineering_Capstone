from pyspark.sql import SparkSession


def create_spark_session():
    """
        Description: This function creates the spark session which will be used in teh rest of the script.

        Arguments:
            None
        Returns:
            spark: SparkSession object
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
        #print(fileName)
        if fileName == tblName:
            #print("Got it !!!!!!")
            fs.delete(spark._jvm.org.apache.hadoop.fs.Path(output_data + fileName), True)
            break
    fs.rename(spark._jvm.org.apache.hadoop.fs.Path(output_data + tblName + "_TEMP"),
              spark._jvm.org.apache.hadoop.fs.Path(output_data + tblName))