from pyspark.sql import SparkSession
import configparser
import boto3

config = configparser.ConfigParser()
config.read('../config/etl.cfg')


def create_spark_session():
    """
        Description: This function creates the spark session which will be used in teh rest of the script.

        Arguments:
            None
        Returns:
            spark: SparkSession object
    """
    # .master("spark://192.168.56.1:7077") \
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.executor.heartbeatInterval","3600s") \
        .config("spark.files.fetchTimeout", "3600s") \
        .config("spark.network.timeout", "4600s") \
        .config("spark.storage.blockManagerSlaveTimeoutMs","3600s") \
        .config("spark.executor.heartbeatInterval","3600s") \
        .config("fs.s3a.access.key",config["S3"]["AWS_ACCESS_KEY_ID"]) \
        .config("fs.s3a.secret.key", config["S3"]["AWS_SECRET_ACCESS_KEY"]) \
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
    if config['GENERAL']['DEBUG'] == "1":
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
    else:
        s3 = boto3.client('s3', aws_access_key_id=config['S3']['AWS_ACCESS_KEY_ID'], \
                          aws_secret_access_key=config['S3']['AWS_SECRET_ACCESS_KEY'], region_name=config["S3"]["AWS_REGION"])
        response = s3.list_objects(Bucket=output_data, Prefix=tblName + "/")
        if "Contents" in response:
            for key in response["Contents"]:
                response = s3.delete_object(Bucket=output_data, Key=key["Key"])
        ################# Rename
        response = s3.list_objects(Bucket=output_data, Prefix=tblName + "_TEMP/")
        for key in response["Contents"]:
            tgt_key = key["Key"].replace(tblName + "_TEMP/", tblName + "/")
            copy_source = {'Bucket': output_data, 'Key': key["Key"]}
            s3.copy_object(CopySource=copy_source, Bucket=output_data, Key=tgt_key)
            s3.delete_object(Bucket=output_data, Key=key["Key"])
