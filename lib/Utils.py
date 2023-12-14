from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_config


def get_spark_session(env: str) -> SparkSession:
    if env == "LOCAL":
        return SparkSession.builder\
            .config(conf=get_spark_config(env))\
            .config("spark.sql.autoBroadcastJoinThreshold", -1)\
            .config("spark.sql.adaptive.enabled", "false")\
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties')\
            .master("local[2]")\
            .enableHiveSupport()\
            .getOrCreate()
    else:
        return SparkSession.builder\
            .config(conf=get_spark_config(env))\
            .enableHiveSupport()\
            .getOrCreate()


