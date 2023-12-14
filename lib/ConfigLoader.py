from configparser import ConfigParser
from pyspark import SparkConf


def get_config(env: str) -> dict:
    config = ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}
    for key, value in config.items(env):
        conf[key] = value
    return conf


def get_spark_config(env: str) -> SparkConf:
    config = ConfigParser()
    spark_conf = SparkConf()
    config.read("conf/spark.conf")
    for key, value in config.items(env):
        spark_conf.set(key, value)
    return spark_conf


def get_data_filter(env: str, data_filter: str) -> str:
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]


def get_param_value(env: str, param: str) -> str:
    conf = get_config(env)
    return conf[param]

