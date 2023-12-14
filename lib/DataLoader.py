from pyspark.sql import SparkSession, DataFrame

from lib.ConfigLoader import get_data_filter, get_param_value

schemas = {
    "accounts": """load_date date,active_ind int,account_id string,
                source_sys string,account_start_date timestamp,
                legal_title_1 string,legal_title_2 string,
                tax_id_type string,tax_id string,branch_code string,country string""",
    "parties": """load_date date,account_id string,party_id string,
                relation_type string,relation_start_date timestamp""",
    "party_address": """load_date date,party_id string,address_line_1 string,
                address_line_2 string,city string,postal_code string,
                country_of_address string,address_start_date date""",
}


def get_schema(schema_name: str) -> str:
    return schemas[schema_name]


def read_account():
    pass


def read_party():
    pass


def read_party_address():
    pass


def read_data(spark: SparkSession, env: str, enable_hive: bool, hive_db: str, hive_table: str) -> DataFrame:

    runtime_filter = get_data_filter(env, f"{hive_table}.filter")

    if enable_hive:
        table_name = get_param_value(env, f"{hive_table}.hive.table")
        return spark.sql(f"select * from {hive_db}.{table_name} where {runtime_filter}")
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_schema(hive_table)) \
            .option("path", f"test_data/{hive_table}/") \
            .load() \
            .where(runtime_filter)
