from datetime import date, datetime
from typing import Any

from chispa import assert_df_equality
import pytest
from pyspark import Row
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType

from lib.ConfigLoader import get_config
from lib.DataLoader import read_data, get_schema
from lib.Transformations import get_contract, get_relations, join_party_address, join_contract_party, apply_header, \
    get_address
from lib.Utils import get_spark_session


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")


@pytest.fixture(scope='session')
def expected_party_rows(spark: SparkSession) -> list[Row]:
    return [Row(load_date=date(2022, 8, 2), account_id='6982391060',
                party_id='9823462810', relation_type='F-N', relation_start_date=datetime(2019, 7, 29, 6, 21, 32)),
            Row(load_date=date(2022, 8, 2), account_id='6982391061', party_id='9823462811', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 31, 5, 27, 22)),
            Row(load_date=date(2022, 8, 2), account_id='6982391062', party_id='9823462812', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 25, 15, 50, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391063', party_id='9823462813', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 11, 7, 23, 28)),
            Row(load_date=date(2022, 8, 2), account_id='6982391064', party_id='9823462814', relation_type='F-N',
                relation_start_date=datetime(2019, 6, 6, 14, 18, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391065', party_id='9823462815', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 4, 5, 12, 37)),
            Row(load_date=date(2022, 8, 2), account_id='6982391066', party_id='9823462816', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 15, 10, 39, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462817', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 16, 9, 53, 4)),
            Row(load_date=date(2022, 8, 2), account_id='6982391068', party_id='9823462818', relation_type='F-N',
                relation_start_date=datetime(2017, 11, 27, 1, 20, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462820', relation_type='F-S',
                relation_start_date=datetime(2017, 11, 20, 14, 18, 5)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462821', relation_type='F-S',
                relation_start_date=datetime(2018, 7, 19, 18, 56, 57))]


@pytest.fixture(scope='session')
def parties_list():
    return [
        (date(2022, 8, 2), '6982391060', '9823462810', 'F-N', datetime.fromisoformat('2019-07-29 06:21:32.000+02:00')),
        (date(2022, 8, 2), '6982391061', '9823462811', 'F-N', datetime.fromisoformat('2018-08-31 05:27:22.000+02:00')),
        (date(2022, 8, 2), '6982391062', '9823462812', 'F-N', datetime.fromisoformat('2018-08-25 15:50:29.000+02:00')),
        (date(2022, 8, 2), '6982391063', '9823462813', 'F-N', datetime.fromisoformat('2018-05-11 07:23:28.000+02:00')),
        (date(2022, 8, 2), '6982391064', '9823462814', 'F-N', datetime.fromisoformat('2019-06-06 14:18:12.000+02:00')),
        (date(2022, 8, 2), '6982391065', '9823462815', 'F-N', datetime.fromisoformat('2019-05-04 05:12:37.000+02:00')),
        (date(2022, 8, 2), '6982391066', '9823462816', 'F-N', datetime.fromisoformat('2019-05-15 10:39:29.000+02:00')),
        (date(2022, 8, 2), '6982391067', '9823462817', 'F-N', datetime.fromisoformat('2018-05-16 09:53:04.000+02:00')),
        (date(2022, 8, 2), '6982391068', '9823462818', 'F-N', datetime.fromisoformat('2017-11-27 01:20:12.000+01:00')),
        (date(2022, 8, 2), '6982391067', '9823462820', 'F-S', datetime.fromisoformat('2017-11-20 14:18:05.000+01:00')),
        (date(2022, 8, 2), '6982391067', '9823462821', 'F-S', datetime.fromisoformat('2018-07-19 18:56:57.000+02:00'))]


@pytest.fixture(scope='session')
def expected_final_df(spark):
    schema = StructType(
        [StructField('keys',
                     ArrayType(StructType([StructField('keyField', StringType()),
                                           StructField('keyValue', StringType())]))),
         StructField('payload',
                     StructType([
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', ArrayType(
                                                     StructType([StructField('contractTitleLineType', StringType()),
                                                                 StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('partyRelations',
                                     ArrayType(StructType([
                                         StructField('partyIdentifier',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationshipType',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationStartDateTime',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', TimestampType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyAddress',
                                                     StructType([StructField('operation', StringType()),
                                                                 StructField(
                                                                     'newValue',
                                                                     StructType(
                                                                         [StructField('addressLine1', StringType()),
                                                                          StructField('addressLine2', StringType()),
                                                                          StructField('addressCity', StringType()),
                                                                          StructField('addressPostalCode',
                                                                                      StringType()),
                                                                          StructField('addressCountry', StringType()),
                                                                          StructField('addressStartDate', DateType())
                                                                          ])),
                                                                 StructField('oldValue', NullType())]))])))]))])
    df = spark.read.format("json").schema(schema).load("test_data/results/final_df.json")
    # df.printSchema()
    return df.select("keys", "payload")


@pytest.fixture(scope='session')
def expected_contract_df(spark: SparkSession) -> DataFrame:
    schema = StructType([StructField('account_id', StringType()),
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             ArrayType(StructType(
                                                                 [StructField('contractTitleLineType', StringType()),
                                                                  StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())]))])

    df = spark.read\
        .format("json")\
        .schema(schema)\
        .option("path", "test_data/results/contract_df.json")\
        .load()
        # .sort("account_id")
    # df.show()
    return df


def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.5.0"


def test_get_config():
    conf_local = get_config("LOCAL")
    conf_qa = get_config("QA")
    assert conf_local["kafka.topic"] == "sbdl_kafka_cloud"
    assert len(conf_local) == 13
    assert conf_qa["hive.database"] == "sbdl_db_qa"
    assert conf_qa["party_address.hive.table"] == "party_address"
    assert len(conf_qa) == 16


@pytest.mark.parametrize("hive_table,env", [("accounts", "LOCAL")])
def test_read_account(spark: SparkSession, hive_table: str, env: str) -> None:
    accounts_df = read_data(spark, env, False, None, hive_table)
    assert accounts_df.count() == 8


@pytest.mark.parametrize("hive_table,env", [("parties", "LOCAL")])
def test_read_parties(spark: SparkSession, hive_table: str, env: str, expected_party_rows: list[Row]) -> None:
    actual_party_rows = read_data(spark, env, False, None, hive_table).collect()
    assert actual_party_rows == expected_party_rows


@pytest.mark.parametrize("hive_table,env", [("accounts", "LOCAL")])
def test_read_contract(spark: SparkSession, hive_table: str, env: str, expected_contract_df: DataFrame) -> None:
    accounts_df = read_data(spark, env, False, None, hive_table)
    actual_contract_df = get_contract(accounts_df)
    assert actual_contract_df.collect() == expected_contract_df.collect()
    # TypeError: assert_df_equality() got an unexpected keyword argument 'ignore_schema'
    # assert_df_equality(actual_contract_df, expected_contract_df, ignore_schema=True)


@pytest.mark.parametrize("schema_name,hive_table,env", [("parties", "parties", "LOCAL")])
def test_get_schema(spark: SparkSession,
                    schema_name: str,
                    hive_table: str,
                    env: str,
                    parties_list: list[Any],
                    ) -> None:
    expected_df = spark.createDataFrame(parties_list, get_schema(schema_name))
    actual_df = read_data(spark, env, False, None, hive_table)
    # assert actual_df.collect() == expected_df.collect()
    assert_df_equality(actual_df, expected_df)


# def test_kafka_kv_df(spark: SparkSession, expected_final_df: DataFrame) -> None:
#
#     accounts_df = read_data(spark, "LOCAL", False, None, "accounts")
#     contract_df = get_contract(accounts_df)
#     parties_df = read_data(spark, "LOCAL", False, None, "parties")
#     relations_df = get_relations(parties_df)
#     address_df = read_data(spark, "LOCAL", False, None, "party_address")
#     relation_address_df = get_address(address_df)
#     party_address_df = join_party_address(relations_df, relation_address_df).sort("partyRelations.partyRelationStartDateTime")
#     data_df = join_contract_party(contract_df, party_address_df)
#     # .sort(expr("keys.contractIdentifier")[0])
#     # expected_final_df.show(truncate=False)
#     # expected_final_df.printSchema()
#     # print(expected_final_df.schema.simpleString())
#     # data_df.select(expr("account_id")).show()
#
#     actual_final_df = apply_header(spark, data_df).sort(expr("keys.keyValue")[0])\
#         .select("keys", "payload")
#     # actual_final_df.show(truncate=False)
#     # assert_df_equality(actual_final_df, expected_final_df)
#     assert actual_final_df.collect() == expected_final_df.sort(expr("keys.keyValue")[0]).collect()
#     # print(actual_final_df.collect())
