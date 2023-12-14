import sys
import uuid

from pyspark.sql.functions import col, to_json, struct

from lib import Utils, ConfigLoader, DataLoader, Transformations
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = f"SBDL-{uuid.uuid4()}"

    print(f"Initializing SBDL job in {job_run_env} with Job ID: {job_run_id}")
    conf = ConfigLoader.get_config(job_run_env)
    enable_hive = True if conf["enable.hive"] == "true" else False
    hive_db = conf["hive.database"]
    # account_hive_tbl = conf["account.hive.tabl"]
    # parties_hive_tbl = conf["parties.hive.table"]
    # party_address_hive_tbl = conf["party.address.hive.table"]

    print("Creating spark session")
    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Reading SBDL Account DF")
    hive_data_tbl = "accounts"
    account_df = DataLoader.read_data(spark, job_run_env, enable_hive, hive_db, hive_data_tbl)
    contracts_df = Transformations.get_contract(account_df)

    logger.info("Reading SBDL Party DF")
    hive_data_tbl = "parties"
    parties_df = DataLoader.read_data(spark, job_run_env, enable_hive, hive_db, hive_data_tbl)
    relations_df = Transformations.get_relations(parties_df)

    logger.info("Reading SBDL Account DF")
    hive_data_tbl = "party_address"
    address_df = DataLoader.read_data(spark, job_run_env, enable_hive, hive_db, hive_data_tbl)
    relation_address_df = Transformations.get_address(address_df)

    logger.info("Joining Party Relations and Address")
    party_address_df = Transformations.join_party_address(relations_df, relation_address_df)

    logger.info("Joining Account and Parties")
    data_df = Transformations.join_contract_party(contracts_df, party_address_df)

    logger.info("Applying Header and creating Event")
    final_df = Transformations.apply_header(spark, data_df)

    logger.info("Preparing to send data to Kafka")
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
                                  to_json(struct("*")).alias("value"))
    api_key = conf["kafka.api_key"]
    api_secret = conf["kafka.api_secret"]

    logger.info(f"Sending events to Kafka topic: {conf['kafka.topic']}")
    kafka_kv_df.write\
        .format("kafka")\
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"])\
        .option("topic", conf["kafka.topic"])\
        .option("kafka.security.protocol", conf["kafka.security.protocol"])\
        .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret))\
        .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"])\
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"])\
        .save()

    logger.info("Finished sending events to Kafka")
    # logger.info("One more message")
