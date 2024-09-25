# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Bronze():
    def __init__(self, env):
        self.Conf = Config()
        self.landing_zone = self.Conf.base_dir_data + "/raw"
        self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = Conf.db_name
        spark.sql(f"USE {self.catalog}.{self.db_name}")
    
    def consume_user_registration(self, once=True, processing_time="5 seconds"):
        import pyspark.sql.functions as F
        schema = "user_id long, device_id long, mac_address string, registration_timestamp double"

        df_stream = (spark.readStream
                          .format("cloudFiles")
                          .schema(schema)
                          .option("cloudFiles.format", "csv")
                          .option("maxFilesPerTrigger", 1)
                          .option("header", "true")
                          .load(self.landing_zone + "/registered_users_bz")
                          .withColumn("load_time", F.current_timestamp())
                          .withColumn("source_file", F.input_file_name())
                    )
        # Use append mode because bronze layer is expected to insert only from source
        stream_writer = (df_stream.writeStream \
                                  .format("delta") \
                                  .option("checkpointLocation", self.checkpoint_base + "/registered_users_bz") \
                                  .outputMode("append") \
                                  .queryName("registered_users_bz_ingestion_stream")
                    
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
