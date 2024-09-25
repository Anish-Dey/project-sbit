# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name
    
    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# COMMAND ----------

class CDCUpserter:
    def __init__(self, merge_query, temp_view_name, id_column, sort_by):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name
        self.id_column = id_column
        self.sort_by = sort_by
    
    def upsert(self, df_micro_batch, batch_id):
        from pyspark.sql.window import Window
        from pyspark.sql import functions as F

        window = Window.partitionBy(self.id_column).orderBy(F.col(self.sort_by).desc())
        df_micro_batch.filter(F.col("update_type").isin(["new", "update"]))\
                      .withColumn("rank", F.rank().over(window))\
                      .filter(F.col("rank") == 1)\
                      .drop("rank")\
                      .createOrReplaceTempView(self.temp_view_name)
        
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# COMMAND ----------

class Silver:
    def __init__(self, env):
        Conf = Config()
        self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = self.Conf.db_name
        self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
        spark.sql(f"USE {self.catalog}.{self.db_name}")
    
    def upsert_users(self, once=True, processing_time = "15 seconds", startingVersion = 0):
        from pyspark.sql import functions as F

        #Idempotent - User cannot register again so ignore the duplicates and insert the new records
        query = f"""
                MERGE INTO {self.catalog}.{self.db_name}.users a
                USING users_delta b
                ON a.user_id = b.user_id
                WHEN NOT MATCHED THEN INSERT *
                """
        
        data_upserter = Upserter(query, "users_delta")
        # Spark Structured Streaming accepts append only sources. 
        #      - This is not a problem for silver layer streams because bronze layer is insert only
        #      - However, you may want to allow bronze layer deletes due to regulatory compliance 
        # Spark Structured Streaming throws an exception if any modifications occur on the table being used as a source
        #      - This is a problem for silver layer streaming jobs.
        #      - ignoreDeletes allows to delete records on partition column in the bronze layer without exception on silver layer streams 
        # Starting version is to allow you to restart your stream from a given version just in case you need it
        #      - startingVersion is only applied for an empty checkpoint
        # Limiting your input stream size is critical for running on limited capacity
        #      - maxFilesPerTrigger/maxBytesPerTrigger can be used with the readStream
        #      - Default value is 1000 for maxFilesPerTrigger and maxBytesPerTrigger has no default value
        #      - The recomended maxFilesPerTrigger is equal to #executors assuming auto optimize file size to 128 MB
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.registered_users_bz")
                         .selectExpr("user_id", "device_id", "mac_address", "cast(registration_timestamp as timestamp)")
                         .withWatermark("registration_timestamp", "30 seconds")
                         .dropDuplicates(["user_id", "device_id"])
                    )
        # We read new records in bronze layer and insert them to silver. So, the silver layer is insert only in a typical case. 
        # However, we want to ignoreDeletes, remove duplicates and also merge with an update statement depending upon the scenarios
        # Hence, it is recomended to se the update mode
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/users")
                                 .queryName("users_upsert_stream")
                        )
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
    
    def upsert_gym_logs(self, once = True, processing_time = "15 seconds", startingVersion = 0):
        from pyspark.sql import functions as F
        #Idempotent - Insert new login records 
        #           - Update logout time when 
        #                   1. It is greater than login time
        #                   2. It is greater than earlier logout
        #                   3. It is not NULL (This is also satisfied by above conditions)
        query = f"""
                MERGE INTO {self.catalog}.{self.db_name}.gym_logs a
                USING gym_logs_delta b
                ON a.mac_address = b.mac_address 
                AND a.gym = b.gym
                AND a.login = b.login
                WHEN MATCHED AND b.logout > a.login AND b.logout > a.logout
                THEN UPDATE SET logout = b.logout
                WHEN NOT MATCHED THEN INSERT *
                """
        
        data_upserter = Upserter(query, "gym_logs_delta")
        df_delta = (spark.readStream
                        .option("startingVersion", startingVersion)
                        .option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                        .table(f"{self.catalog}.{self.db_name}.gym_logs_bz")
                        .selectExpr("mac_address", "gym", "cast(login as timestamp)", "cast(logout as timestamp)")
                        .withWatermark("login", "30 seconds")
                        .dropDuplicates(["mac_address", "gym", "login"])
                    )
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/gym_logs")
                                 .queryName("gym_logs_upsert_stream")
                        )
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        
    
