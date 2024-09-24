# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class SetupHelper():
    def __init__(self, env):
        conf = Config()
        self.landing_zone = conf.base_dir_data + "/raw"
        self.checkpoint_base = conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        

