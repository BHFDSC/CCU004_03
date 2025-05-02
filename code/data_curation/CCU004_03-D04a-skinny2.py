# Databricks notebook source
# MAGIC %md # CCU004_03-D04a-skinny
# MAGIC  
# MAGIC **Description** This notebook creates the skinny patient table, which includes key patient characteristics.
# MAGIC  
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D04-skinny
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_tmp_skinny`** : Skinny assembled

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')
spark.sql('CLEAR CACHE')

# COMMAND ----------

import sys, os

# Suppress pyspark complaints when calling DataFrame.saveAsTable() with overwrite mode
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# Prevent pyspark from complaining when parsing certain datetime strings
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Clear cache
spark.sql('CLEAR CACHE')

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import pyspark.pandas as ps
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# %run "/Repos/sjk98@medschl.cam.ac.uk/ccu004_03/functions"

# COMMAND ----------

# MAGIC %run "/Shared/SHDS/common/skinny_20221113"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU004_03-D01-parameters"

# COMMAND ----------

# widgets
dbutils.widgets.removeAll()
dbutils.widgets.text('1 project', proj)
dbutils.widgets.text('2 cohort', cohort)
dbutils.widgets.text('3 pipeline production date', pipeline_production_date)

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

skinny = spark.table(hds_curated_assets_demographic)

# COMMAND ----------

# MAGIC %md # 2 Create

# COMMAND ----------

display(skinny)

# COMMAND ----------

_skinny = (
    skinny
    .withColumnRenamed("person_id", "PERSON_ID")
    .withColumnRenamed("date_of_birth", "DOB")
    .withColumnRenamed("ethnicity_5_group", "ETHNIC_CAT")
    .withColumnRenamed("ethnicity_18_group", "ETHNIC_DESC")
    .withColumnRenamed("ethnicity_18_code", "ETHNIC")
    .withColumnRenamed("date_of_death", "DOD")
    .withColumnRenamed("sex", "SEX")
    .withColumnRenamed("lsoa", "recent_lsoa") # renaming these variables as they are closest to present rather than closest to baseline
    .withColumnRenamed("region", "recent_region") # renaming these variables as they are closest to present rather than closest to baseline
    .withColumnRenamed("imd_decile", "recent_imd_decile") # renaming these variables as they are closest to present rather than closest to baseline
    .withColumnRenamed("imd_quintile", "recent_imd_quintile") # renaming these variables as they are closest to present rather than closest to baseline
)

# Applying the transformation
_skinny = _skinny.withColumn(
    "SEX",
    f.when(f.col("SEX") == "F", 2)
     .when(f.col("SEX") == "M", 1)
     .otherwise(f.col("SEX"))
)

# COMMAND ----------

display(_skinny)

# COMMAND ----------

# MAGIC %md # 3 Save

# COMMAND ----------

save_table(df=_skinny, out_name=f'{proj}_tmp_skinny2_{cohort}', save_previous=False) #cohortrefactoring