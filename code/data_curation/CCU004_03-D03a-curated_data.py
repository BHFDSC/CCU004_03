# Databricks notebook source
# MAGIC %md # CCU004_03-D03a-curated_data
# MAGIC
# MAGIC **Project** CCU004_03
# MAGIC
# MAGIC **Description** This notebook creates the curated tables.
# MAGIC
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D03a-curated_data
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_cur_hes_apc_all_years_archive_long`** : HES APC codes in long format
# MAGIC - **`ccu004_03_archive_sing`** : Death codes in wide format (one row per person)
# MAGIC - **`ccu004_03_archive_long`** : Death codes in long format
# MAGIC - **`ccu004_03_cur_gdppr`** : GDPPR curated with a monotonically increasing ID

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window
from pyspark.sql.functions import upper # Function to convert string into capital letters

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

# DBTITLE 1,Common Functions
#%run "../../shds/common/functions"

# COMMAND ----------

# MAGIC %run "/Repos/sjk98@medschl.cam.ac.uk/ccu004_03/functions"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU004_03-D01-parameters"

# COMMAND ----------

# widgets
dbutils.widgets.removeAll()
dbutils.widgets.text('1 project', proj)
dbutils.widgets.text('2 cohort', cohort)
dbutils.widgets.text('3 pipeline production date', pipeline_production_date)

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------


# gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
# hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
# deaths  = extract_batch_from_archive(parameters_df_datasets, 'deaths')

# HDS team pre-prepared curated assets
hes_apc_curated_asset = spark.table(f'{dsa}.hds_curated_assets__hes_apc_diagnosis_2024_10_01')
deaths_curated_asset = spark.table(f'{dsa}.hds_curated_assets__deaths_single_2024_10_01')
deaths_curated_asset_long = spark.table(f'{dsa}.hds_curated_assets__deaths_cause_of_death_2024_10_01')


# COMMAND ----------

# MAGIC %md # 3. HES_APC

# COMMAND ----------


# # select columns (PERSON_ID, RECORD_ID, DATE, Diagnostic columns)
# # rename PERSON_ID
# _hes_apc = (
#   hes_apc  
#   .select(['PERSON_ID_DEID', 'EPIKEY', 'EPISTART'] 
#           + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])
#   .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
#   .orderBy('PERSON_ID', 'EPIKEY')
# )

# # check
# count_var(hes_apc, 'PERSON_ID_DEID'); print()
# count_var(hes_apc, 'EPIKEY'); print()

# # check for null EPISTART and potential use ADMIDATE to supplement
# tmpp = (
#   hes_apc
#   .select('EPISTART', 'ADMIDATE')
#   .withColumn('_EPISTART', f.when(f.col('EPISTART').isNotNull(), 1).otherwise(0))
#   .withColumn('_ADMIDATE', f.when(f.col('ADMIDATE').isNotNull(), 1).otherwise(0))
# )
# tmpt = tab(tmpp, '_EPISTART', '_ADMIDATE', var2_unstyled=1); print()
# # => ADMIDATE is always null when EPISTART is null

# COMMAND ----------

# MAGIC %md ## 3.1 Diagnosis

# COMMAND ----------

# MAGIC %md ### 3.1.1 Create

# COMMAND ----------


# # reshape twice, tidy, and remove records with missing code

# hes_apc_long = (
#   reshape_wide_to_long_multi(_hes_apc, i=['PERSON_ID', 'EPIKEY', 'EPISTART'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
#   .withColumn('_tmp', f.substring(f.col('DIAG_4_'), 1, 3))
#   .withColumn('_chk', udf_null_safe_equality('DIAG_3_', '_tmp').cast(t.IntegerType()))
#   .withColumn('_DIAG_4_len', f.length(f.col('DIAG_4_')))
#   .withColumn('_chk2', f.when((f.col('_DIAG_4_len').isNull()) | (f.col('_DIAG_4_len') <= 4), 1).otherwise(0))
# )

# COMMAND ----------


# # check
# # tmpt = tab(hes_apc_long, '_chk'); print()
# assert hes_apc_long.where(f.col('_chk') == 0).count() == 0
# tmpt = tab(hes_apc_long, '_DIAG_4_len'); print()
# tmpt = tab(hes_apc_long, '_chk2'); print()
# assert hes_apc_long.where(f.col('_chk2') == 0).count() == 0

# # tidy
# hes_apc_long = (
#   hes_apc_long
#   .drop('_tmp', '_chk')
# )

# hes_apc_long = reshape_wide_to_long_multi(hes_apc_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
#   .withColumnRenamed('POSITION', 'DIAG_POSITION')\
#   .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
#   .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
#   .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
#   .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
#   .withColumnRenamed('DIAG_', 'CODE')\
#   .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
#   .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

# Curated assets columns need to be renamed to match with the rest of the pipeline
hes_apc_curated_asset = (
    hes_apc_curated_asset
    .withColumnRenamed('person_id', 'PERSON_ID')
    .withColumnRenamed('epikey', 'EPIKEY')
    .withColumnRenamed('epistart', 'EPISTART')
    .withColumnRenamed('diag_position', 'DIAG_POSITION')
    .withColumnRenamed('diag_digits', 'DIAG_DIGITS')
    .withColumnRenamed('code', 'CODE')
    .select('PERSON_ID', 'EPIKEY', 'EPISTART', 'DIAG_POSITION', 'DIAG_DIGITS', 'CODE')
    )

# COMMAND ----------

# MAGIC %md ### 3.1.2 Save

# COMMAND ----------

# save_table(df=hes_apc_long, out_name=f'{proj}_cur_hes_apc_all_years_archive_long_{cohort}', save_previous=False) #cohortrefactoring
save_table(df=hes_apc_curated_asset, out_name=f'{proj}_cur_hes_apc_all_years_archive_long_{cohort}', save_previous=False) #cohortrefactoring

# COMMAND ----------

# hes_apc_long = spark.table(f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long_{cohort}') #cohortrefactoring
hes_apc_curated_asset = spark.table(f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long_{cohort}') #cohortrefactoring

# COMMAND ----------

# MAGIC %md ### 3.1.3 Check

# COMMAND ----------

# code refactored to perform checks in curated assets table
# check
count_var(hes_apc_curated_asset, 'PERSON_ID'); print()
count_var(hes_apc_curated_asset, 'EPIKEY'); print()

# check removal of trailing X
tmpp = hes_apc_curated_asset\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpp, 'flag'); print()
tmpt = tab(tmpp.where(f.col('CODE').rlike('X')), 'CODE', 'flag', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md # 4. Deaths

# COMMAND ----------

# MAGIC %md ## 4.1 Create

# COMMAND ----------

# import os 
# import sys

# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# # check
# count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID'); print()
# assert dict(deaths.dtypes)['REG_DATE'] == 'string'
# assert dict(deaths.dtypes)['REG_DATE_OF_DEATH'] == 'string'

# # define window for the purpose of creating a row number below as per the skinny patient table
# _win = Window\
#   .partitionBy('PERSON_ID')\
#   .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))

# # rename ID
# # remove records with missing IDs
# # reformat dates
# # reduce to a single row per individual as per the skinny patient table
# # select columns required
# # rename column ahead of reshape below
# # sort by ID
# deaths_out = deaths\
#   .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')\
#   .where(f.col('PERSON_ID').isNotNull())\
#   .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))\
#   .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))\
#   .withColumn('_rownum', f.row_number().over(_win))\
#   .where(f.col('_rownum') == 1)\
#   .select(['PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])\
#   .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')\
#   .orderBy('PERSON_ID')

# # check
# count_var(deaths_out, 'PERSON_ID'); print()
# count_var(deaths_out, 'REG_DATE_OF_DEATH'); print()
# count_var(deaths_out, 'S_COD_CODE_UNDERLYING'); print()

# # single row deaths 
# deaths_out_sing = deaths_out

# # remove records with missing DOD
# deaths_out = deaths_out\
#   .where(f.col('REG_DATE_OF_DEATH').isNotNull())\
#   .drop('REG_DATE')

# # check
# count_var(deaths_out, 'PERSON_ID'); print()

# # reshape
# # add 1 to diagnosis position to start at 1 (c.f., 0) - will avoid confusion with HES long, which start at 1
# # rename 
# # remove records with missing cause of death
# deaths_out_long = reshape_wide_to_long(deaths_out, i=['PERSON_ID', 'REG_DATE_OF_DEATH'], j='DIAG_POSITION', stubname='S_COD_CODE_')\
#   .withColumn('DIAG_POSITION', f.when(f.col('DIAG_POSITION') != 'UNDERLYING', f.concat(f.lit('SECONDARY_'), f.col('DIAG_POSITION'))).otherwise(f.col('DIAG_POSITION')))\
#   .withColumnRenamed('S_COD_CODE_', 'CODE4')\
#   .where(f.col('CODE4').isNotNull())\
#   .withColumnRenamed('REG_DATE_OF_DEATH', 'DATE')\
#   .withColumn('CODE3', f.substring(f.col('CODE4'), 1, 3))
# deaths_out_long = reshape_wide_to_long(deaths_out_long, i=['PERSON_ID', 'DATE', 'DIAG_POSITION'], j='DIAG_DIGITS', stubname='CODE')\
#   .withColumn('CODE', f.regexp_replace('CODE', r'[.,\-\s]', ''))
  
# # check
# count_var(deaths_out_long, 'PERSON_ID'); print()  
# tmpt = tab(deaths_out_long, 'DIAG_POSITION', 'DIAG_DIGITS', var2_unstyled=1); print() 
# tmpt = tab(deaths_out_long, 'CODE', 'DIAG_DIGITS', var2_unstyled=1); print()   
# # TODO - add valid ICD-10 code checker...

# COMMAND ----------

# Rename the columns of the deaths curates assets to match the rest of the pipeline
deaths_curated_asset = (
    deaths_curated_asset
    .withColumnRenamed('person_id', 'PERSON_ID')
    .withColumnRenamed('date_of_death', 'REG_DATE_OF_DEATH')
    .withColumnRenamed('reg_date', 'REG_DATE')
    .withColumnRenamed('s_underlying_cod_icd10', 'S_COD_CODE_UNDERLYING')
    .withColumnRenamed('s_cod_code_1', 'S_COD_CODE_1')
    .withColumnRenamed('s_cod_code_2', 'S_COD_CODE_2')
    .withColumnRenamed('s_cod_code_3', 'S_COD_CODE_3')
    .withColumnRenamed('s_cod_code_4', 'S_COD_CODE_4')
    .withColumnRenamed('s_cod_code_5', 'S_COD_CODE_5')
    .withColumnRenamed('s_cod_code_6', 'S_COD_CODE_6')
    .withColumnRenamed('s_cod_code_7', 'S_COD_CODE_7')
    .withColumnRenamed('s_cod_code_8', 'S_COD_CODE_8')
    .withColumnRenamed('s_cod_code_9', 'S_COD_CODE_9')
    .withColumnRenamed('s_cod_code_10', 'S_COD_CODE_10')
    .withColumnRenamed('s_cod_code_11', 'S_COD_CODE_11')
    .withColumnRenamed('s_cod_code_12', 'S_COD_CODE_12')
    .withColumnRenamed('s_cod_code_13', 'S_COD_CODE_13')
    .withColumnRenamed('s_cod_code_14', 'S_COD_CODE_14')
    .withColumnRenamed('s_cod_code_15', 'S_COD_CODE_15')
    .select('PERSON_ID', 'REG_DATE_OF_DEATH', 'REG_DATE', 'S_COD_CODE_UNDERLYING', 'S_COD_CODE_1', 'S_COD_CODE_2', 'S_COD_CODE_3', 'S_COD_CODE_4', 'S_COD_CODE_5', 'S_COD_CODE_6', 'S_COD_CODE_7', 'S_COD_CODE_8', 'S_COD_CODE_9', 'S_COD_CODE_10', 'S_COD_CODE_11', 'S_COD_CODE_12', 'S_COD_CODE_13', 'S_COD_CODE_14', 'S_COD_CODE_15')
)
deaths_curated_asset_long = (
    deaths_curated_asset_long
    .withColumnRenamed('person_id', 'PERSON_ID')
    .withColumnRenamed('date_of_death', 'DATE')
    .withColumnRenamed('cod_position', 'DIAG_POSITION')
    .withColumnRenamed('cod_digits', 'DIAG_DIGITS')
    .withColumnRenamed('code', 'CODE')
    .withColumn('DIAG_POSITION', upper(f.col('DIAG_POSITION')))
)

# COMMAND ----------

# MAGIC %md ## 4.2 Check

# COMMAND ----------

display(deaths_curated_asset.limit(5))
display(deaths_curated_asset_long.limit(5))

# COMMAND ----------

#  display(deaths_out_sing)

# COMMAND ----------

# display(deaths_out_long)

# COMMAND ----------

# MAGIC %md ## 4.3 Save

# COMMAND ----------

# save_table(df=deaths_out_sing, out_name=f'{proj}_cur_deaths_{db}_archive_sing_{cohort}', save_previous=False) #cohortrefactoring
save_table(df=deaths_curated_asset, out_name=f'{proj}_cur_deaths_{db}_archive_sing_{cohort}', save_previous=False) #cohortrefactoring

# COMMAND ----------

# save_table(df=deaths_out_long, out_name=f'{proj}_cur_deaths_{db}_archive_long_{cohort}', save_previous=False) #cohortrefactoring
save_table(df=deaths_curated_asset_long, out_name=f'{proj}_cur_deaths_{db}_archive_long_{cohort}', save_previous=False) #cohortrefactoring


# COMMAND ----------

# MAGIC %md # 5. GDPPR

# COMMAND ----------

#  #GDPPR no longer needs a consistent ID order
# gdppr_id = (
#   gdppr
#   .withColumn('mono_inc_id', f.monotonically_increasing_id())
# )

# COMMAND ----------

#  #GDPPR no longer needs a consistent ID order
# save_table(df=gdppr_id, out_name=f'{proj}_cur_gdppr_{cohort}', save_previous=False) #cohortrefactoring