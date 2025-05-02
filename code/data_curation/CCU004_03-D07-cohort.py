# Databricks notebook source
# MAGIC %md
# MAGIC # CCU004_03-D07-cohort
# MAGIC  
# MAGIC **Description** This notebook creates the final cohort table.
# MAGIC  
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC  
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D09-cohort
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_out_cohort`** : final cohort

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

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

#%run "../../shds/common/functions"

# COMMAND ----------

# MAGIC %run "/Repos/sjk98@medschl.cam.ac.uk/ccu004_03/functions"

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

spark.sql(f"""REFRESH TABLE {path_tmp_inc_exc_2_cohort}""") #cohortrefactoring
cohort_dataset = spark.table(path_tmp_inc_exc_2_cohort)

spark.sql(f'REFRESH TABLE {dsa}.{proj}_lsoa_{cohort}') #cohortrefactoring #removing as this should be included in skinny2 # re-adding
lsoa = spark.table(f'{dsa}.{proj}_lsoa_{cohort}') #cohortrefactoring #removing as this should be included in skinny2 # re-adding

hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
hes_ae  = extract_batch_from_archive(parameters_df_datasets, 'hes_ae')
hes_op  = extract_batch_from_archive(parameters_df_datasets, 'hes_op')

# COMMAND ----------

# check
count_var(cohort_dataset, 'PERSON_ID'); print()

# COMMAND ----------

# cohort
display(cohort_dataset)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('HES indicators')
print('--------------------------------------------------------------------------------------')
# hes_apc
_hes_apc = (
  hes_apc
  .select(f.col('PERSON_ID_DEID').alias('PERSON_ID'))
  .distinct()
  .where(f.col('PERSON_ID').isNotNull())
  .withColumn('in_hes_apc', f.lit(1))
)

# hes_ae
_hes_ae = (
  hes_ae
  .select(f.col('PERSON_ID_DEID').alias('PERSON_ID'))
  .distinct()
  .where(f.col('PERSON_ID').isNotNull())
  .withColumn('in_hes_ae', f.lit(1))
)

# hes_op
_hes_op = (
  hes_op
  .select(f.col('PERSON_ID_DEID').alias('PERSON_ID'))
  .distinct()
  .where(f.col('PERSON_ID').isNotNull())
  .withColumn('in_hes_op', f.lit(1))
)

# merge
hes = merge(_hes_apc, _hes_ae, ['PERSON_ID'], validate='1:1', indicator=0); print()
hes = merge(hes, _hes_op, ['PERSON_ID'], validate='1:1', indicator=0); print()

# add max and concat
hes = (
  hes
  .na.fill(value=0, subset=['in_hes_apc', 'in_hes_ae', 'in_hes_op'])
  .withColumn('in_hes', f.greatest(f.col('in_hes_apc'), f.col('in_hes_ae'), f.col('in_hes_op')))
  .withColumn('in_hes_concat', f.concat(f.col('in_hes_apc'), f.col('in_hes_ae'), f.col('in_hes_op')))
)


# COMMAND ----------

# temp save
hes = temp_save(df=hes, out_name=f'{proj}_tmp_cohort_hes_{cohort}'); print() #cohortrefactoring

# check
count_var(hes, 'PERSON_ID'); print()
tmpt = tab(hes, 'in_hes'); print()
tmpt = tab(hes, 'in_hes_concat'); print()
tmpt = tab(hes, 'in_hes_concat', 'in_hes', var2_unstyled=1); print()
print(hes.limit(10).toPandas().to_string()); print()

# COMMAND ----------

hes = spark.table(f'{dsa}.{proj}_tmp_cohort_hes_{cohort}') #cohortrefactoring

# COMMAND ----------

# MAGIC %md # 3 Create

# COMMAND ----------

# MAGIC %md ## 3.1 Add HES indicators

# COMMAND ----------

tmp1 = merge(cohort_dataset, hes, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0)

# check
count_var(tmp1, 'PERSON_ID'); print()
tmpt = tab(tmp1, 'in_hes'); print()
tmpt = tab(tmp1, 'in_hes_concat'); print()

# COMMAND ----------

# check
display(tmp1)

# COMMAND ----------

# MAGIC %md ## 3.2 Add dates

# COMMAND ----------

print(f'study_start_date = {study_start_date}')
print(f'study_end_date   = {study_end_date}')

# add dates and ages
tmp2 = (
  tmp1
  .withColumn('study_start_date', f.to_date(f.lit(study_start_date)))
  .withColumn('study_end_date', f.to_date(f.lit(study_end_date)))
  .withColumn('study_start_age', f.round(f.datediff(f.col('study_start_date'), f.col('DOB'))/365.25, 2))
  .withColumn('fu_end_date', f.least('DOD', 'study_end_date'))
  .withColumn('_fu_end_date_source', 
              f.when(f.col('fu_end_date') == f.col('DOD'), 'DOD')
              .when(f.col('fu_end_date') == f.col('study_end_date'), 'study_end_date')
             )    
  .withColumn('fu_end_age', f.round(f.datediff(f.col('fu_end_date'), f.col('DOB'))/365.25, 2))
  .withColumn('fu_days', f.datediff(f.col('fu_end_date'), f.col('baseline_date')))
)

# COMMAND ----------

# check
display(tmp2)

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

count_var(tmp2, 'PERSON_ID'); print()
tmpt = tab(tmp2, 'SEX'); print()
tmpt = tab(tmp2, 'ETHNIC_CAT'); print()
tmpt = tabstat(tmp2, 'DOB', date=1); print()
tmpt = tabstat(tmp2, 'study_start_age'); print()

# COMMAND ----------

# DBTITLE 1,study_start_age
tmpp = (
  tmp2
  .withColumn('study_start_age', f.round(f.col('study_start_age')*12)/12)
  .groupBy('study_start_age')
  .agg(f.count(f.lit(1)).alias('n'))
  .toPandas()
)

plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['study_start_age'], tmpp['n'], width = 1/12, edgecolor = None)
axes.set(xticks=np.arange(0, 116, step=5))
axes.set_xlim(0,116)
axes.set(xlabel="Age at baseline (years)")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
display(fig)

# COMMAND ----------

# DBTITLE 1,DOB
tmpp = (
  tmp2
  .groupBy('DOB')
  .agg(f.count(f.lit(1)).alias('n'))
  .toPandas()
)
tmpp['date_formatted'] = pd.to_datetime(tmpp['DOB'], errors='coerce')

# plot
plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2

axes.bar(tmpp['date_formatted'], tmpp['n'], width = 31, edgecolor = None)
# axes.set(xticks=np.arange(0, 19, step=1))
axes.set_xlim(datetime.datetime(1904, 11, 1), datetime.datetime(2019, 11, 1))
axes.set(xlabel="Date of birth")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
display(fig)

# COMMAND ----------

# check
tmpt = tabstat(tmp2, 'study_start_date', date=1); print()
tmpt = tabstat(tmp2, 'study_end_date', date=1); print()

tmpt = tab(tmp2, '_fu_end_date_source'); print() 
tmpt = tabstat(tmp2, 'fu_end_date', date=1); print()
tmpt = tabstat(tmp2, 'fu_end_date', byvar='_fu_end_date_source', date=1); print()
tmpt = tabstat(tmp2, 'fu_end_age'); print()
tmpt = tabstat(tmp2, 'fu_days'); print()


# further checks
tmpp = (
  tmp2
  .withColumn('_nse', udf_null_safe_equality('baseline_date', 'study_start_date').cast(t.IntegerType()))  
  .withColumn('_flag_DOD_lt_study_start_date', f.when(f.col('DOD') < f.col('study_start_date'), 1).otherwise(0))  
  .withColumn('_flag_study_end_date_lt_study_start_date', f.when(f.col('study_end_date') < f.col('study_start_date'), 1).otherwise(0))
)


# COMMAND ----------

tmpt = tab(tmpp, '_nse'); print()
assert tmpp.where(f.col('_nse') != 1).count() == 0
tmpt = tab(tmpp, '_flag_DOD_lt_study_start_date'); print()
assert tmpp.where(f.col('_flag_DOD_lt_study_start_date') != 0).count() == 0
tmpt = tab(tmpp, '_flag_study_end_date_lt_study_start_date'); print()      
assert tmpp.where(f.col('_flag_study_end_date_lt_study_start_date') != 0).count() == 0

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

tmp3 = tmp2.drop('baseline_date', 'LSOA', 'IMD_2019_DECILES', 'region')

# COMMAND ----------

cohort_lsoa = merge(tmp3, lsoa, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0) # remove as this is now included in cohort # re-adding

# COMMAND ----------

save_table(df=tmp3, out_name=f'{proj}_out_cohort_{cohort}', save_previous=True) #cohortrefactoring
save_table(df=cohort_lsoa, out_name=f'{proj}_out_cohort_lsoa_{cohort}', save_previous=True) #cohortrefactoring # remove as this is now included in cohort # re-adding