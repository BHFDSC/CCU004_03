# Databricks notebook source
# MAGIC %md # CCU004_01-D06c-inclusion_exclusion_2
# MAGIC  
# MAGIC **Description** This notebook applies further inclusion/exclusion criteria.
# MAGIC  
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC  
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D08-inclusion_exclusion
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_01_tmp_inc_exc_2_cohort`** : cohort remaining after inclusion/exclusion criteria applied
# MAGIC - **`ccu004_01_tmp_inc_exc_2_flow`** : flowchart displaying total n of cohort after each inclusion/exclusion rule is applied

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
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

# DBTITLE 1,Functions
# %run "../../shds/common/functions"

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

spark.sql(f'REFRESH TABLE {path_tmp_inc_exc_cohort}')
spark.sql(f'REFRESH TABLE {path_tmp_inc_exc_flow}')
spark.sql(f'REFRESH TABLE {path_tmp_hx_nonfatal}')

cohort_dataset     = spark.table(path_tmp_inc_exc_cohort)
flow       = spark.table(path_tmp_inc_exc_flow)
hx_af_nonfatal  = spark.table(path_tmp_hx_nonfatal)

# COMMAND ----------

display(cohort_dataset)

# COMMAND ----------

cohort_dataset.count()

# COMMAND ----------

display(flow)

# COMMAND ----------

display(hx_af_nonfatal)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('cohort')
print('---------------------------------------------------------------------------------')
# check
count_var(cohort_dataset, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('history of nonfatal stroke and MI')
print('---------------------------------------------------------------------------------')
_hx_af_nonfatal = (
  hx_af_nonfatal
  .withColumn('hx_nonfatal', f.when((f.col('cov_hx_nonfatal_myocardial_infarction_flag') == 1) | (f.col('cov_hx_nonfatal_stroke_flag') == 1) , 1).otherwise(0))
)

# check
count_var(_hx_af_nonfatal, 'PERSON_ID'); print()
tmpt = tab(_hx_af_nonfatal, 'hx_nonfatal'); print()
#tmpt = tab(_hx_af_nonfatal.where(f.col('hx_af_nonfatal') == 0), 'cov_hx_af_flag', 'cov_hx_hypertension_flag', var2_unstyled=1); print()
#tmpt = tab(_hx_af_nonfatal.where(f.col('hx_af_nonfatal') == 1), 'cov_hx_af_flag', 'cov_hx_hypertension_flag', var2_unstyled=1); print()
#tmpt = tab(_hx_af_nonfatal, 'hx_af_nonfatal', 'cov_hx_hypertension_drugs_flag', var2_unstyled=1); print()

# tidy
_hx_af_nonfatal = _hx_af_nonfatal.select('PERSON_ID', 'cov_hx_nonfatal_myocardial_infarction_flag', 'cov_hx_nonfatal_stroke_flag','hx_nonfatal')


print('---------------------------------------------------------------------------------')
print('merged')
print('---------------------------------------------------------------------------------')
# merge in _hx_af_nonfatal
_merged = merge(cohort_dataset, _hx_af_nonfatal, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()

# temp save
_merged = temp_save(df=_merged, out_name=f'{proj}_tmp_inc_exc_2_merged_{cohort}'); print() #cohortrefactoring

# check
count_var(_merged, 'PERSON_ID'); print()

# COMMAND ----------

# check
display(_merged)

# COMMAND ----------

# check
tmpt = tab(_merged, 'hx_nonfatal'); print()

# COMMAND ----------

# MAGIC %md # 3 Inclusion / exclusion

# COMMAND ----------

tmp0 = _merged
tmpc = count_var(tmp0, 'PERSON_ID', ret=1, df_desc='original', indx=7); print()

# COMMAND ----------

display(tmpc)

# COMMAND ----------

# MAGIC %md ## 3.1 Exclude patients with a prior history of nonfatal stroke or MI

# COMMAND ----------

# note: need to apply this last, as the above exclusion criteria is also applied in the hx_nonfatal notebook

# check that the number of individuals remaining is equal to the number of individuals that we have computed prior history of nonfatal stoke or MI
# i.e., that the inclusion/exclusion criteria that was applied in the hx_af_nonfatal notebook is consistent with the above

# check
tmpt = tab(tmp0, 'hx_nonfatal'); print()
assert tmp0.where((f.col('hx_nonfatal').isNull()) | (~f.col('hx_nonfatal').isin([0,1]))).count() == 0

# filter
#tmp1 = tmp0.where(f.col('hx_nonfatal') == 0) ##ccu004-03 we want everyone
tmp1 = tmp0

# check
tmpt = count_var(tmp1, 'PERSON_ID', ret=1, df_desc='post exclusion of patients with hx of nonfatal stroke or MI', indx=8); print()
tmpc = tmpc.unionByName(tmpt)

# check
tmpt = tab(tmp1, 'hx_nonfatal'); print()

# COMMAND ----------

# MAGIC %md # 4 Flow diagram

# COMMAND ----------

# check flow table
tmpp = (
  flow
  .unionByName(tmpc)
  .orderBy('indx')
  .select('indx', 'df_desc', 'n', 'n_id', 'n_id_distinct')
  .withColumnRenamed('df_desc', 'stage')
  .toPandas()
)

tmpp['n'] = tmpp['n'].astype(int)
tmpp['n_id'] = tmpp['n_id'].astype(int)
tmpp['n_id_distinct'] = tmpp['n_id_distinct'].astype(int)
tmpp['n_diff'] = (tmpp['n'] - tmpp['n'].shift(1)).fillna(0).astype(int)
tmpp['n_id_diff'] = (tmpp['n_id'] - tmpp['n_id'].shift(1)).fillna(0).astype(int)
tmpp['n_id_distinct_diff'] = (tmpp['n_id_distinct'] - tmpp['n_id_distinct'].shift(1)).fillna(0).astype(int)
for v in [col for col in tmpp.columns if re.search("^n.*", col)]:
  tmpp.loc[:, v] = tmpp[v].map('{:,.0f}'.format)
for v in [col for col in tmpp.columns if re.search(".*_diff$", col)]:  
  tmpp.loc[tmpp['stage'] == 'original', v] = ''
# tmpp = tmpp.drop('indx', axis=1)
print(tmpp.to_string()); print()
# check indx 5 equals indx 6 
# drop indx 6

# COMMAND ----------

print(tmpp)
tmpp2 = pd.DataFrame(tmpp)
print(tmpp2)

# COMMAND ----------

## suppress cols in tmp1 by creating a new dataframe tmpp2 - to be able to export
#tmpp2 = tmpp
#cols = ['n', 'n_id', 'n_id_distinct']
#for i, var in enumerate(cols):
#  tmpp2 = tmpp2.withColumn(var, f.col(var).cast(t.IntegerType()))
#  typ = dict(tmpp2.dtypes)[var]
#  print(i, var, typ)  
#  assert str(typ) in('bigint')
#  assert tmpp2.where(f.col(var)<0).count() == 0
#  tmpp2 = (tmpp2
#         .withColumn(var,
#                     f.when(f.col(var) == 0, 0)
#                     .when(f.col(var) < 10, 10)
#                     .when(f.col(var) >= 10, 5*f.round(f.col(var)/5))
#                    )
#        )

# COMMAND ----------

display(tmpp)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

display(tmp1)

# COMMAND ----------

# MAGIC %md ## 5.1 Cohort

# COMMAND ----------

tmpf = tmp1.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'DOD', 'baseline_date', 'region', 'IMD_2019_DECILES', 'LSOA',
                   'cov_hx_nonfatal_myocardial_infarction_flag',
                   'cov_hx_nonfatal_stroke_flag',
                   'hx_nonfatal')

# check
count_var(tmpf, 'PERSON_ID')

# COMMAND ----------

# check 
display(tmpf)

# COMMAND ----------

save_table(df=tmpf, out_name=f'{proj}_tmp_inc_exc_2_cohort_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------

# MAGIC %md ## 5.2 Flow

# COMMAND ----------

save_table(df=tmpc, out_name=f'{proj}_tmp_inc_exc_2_flow_{cohort}', save_previous=True) #cohortrefactoring