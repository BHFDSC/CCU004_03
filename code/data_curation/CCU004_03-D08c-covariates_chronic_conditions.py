# Databricks notebook source
# MAGIC %md # CCU004_03-D08c-covariates_chronic_conditions
# MAGIC  
# MAGIC **Description** This notebook creates the covariates which are the chronic conditions. Covariates will be defined from the latest records before the study start date (with the exception of LSOA) for each individual as follows:
# MAGIC * LSOA: used to derive MSOA, region and deprivation;
# MAGIC * Prior history of outcomes;
# MAGIC * Prior history of comorbidities;
# MAGIC  
# MAGIC **Authors** Fionna Chalmers, Anna Stevenson, Carmen Petitjean and adapted by Spencer Keene
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton, Alexia Sampri for CCU018_01, earlier CCU002 sub-projects and subsequently CCU003_05-D10-covariates
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_out_covariates_chronic_conditions`** : chronic covariates for the cohort

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window
from pyspark.sql.functions import expr

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

spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_codelist_covariates_chronic_conditions') #cohortrefactoring
codelist = spark.table(f'{dsa}.{proj}_out_codelist_covariates_chronic_conditions') #cohortrefactoring

spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_codelist_covariates_diabetes') #ccu004-03 #cohortrefactoring
codelist_diabetes = spark.table(f'{dsa}.{proj}_out_codelist_covariates_diabetes') #ccu004-03 #cohortrefactoring

spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_cohort_{cohort}') #cohortrefactoring
cohort_dataset       = spark.table(path_out_cohort)

gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc_long = spark.table(path_cur_hes_apc_long)

# COMMAND ----------

display(hes_apc_long)

# COMMAND ----------

display(codelist)

# COMMAND ----------

codelist = codelist.union(codelist_diabetes)
display(codelist)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
print(f'study_start_date = {study_start_date}')
individual_censor_dates = (
  cohort_dataset
  .select('PERSON_ID', 'DOB')
  .withColumnRenamed('DOB', 'CENSOR_DATE_START')
  .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{study_start_date}')))
)

# check
# count_var(individual_censor_dates, 'PERSON_ID'); print()
# print(individual_censor_dates.limit(10).toPandas().to_string()); print()

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
gdppr_prepared = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# add individual censor dates
gdppr_prepared = (
  gdppr_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
gdppr_prepared = (
  gdppr_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()
# print(gdppr_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
gdppr_prepared = temp_save(df=gdppr_prepared, out_name=f'{proj}_tmp_covariates_chronic_conditions_gdppr_{cohort}') #cohortrefactoring

# COMMAND ----------

#Print codelists
#display(codelist.where(f.col('name') == 'ILD'))

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
hes_apc_long_prepared = (
  hes_apc_long
  .select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE', 'DIAG_POSITION', 'DIAG_DIGITS')
)

# check 1
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# merge in individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check 2
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .withColumn('flag_1',
    f.when((f.col('DATE').isNull()), 1)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)
  )
)
# tmpt = tab(hes_apc_long_prepared, '_tmp1'); print()

# filter to before CENSOR_DATE_END
# keep _tmp1 == 2
# tidy
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(f.col('flag_1').isin([2]))
  .drop('flag_1')
)

# check 3
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .withColumn('flag_2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)\
  )
)
# tmpt = tab(hes_apc_long_prepared, 'flag_2'); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(f.col('flag_2').isin([1]))
  .drop('flag_2')
)

# check 4
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()
# print(hes_apc_long_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
hes_apc_long_prepared = temp_save(df=hes_apc_long_prepared, out_name=f'{proj}_tmp_covariates_chronic_conditions_hes_apc_{cohort}') #cohortrefactoring

# COMMAND ----------

gdppr_prepared = spark.table(f'{dsa}.{proj}_tmp_covariates_chronic_conditions_gdppr_{cohort}') #cohortrefactoring
hes_apc_long_prepared = spark.table(f'{dsa}.{proj}_tmp_covariates_chronic_conditions_hes_apc_{cohort}') #cohortrefactoring

# COMMAND ----------

individual_censor_dates.select(f.min('CENSOR_DATE_START')).show()
hes_apc_long_prepared.select(f.min('CENSOR_DATE_START')).show()

# COMMAND ----------

# MAGIC %md # 3 HX chronic conditions

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

print('codelist_icd\n')
codelist_icd = (
  codelist
  .where(f.col('terminology') == 'ICD10')
)
tmpt = tab(codelist_icd, 'name', 'terminology'); print()
print(codelist_icd.orderBy('name', 'code').toPandas().to_string()); print()


print('codelist_snomed\n')
codelist_snomed = (
  codelist
  .where(f.col('terminology') == 'SNOMED')
)
tmpt = tab(codelist_snomed, 'name', 'terminology'); print()
print(codelist_snomed.orderBy('name', 'code').toPandas().to_string()); print()

# COMMAND ----------

#display(codelist_snomed)

# COMMAND ----------

#display(codelist_icd)

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

#display(hes_apc_long_prepared)

# COMMAND ----------

# dropping diag cols from prepared as hes and gdppr need the same column names to union below
hes_apc_long_prepared_test = (hes_apc_long_prepared.drop("DIAG_POSITION","DIAG_DIGITS"))

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_hx_out = {
    'hes_apc':  ['hes_apc_long_prepared_test',  'codelist_icd',  1]
  , 'gdppr':    ['gdppr_prepared',         'codelist_snomed', 2]
}

# run codelist match and codelist match summary functions
hx_out, hx_out_1st, hx_out_1st_wide = codelist_match(dict_hx_out, _name_prefix=f'cov_chron_'); print()
hx_out_summ_name, hx_out_summ_name_code = codelist_match_summ(dict_hx_out, hx_out); print()

# COMMAND ----------

## temp save
hx_out_all = hx_out['all']
hx_out_all = temp_save(df=hx_out_all, out_name=f'{proj}_tmp_covariates_chronic_conditions_all_{cohort}'); print() #cohortrefactoring
hx_out_1st = temp_save(df=hx_out_1st, out_name=f'{proj}_tmp_covariates_chronic_conditions_1st_{cohort}'); print() #cohortrefactoring
hx_out_1st_wide = temp_save(df=hx_out_1st_wide, out_name=f'{proj}_tmp_covariates_chronic_conditions_1st_wide_{cohort}'); print() #cohortrefactoring
hx_out_summ_name = temp_save(df=hx_out_summ_name, out_name=f'{proj}_tmp_covariates_chronic_conditions_summ_name_{cohort}'); print() #cohortrefactoring
hx_out_summ_name_code = temp_save(df=hx_out_summ_name_code, out_name=f'{proj}_tmp_covariates_chronic_conditions_summ_name_code_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

#display(cohort_dataset)

cohort_dataset.select(f.min('DOB')).show()

# COMMAND ----------

# check result
display(hx_out_1st_wide)

# COMMAND ----------

# check codelist match summary by name and source
display(hx_out_summ_name)

# COMMAND ----------

# check codelist match summary by name, source, and code
display(hx_out_summ_name_code)

# COMMAND ----------

# MAGIC %md # 4 Diabetes count typing 

# COMMAND ----------

hx_out_all = hx_out['all']

win = Window\
    .partitionBy(['PERSON_ID', 'name'])\
    .orderBy(f.desc('DATE')) #changed

diab2 = hx_out_all\
    .where(f.col('name').like("%diabete%"))\
    .withColumn('count_type', f.row_number().over(win)) 

win2 = Window\
    .partitionBy(['PERSON_ID'])
#    .orderBy('DATE', 'sourcen', 'code')

columns_to_drop = ['max_count_type', 'count_type', 'sourcen', 'code'] #changed 
diab3 = diab2\
    .withColumn('max_count_type', f.max('count_type').over(win2))\
    .where(f.col('max_count_type') == f.col('count_type'))\
    .drop(*columns_to_drop)  

win3 = Window\
    .partitionBy(['PERSON_ID'])\
    .orderBy(f.desc('DATE'))   

diab3 = diab3\
    .withColumn('_rownum', f.row_number().over(win3))\
    .where(f.col('_rownum') == 1)\
    .drop('_rownum')         

#display(diab3)

#diab4 = merge(diab3, cohort_dataset.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print() #ccu004-03 DOB added above

diab4 = diab3.withColumn("diabetes_age_type_count", expr("year(DATE) - year(CENSOR_DATE_START)"))

display(diab4)

#f.max(f.col(col)).alias('max')\
#f.max(f.col(col + '_date')).alias('max')\

# COMMAND ----------

#tmpf = (merge(diab4, cohort_dataset.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])

tmpf = merge(diab4, cohort_dataset.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print()

# COMMAND ----------

save_table(df=tmpf, out_name=f'{proj}_out_diabetes_type_count_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------

# MAGIC %md # F Save

# COMMAND ----------

tmp1 = merge(hx_out_1st_wide, cohort_dataset.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print() #ccu004-03 DOB added above

#tmp1 = (
#  hx_out_1st_wide
#  .join(individual_censor_dates, on='PERSON_ID', how='right')
#)


#do i need to convert to date format for the columns below?
tmp1 = tmp1.withColumn("diabetes_general_age", expr("year(cov_chron_diabetes_general_date) - year(DOB)")) #ccu004-03
tmp1 = tmp1.withColumn("diabetes_type2_age", expr("year(cov_chron_diabetes_type2_date) - year(DOB)")) #ccu004-03

# check
count_var(tmp1, 'PERSON_ID'); print()
print(len(tmp1.columns)); print()
print(pd.DataFrame({f'_cols': tmp1.columns}).to_string()); print()

# COMMAND ----------

# check final
#display(tmp1)
#count_var(tmp1, 'PERSON_ID'); print()
#tmp2 = tmp1.filter(tmp1.DOB.isNotNull())

#tmp1.where(tmp1.DOB.isNull()).count()

# COMMAND ----------

# Add chunk column
tmp1 = (
  tmp1
  .withColumn('CHUNK', f.floor(f.rand(seed=1234) * 10) + f.lit(1))
)

# COMMAND ----------

save_table(df=tmp1, out_name=f'{proj}_out_covariates_chronic_conditions_{cohort}', save_previous=True) #cohortrefactoring