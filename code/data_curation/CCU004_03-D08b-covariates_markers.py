# Databricks notebook source
# MAGIC %md # CCU004_03_D08b_covariates_markers
# MAGIC
# MAGIC **Description** This notebook creates the covariates for markers such as SBP and HDL etc.
# MAGIC   
# MAGIC **Author(s)** Health Data Science Team, BHF Data Science Centre (TB)
# MAGIC
# MAGIC **Project** CCU051
# MAGIC
# MAGIC **First copied over** 2022.11.29 (from CCU002_07)
# MAGIC
# MAGIC **Date last updated** 2023.03.12
# MAGIC
# MAGIC **Date last run** 2023.03.12
# MAGIC
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC codelist_qcovid - codelist_covariates\
# MAGIC 'ccu051_tmp_cohort'\
# MAGIC gdppr - hes_apc_long - hes_apc_oper_long
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Adapted from previous work by Tom Bolton (John Nolan, Elena Raffetti, Alexia Sampri) from CCU002_07, CCU018_01 and earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_out_covariates_markers`** : covariate markers for the cohort

# COMMAND ----------

spark.sql('CLEAR CACHE')

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
#%run "../../shds/common/functions"

# COMMAND ----------

# MAGIC %run "/Repos/sjk98@medschl.cam.ac.uk/ccu004_03/functions"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "./CCU004_03-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

# codelists
spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_codelist_covariates') 
codelist_covariates = spark.table(f'{dsa}.{proj}_out_codelist_covariates_markers') 

# cohort
spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_cohort_{cohort}') #cohortrefactoring
cohort_dataset       = spark.table(path_out_cohort)

# data sources
gdppr = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
  # use mono_inc_id version # - we no longer use the mono_inc_id version

# COMMAND ----------

# MAGIC %md # 2. Check

# COMMAND ----------

# MAGIC %md ## 2.1. Codelist

# COMMAND ----------

# check
tmpt = tab(codelist_covariates, 'name', 'terminology'); print() 

# COMMAND ----------

# check
display(codelist_covariates.orderBy('name', 'terminology', 'code'))

# COMMAND ----------

# MAGIC %md ## 2.2. Cohort

# COMMAND ----------

# check
display(cohort_dataset)

# COMMAND ----------

# check
count_var(cohort_dataset, 'PERSON_ID'); print()

# COMMAND ----------

tmpt = tab(cohort_dataset, 'cov_hx_nonfatal_myocardial_infarction_flag', 'cov_hx_nonfatal_stroke_flag'); print()
tmpt = tab(cohort_dataset, 'hx_nonfatal'); print()

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# MAGIC %md ## 3.1. Codelist

# COMMAND ----------

# check
count_varlist(codelist_covariates, ['name'])
count_varlist(codelist_covariates, ['name', 'terminology', 'code'])
tmpt = tab(codelist_covariates, 'name', 'terminology'); print()

# filter
codelist_covariates_prepared = (
  codelist_covariates
  .where(f.col('name').isin(['bmi', 'sbp', 'tchol', 'hdl', 'hba1c', 'egfr', 'creat', 'height', 'weight'])) #ccu004-03
)
 
# check
count_varlist(codelist_covariates_prepared, ['name'])
count_varlist(codelist_covariates_prepared, ['name', 'terminology', 'code'])
tmpt = tab(codelist_covariates_prepared, 'name', 'terminology'); print()
print(codelist_covariates_prepared.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

#display(cohort_dataset)

# COMMAND ----------

# MAGIC %md ## 3.2. Cohort

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates - lookback to identify covariates')
print('--------------------------------------------------------------------------------------')
print(study_start_date)
# assert study_start_date == '2022-01-01' #ccu004-03 #cohortrefactoring - this would not work in the multicohort pipeline
assert cohort_dataset.where(f.col('study_start_date') == study_start_date).count() == cohort_dataset.count() #cohortrefactoring


# CENSOR_DATE_START is the start of the lookback period, CENSOR_DATE_END minus 5 years
# CENSOR_DATE_END is the end of the lookback period, so study_start_date (i.e. 2020-01-01) + 6 months for this project (wanting to look slightly into the future)
individual_censor_dates = (
  cohort_dataset
  .select('PERSON_ID', 'study_start_date')\
  .withColumn('CENSOR_DATE_START', f.add_months(f.col('study_start_date'), -18))\
  .withColumn('CENSOR_DATE_END', f.add_months(f.col('study_start_date'), 3))\
  .drop('study_start_date')
)
#ccu004-03 changed from -12*5 lookback (5-years) and 6 months forward

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
tmpt = tabstat(individual_censor_dates, 'CENSOR_DATE_START', date=1); print()
tmpt = tabstat(individual_censor_dates, 'CENSOR_DATE_END', date=1); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 3.3. Source data

# COMMAND ----------

# copied from CCU003_05

# # # # NOTE # # # #
# The covariates in this notebook are defined according to the last 1 or 3 years of data
# therefore we only use DATE and do not utilise RECORD_DATE as done previously
# because RECORD_DATE could relate to a DATE that is outside of the last 1 or 3 years
# e.g., DATE is null, RECORD_DATE is as at last 1 year date - but the true DATE could be before RECORD_DATE...

# print('---------------------------------------------------------------------------------')
# print(f'gdppr') 
# print('---------------------------------------------------------------------------------')
# reduce and rename columns
# remove nulls
gdppr_1 = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE', 'VALUE1_CONDITION', 'VALUE2_CONDITION')
  .where(f.col('PERSON_ID').isNotNull())  
  .where(f.col('DATE').isNotNull())
  .withColumn('mono_id', f.monotonically_increasing_id())
)

# add individual censor dates
# gdppr_2 = merge(gdppr_1, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
gdppr_2 = (
  gdppr_1
  .join(individual_censor_dates, on=['PERSON_ID'], how='inner')
)
  
# check
count_var(gdppr_2, 'PERSON_ID'); print()  
  
# filter
gdppr_3 = (
  gdppr_2
  .where(f.col('DATE') >= f.col('CENSOR_DATE_START')) 
  .where(f.col('DATE') < f.col('CENSOR_DATE_END'))
  .withColumn('_diff', f.datediff(f.col('CENSOR_DATE_END'), f.col('DATE')))
)

# temp save
gdppr_prepared = temp_save(df=gdppr_3, out_name=f'{proj}_tmp_covariates_markers_gdppr_{cohort}'); print() #cohortrefactoring

# check
count_var(gdppr_prepared, 'PERSON_ID'); print()
tmpt = tabstat(gdppr_prepared, 'DATE', date=1); print()  
tmpt = tabstat(gdppr_prepared, '_diff'); print()  
  
# tidy
gdppr_prepared = (
  gdppr_prepared
  .drop('_diff')
)

# check
print(gdppr_prepared.limit(10).toPandas().to_string()); print()

# # 18 mins

# COMMAND ----------

# alternative to running cell above if updates to this table are not needed
# gdppr_prepared = (
#   spark.table(f'{dsa}.{proj}_tmp_covariates_markers_gdppr')
#   .drop('_diff')
# )

# COMMAND ----------

# MAGIC %md # 4. Codelist match

# COMMAND ----------

# MAGIC %md ## 4.1. Codelist

# COMMAND ----------

# check terminologies
tmpt = tab(codelist_covariates_prepared, 'name', 'terminology'); print()
list_terminology = (
  list(codelist_covariates_prepared
    .select('terminology')
    .distinct()
    .toPandas()['terminology']
  )
)
assert set(list_terminology) <= set(['SNOMED', 'DMD', 'ICD10', 'OPCS4', 'BNF'])

# partition codelist 
codelist_snomed_dmd = (
  codelist_covariates_prepared
  .where(f.col('terminology').isin(['SNOMED', 'DMD']))
)
# codelist_icd10 = (
#   codelist_prepared
#   .where(f.col('terminology').isin(['ICD10']))
# )
# codelist_opcs4 = (
#   codelist_prepared
#   .where(f.col('terminology').isin(['OPCS4']))
# )
# # codelist_bnf = (
# #   codelist_prepared
# #   .where(f.col('terminology').isin(['BNF']))
# # )

# check
tmpt = tab(codelist_snomed_dmd, 'name', 'terminology'); print()
# tmpt = tab(codelist_icd10, 'name', 'terminology'); print()
# tmpt = tab(codelist_opcs4, 'name', 'terminology'); print()
# tmpt = tab(codelist_bnf, 'name', 'terminology'); print()

# COMMAND ----------

# MAGIC %md ## 4.2. Create

# COMMAND ----------

# dictionary - key: dataset, codelist, and ordering in the event of tied records
dict_input = {
    'gdppr': ['gdppr_prepared', 'codelist_snomed_dmd', 1]
}

# run codelist match and codelist match summary functions
# , df_1st, df_1st_wide
dict_all = codelist_match_v2_test(dict_input, _name_prefix=f'cov_', stages_to_run=1); print()
df_all_summ_name, df_all_summ_name_code = codelist_match_summ(dict_input, dict_all); print()

# temp save
df_all = dict_all['all']
df_all = temp_save(df=df_all, out_name=f'{proj}_tmp_covariates_markers_df_all_{cohort}'); print() # _tmp_hx_all_flag_2 #cohortrefactoring
# df_1st = temp_save(df=_hx_1st, out_name=f'{proj2}_{datedir}_tmp_hx_1st'); print()
# df_1st_wide = temp_save(df=_hx_1st_wide, out_name=f'{proj2}_{datedir}_tmp_hx_1st_wide'); print()
df_all_summ_name = temp_save(df=df_all_summ_name, out_name=f'{proj}_tmp_covariates_markers_df_all_summ_name_{cohort}'); print() #cohortrefactoring
df_all_summ_name_code = temp_save(df=df_all_summ_name_code, out_name=f'{proj}_tmp_covariates_markers_df_all_summ_name_code_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

# MAGIC %md ## 4.3. Check

# COMMAND ----------

display(df_all.orderBy('PERSON_ID', 'DATE', 'name', 'source', 'code'))

# COMMAND ----------

# add commas
df_all_summ_name_commas = df_all_summ_name
for colx in [col for col in df_all_summ_name.columns if re.match('^n_.*$', col)]:
  df_all_summ_name_commas = (
    df_all_summ_name_commas
    .withColumn(colx, f.format_number(colx, 0))
  )
display(df_all_summ_name_commas.orderBy('name'))

# COMMAND ----------

# add commas
df_all_summ_name_code_commas = df_all_summ_name_code
for colx in [col for col in df_all_summ_name_code.columns if (re.match('^n_.*$', col)) or (re.match('^n$', col))]:
  df_all_summ_name_code_commas = (
    df_all_summ_name_code_commas
    .withColumn(colx, f.format_number(colx, 0))
  )
display(df_all_summ_name_code_commas.orderBy('name', 'terminology', 'code'))

# COMMAND ----------

display(df_all_summ_name_code)

# COMMAND ----------

# MAGIC %md # 5. Prepare

# COMMAND ----------

df_all = spark.table(f'{dsa}.{proj}_tmp_covariates_markers_df_all_{cohort}') #cohortrefactoring

# COMMAND ----------

# check
display(df_all.orderBy('PERSON_ID', 'name', 'DATE', 'source', 'code'))

# COMMAND ----------

# MAGIC %md ## 5.1. Filter nulls/OOR

# COMMAND ----------

# filter out nulls or out of range (OOR) values to avoid selecting these in the next stage

## round values
#df_all_1 = (
#  df_all
#  .withColumn('VALUE1_CONDITION_orig', f.col('VALUE1_CONDITION'))
#  .withColumn('VALUE1_CONDITION', f.round(f.col('VALUE1_CONDITION'), 2))
#)

## check
#tmpt = tab(df_all_1, 'name', 'source'); print()
#tmpt = tabstat(df_all_1, 'VALUE1_CONDITION', byvar='name'); print()

## filter nulls and out of range
#df_all_2 = (
#  df_all_1
#  .where(f.col('VALUE1_CONDITION').isNotNull())
#  # filter bmi
#  .where(
#    (f.col('name') != 'bmi')
#    | ((f.col('name') == 'bmi') & (f.col('VALUE1_CONDITION') >= 10) & (f.col('VALUE1_CONDITION') <= 100))
#  )
#  # filter sbp
#  .where(
#    (f.col('name') != 'sbp')
#    | ((f.col('name') == 'sbp') & (f.col('VALUE1_CONDITION') >= 60) & (f.col('VALUE1_CONDITION') <= 250))
#  )
#  # filter tchol
#  .where(
#    (f.col('name') != 'tchol')
#    | ((f.col('name') == 'tchol') & (f.col('VALUE1_CONDITION') >= 1.75) & (f.col('VALUE1_CONDITION') <= 20))
#  )
#  # filter hdl
#  .where(
#    (f.col('name') != 'hdl')
#    | ((f.col('name') == 'hdl') & (f.col('VALUE1_CONDITION') >= 0.2) & (f.col('VALUE1_CONDITION') <= 10))
#  )
#  # filter hba1c
#  .where(
#    (f.col('name') != 'hba1c')
#    | ((f.col('name') == 'hba1c') & (f.col('VALUE1_CONDITION') >= 0) & (f.col('VALUE1_CONDITION') <= 195))
#  )
#  # filter egfr
#  .where(
#    (f.col('name') != 'egfr')
#    | ((f.col('name') == 'egfr') & (f.col('VALUE1_CONDITION') >= 0) & (f.col('VALUE1_CONDITION') <= 175))
#  )
#  # filter creatinine
#  .where(
#    (f.col('name') != 'creat')
#    | ((f.col('name') == 'creat') & (f.col('VALUE1_CONDITION') >= 0) & (f.col('VALUE1_CONDITION') <= 250))
#  )
#)

## check
#tmpt = tab(df_all_2, 'name', 'source'); print()
#tmpt = tabstat(df_all_2, 'VALUE1_CONDITION', byvar='name'); print()

# number of names check
# assert df_all_2.select('name').distinct().count() == count_name # check no names have been missed in the filtering above

# COMMAND ----------

# filter out nulls or out of range (OOR) values to avoid selecting these in the next stage

# round values
df_all_1 = (
  df_all
  .withColumn('VALUE1_CONDITION_orig', f.col('VALUE1_CONDITION'))
  .withColumn('VALUE1_CONDITION', f.round(f.col('VALUE1_CONDITION'), 2))
  .where(f.col('VALUE1_CONDITION').isNotNull())
)

# check
#tmpt = tab(df_all_1, 'name', 'source'); print()
#tmpt = tabstat(df_all_1, 'VALUE1_CONDITION', byvar='name'); print()

# filter nulls and out of range
df_all_2 = (
  df_all_1
  # filter bmi
  .where(
    (f.col('name') != 'bmi')
    | ((f.col('name') == 'bmi') & (f.col('VALUE1_CONDITION') >= 10) & (f.col('VALUE1_CONDITION') <= 100))
  )
)



##ccu004-03 if cell doesn't run it is because of the part directly below.
# convert cm to m for height
#df_all_2 = (
#  df_all_2
#  .withColumn('VALUE1_CONDITION',
#    f.when((f.col('name')=='height' & f.col('VALUE1_CONDITION') < 3), f.col('VALUE1_CONDITION'*100)).otherwise(f.col('VALUE1_CONDITION'))
#    )
#)


df_all_2 = (
  df_all_2
  .withColumn('VALUE1_CONDITION', 
    f.when((f.col('name')=='height') & (f.col('VALUE1_CONDITION') < 3), f.col('VALUE1_CONDITION')*100).otherwise(f.col('VALUE1_CONDITION'))
    )
)

df_all_2 = (
  df_all_2
  # filter height
  .where(
    (f.col('name') != 'height')
    | ((f.col('name') == 'height') & (f.col('VALUE1_CONDITION') >= 100) & (f.col('VALUE1_CONDITION') < 300))
  )
)

df_all_2 = (
  df_all_2
  # filter weight
  .where(
    (f.col('name') != 'weight')
    | ((f.col('name') == 'weight') & (f.col('VALUE1_CONDITION') >= 10) & (f.col('VALUE1_CONDITION') <= 450))
  )
)

df_all_2 = (
  df_all_2
  # filter sbp
  .where(
    (f.col('name') != 'sbp')
    | ((f.col('name') == 'sbp') & (f.col('VALUE1_CONDITION') >= 60) & (f.col('VALUE1_CONDITION') <= 250))
  )
)

df_all_2 = (
  df_all_2
  # filter tchol
  .where(
    (f.col('name') != 'tchol')
    | ((f.col('name') == 'tchol') & (f.col('VALUE1_CONDITION') >= 1.75) & (f.col('VALUE1_CONDITION') <= 20))
  )
)

df_all_2 = (
  df_all_2
  # filter hdl
  .where(
    (f.col('name') != 'hdl')
    | ((f.col('name') == 'hdl') & (f.col('VALUE1_CONDITION') >= 0.2) & (f.col('VALUE1_CONDITION') <= 10))
  )
)

df_all_2 = (
  df_all_2
  # filter hba1c
  .where(
    (f.col('name') != 'hba1c')
    | ((f.col('name') == 'hba1c') & (f.col('VALUE1_CONDITION') >= 0) & (f.col('VALUE1_CONDITION') <= 195))
  )
)


df_all_2 = (
  df_all_2
  # filter egfr
  .where(
    (f.col('name') != 'egfr')
    | ((f.col('name') == 'egfr') & (f.col('VALUE1_CONDITION') >= 0) & (f.col('VALUE1_CONDITION') <= 175))
  )
)

df_all_2 = (
  df_all_2
  # filter creatinine
  .where(
    (f.col('name') != 'creat')
    | ((f.col('name') == 'creat') & (f.col('VALUE1_CONDITION') >= 0) & (f.col('VALUE1_CONDITION') <= 250))
  )
)

# check
tmpt = tab(df_all_2, 'name', 'source'); print()
tmpt = tabstat(df_all_2, 'VALUE1_CONDITION', byvar='name'); print()

# number of names check
# assert df_all_2.select('name').distinct().count() == count_name # check no names have been missed in the filtering above

# COMMAND ----------

# MAGIC %md ## 5.2. Filter duplicates

# COMMAND ----------

# flag duplicates (using a stable ordering)
win1 = Window\
  .partitionBy('PERSON_ID', 'name', 'DATE', 'VALUE1_CONDITION')\
  .orderBy('mono_id')
win2 = Window\
  .partitionBy('PERSON_ID', 'name', 'DATE', 'VALUE1_CONDITION')
df_all_2 = (
  df_all_2
  .withColumn('rownum', f.row_number().over(win1))
  .withColumn('rownummax', f.count('PERSON_ID').over(win2))
)
  
# check
tmpt = tab(df_all_2.where(f.col('rownum') == 1), 'rownummax'); print()

# filter duplicates
df_all_3 = (
  df_all_2
  .where(f.col('rownum') == 1)
)

# check
tmpt = tab(df_all_3, 'rownum'); print()
tmpt = tab(df_all_3, 'name', 'source'); print()
tmpt = tabstat(df_all_3, 'VALUE1_CONDITION', byvar='name'); print()

# tidy
df_all_3 = df_all_3.drop('rownum', 'rownummax')

# COMMAND ----------

# check
display(df_all_2.where(f.col('rownummax') > 1))

# COMMAND ----------

# MAGIC %md ## 5.3. Filter lookback

# COMMAND ----------

# NOT NEEDED FOR CCU004_01 - lookbacks are the same for all markers


# # filter lookback according to name

# # define 
# list_6m = [  
#   'qcovid_BoneMarrowTransplant'
#   , 'qcovid_RadioTherapyInLast6M'
#   , 'qcovid_SolidOrganTransplant'
#   , 'qcovid_PrescribedImmunoSupp'
#   , 'qcovid_PrescribedOralSteroi'
#   , 'qcovid_TakingAntiLeukotrien'
# ]

# # prepare
# df_all_filtered_1 = (
#   df_all_filtered_1
#   .withColumn('lookback_filter', f.when(f.col('name').isin(list_6m), f.lit('6m')).otherwise(f.lit('none')))
#   .withColumn('fu', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_END'))/365.25)
#   .withColumn('CENSOR_DATE_END_minus_6m', f.add_months(f.col('CENSOR_DATE_END'), -6))
# )

# # check
# tmpt = tab(df_all_filtered_1, 'name', 'lookback_filter'); print()
# tmpt = tabstat(df_all_filtered_1, 'fu', byvar=['lookback_filter', 'name']); print()
# tmpt = tabstat(df_all_filtered_1, 'CENSOR_DATE_END_minus_6m', byvar='name', date=1); print()

# # filter
# df_all_filtered_2 = (
#   df_all_filtered_1  
#   .where(
#     # no lookback filtering (i.e., this remains 5 years as per the above)
#     (f.col('lookback_filter') == 'none')
#     # 6-month lookback filter
#     | ((f.col('lookback_filter') == '6m') & (f.col('DATE') >= f.col('CENSOR_DATE_END_minus_6m')))
#   )
# )

# # check
# assert df_all_filtered_1.select('name').distinct().count() == df_all_filtered_2.select('name').distinct().count() # check no names have been dropped
# tmpt = tabstat(df_all_filtered_2, 'fu', byvar=['lookback_filter', 'name']); print()

# COMMAND ----------

# MAGIC %md ## 5.4. Add selection group

# COMMAND ----------

# add a selection group to allow prioritisation of the different time periods around the study start date
# 1: -0.5 <= x < 0 (half a year before)
# 2: 0 <= x < +0.5 (half a year after)
# 3: -5 <= x < -0.5 (five years before)
# where x is the difference in years from the DATE to the study start date

# updated 20230620
# 1: -5 <= x < 0 (five years before)
# 2: 0 <= x < +0.5 (half a year after)

# check
print(f'study_start_date = {study_start_date}')

# create selection group
df_all_4 = (
  df_all_3
  .withColumn('study_start_date', f.to_date(f.lit(study_start_date)))
  # .withColumn('study_start_date_minus_6m', f.add_months(f.col('study_start_date'), -6))
  # .withColumn('selection_group',
  #            f.when((f.col('DATE') >= f.col('study_start_date_minus_6m')) & (f.col('DATE') < f.col('study_start_date')), 1)
  #            .when((f.col('DATE')  >= f.col('study_start_date')) & (f.col('DATE') < f.col('CENSOR_DATE_END')), 2)
  #            .when((f.col('DATE')  >= f.col('CENSOR_DATE_START')) & (f.col('DATE') < f.col('study_start_date_minus_6m')), 3)
  #            .otherwise(999999))
  # updated 20230620
  .withColumn('selection_group',
             f.when((f.col('DATE')  >= f.col('CENSOR_DATE_START')) & (f.col('DATE') < f.col('study_start_date')), 1)
             .when((f.col('DATE')  >= f.col('study_start_date')) & (f.col('DATE') < f.col('CENSOR_DATE_END')), 2)
             .otherwise(999999))  
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('study_start_date'))/365.25)
  .withColumn('diff_abs', f.abs(f.col('diff')))
)

# temp save
#df_all_4 = temp_save(df=df_all_4, out_name=f'{proj}_tmp_covariates_markers_df_all_4'); print() 

# check 
tmpt = tabstat(df_all_4, 'DATE', byvar='selection_group', date=1); print()
tmpt = tabstat(df_all_4, 'diff', byvar='selection_group'); print()
tmpt = tabstat(df_all_4, 'diff_abs', byvar='selection_group'); print()
tmpt = tab(df_all_4, 'name', 'selection_group'); print()
tmpt = tabstat(df_all_4, 'VALUE1_CONDITION', byvar='name'); print()


# COMMAND ----------

# MAGIC %md ## 5.5. Check

# COMMAND ----------

#df_all_4 = spark.table(f'{dsa}.{proj}_tmp_covariates_markers_df_all_4')
#tmpt = tabstat(df_all_4, 'VALUE1_CONDITION', byvar='name'); print()

# COMMAND ----------

# check
display(df_all_4.orderBy('PERSON_ID', 'DATE', 'name', 'source', 'code'))

# COMMAND ----------

# MAGIC %md # 6. Selection

# COMMAND ----------

# MAGIC %md ## 6.1 Dense rank

# COMMAND ----------

# add a dense rank - this can be used to keep multiple distinct values on the same DATE
win_denserank = (
  Window
  .partitionBy('PERSON_ID', 'name')
  .orderBy('selection_group', 'diff_abs')  
)
df_all_5 = (
  df_all_4
  .withColumn('denserank', f.dense_rank().over(win_denserank)) 
)  

# COMMAND ----------

# check
display(df_all_5.orderBy('PERSON_ID', 'name', 'DATE'))

# COMMAND ----------

win_rownum = (
  Window
  .partitionBy('PERSON_ID', 'name')
  .orderBy('selection_group', 'diff_abs', 'mono_id')  
)
win_rownummax = (
  Window
  .partitionBy('PERSON_ID', 'name', 'DATE')
)

# filter
df_all_6 = (
  df_all_5
  .where(f.col('denserank') == 1)
  .withColumn('rownum', f.row_number().over(win_rownum))
  .withColumn('rownummax', f.count(f.lit(1)).over(win_rownummax))
) 

# temp save
#df_all_6 = temp_save(df=df_all_6, out_name=f'{proj}_tmp_covariates_markers_df_all_6'); print() 

# check
count_varlist(df_all_6, ['PERSON_ID', 'name'])
tmpt = tab(df_all_6, 'name'); print()
tmpt = tab(df_all_6, 'rownum', 'name'); print()
tmpt = tab(df_all_6.where(f.col('rownum') == 1), 'rownummax', 'name'); print()
tmpt = tabstat(df_all_6, 'VALUE1_CONDITION', byvar='name'); print()

# COMMAND ----------

 # check
display(df_all_6.where(f.col('rownummax') > 1).where(f.col('name') == 'bmi').orderBy('PERSON_ID', 'DATE'))

# COMMAND ----------

 # check
display(df_all_6.where(f.col('rownummax') > 1).where(f.col('name') != 'bmi').orderBy('PERSON_ID', 'name', 'DATE'))

# COMMAND ----------

# MAGIC %md ## 6.2 BMI rounding

# COMMAND ----------

# prioritise most precise value - being aware that there may be multiple distinct values with the same precision - 68.53 71.39 
# order the most precise value first and remove rounded values below - distinct equally precise values will not be removed and reported

# COMMAND ----------

# non-BMI
df_all_6_nonbmi = (
  df_all_6
  .where(f.col('name') != 'bmi')
)

# BMI
df_all_6_bmi = (
  df_all_6
  .where(f.col('name') == 'bmi')
)

# BMI - 1 row
df_all_6_bmi_1 = (
  df_all_6_bmi
  .where(f.col('rownummax') == 1)
)  

# BMI - multi row
df_all_6_bmi_m = (
  df_all_6_bmi
  .where(f.col('rownummax') > 1)
)    

# COMMAND ----------

# identify number of decimal places a value has been provided to
df_all_6_bmi_m_1 = (
  df_all_6_bmi_m
  .withColumn('VALUE1_CONDITION_str', f.col('VALUE1_CONDITION').cast(t.StringType()))
  .withColumn('check', f.when(f.col('VALUE1_CONDITION_str').rlike(r'^(\d)?\d\d\.\d\d$'), 1).otherwise(0))  
  .withColumn('dp1', f.regexp_extract(f.col('VALUE1_CONDITION_str'), r'^(\d)?\d\d\.(\d)(\d)$', 2)) 
  .withColumn('dp2', f.regexp_extract(f.col('VALUE1_CONDITION_str'), r'^(\d)?\d\d\.(\d)(\d)$', 3))
  .withColumn('dp', 
              f.when(f.col('dp2') != '0', 2)
              .when(f.col('dp1') != '0', 1)
              .otherwise(0)
             )
)

# check
tmpt = tab(df_all_6_bmi_m_1, 'rownum'); print()
tmpt = tab(df_all_6_bmi_m_1, 'check'); print()
tmpt = tab(df_all_6_bmi_m_1, 'dp1', 'dp2'); print()
tmpt = tab(df_all_6_bmi_m_1, 'dp'); print()
tmpt = tab(df_all_6_bmi_m_1, 'dp2', 'dp'); print()
tmpt = tab(df_all_6_bmi_m_1.where(f.col('dp2') == '0'), 'dp1', 'dp'); print()

# COMMAND ----------

# check
display(df_all_6_bmi_m_1.orderBy('PERSON_ID', 'DATE', f.desc('dp')))

# COMMAND ----------

# flag records to drop
win = Window\
  .partitionBy('PERSON_ID', 'DATE')\
  .orderBy(f.desc('dp'), 'mono_id')
win_egen = Window\
  .partitionBy('PERSON_ID', 'DATE')\
  .orderBy(f.desc('dp'), 'mono_id')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_all_6_bmi_m_2 = (
  df_all_6_bmi_m_1
  .withColumn('rownum', f.row_number().over(win))
  .withColumn('rownum1_round1', f.when((f.col('rownum') == 1) & (f.col('dp') == 2), f.round(f.col('VALUE1_CONDITION'), 1)).otherwise(None))
  .withColumn('rownum1_round0', f.when((f.col('rownum') == 1) & (f.col('dp').isin([1,2])), f.round(f.col('VALUE1_CONDITION'), 0)).otherwise(None))
  .withColumn('rownum1_round1_egen', f.min(f.col('rownum1_round1')).over(win_egen))
  .withColumn('rownum1_round0_egen', f.min(f.col('rownum1_round0')).over(win_egen))
  .withColumn('to_drop', 
              f.when(
                (f.col('VALUE1_CONDITION') == f.col('rownum1_round1_egen')) 
                | (f.col('VALUE1_CONDITION') == f.col('rownum1_round0_egen'))
                , 1)
              .otherwise(0))
)  
  
# check
tmpt = tab(df_all_6_bmi_m_2, 'rownum', 'to_drop'); print()

# COMMAND ----------

# check  
display(df_all_6_bmi_m_2.orderBy('PERSON_ID', 'DATE', f.desc('dp'), 'mono_id'))

# COMMAND ----------

# check
tmpt = tab(df_all_6_bmi_m_2.where(f.col('rownum') == 1), 'rownummax'); print()
tmpt = tab(df_all_6_bmi_m_2, 'rownum', 'to_drop'); print()

# filter
df_all_6_bmi_m_3 = (
  df_all_6_bmi_m_2
  .where(f.col('to_drop') == 0)
  .withColumn('rownum', f.row_number().over(win))
  .withColumn('rownummax', f.count(f.lit(1)).over(win_rownummax))
)

# check
tmpt = tab(df_all_6_bmi_m_3, 'rownum', 'to_drop'); print()

# tidy
win_rownum = (
  Window
  .partitionBy('PERSON_ID', 'DATE')
  .orderBy(f.desc('dp'), 'mono_id')
)
win_rownummax = (
  Window
  .partitionBy('PERSON_ID', 'DATE')
)
df_all_6_bmi_m_4 = (
  df_all_6_bmi_m_3
  .drop('to_drop')
  .withColumn('rownum', f.row_number().over(win_rownum))
  .withColumn('rownummax', f.count(f.lit(1)).over(win_rownummax))
)

# check
tmpt = tab(df_all_6_bmi_m_4.where(f.col('rownum') == 1), 'rownummax'); print()

# COMMAND ----------

# check
display(df_all_6_bmi_m_4.where(f.col('rownummax') > 1).orderBy('PERSON_ID', 'DATE', f.desc('dp'), 'mono_id'))

# COMMAND ----------

# tidy
df_all_6_bmi_m_5 =(
  df_all_6_bmi_m_4
  .drop('VALUE1_CONDITION_str', 'check', 'dp1', 'dp2', 'dp', 'rownum1_round1', 'rownum1_round0', 'rownum1_round1_egen', 'rownum1_round0_egen')
)

# reassemble
df_all_7 = (
  df_all_6_nonbmi
  .unionByName(df_all_6_bmi_1)
  .unionByName(df_all_6_bmi_m_5)
)

# temp save
#df_all_7 = temp_save(df=df_all_7, out_name=f'{proj}_tmp_covariates_markers_df_all_7'); print() 

# COMMAND ----------

# MAGIC %md ## 6.3 Row number

# COMMAND ----------

win_rownum = (
  Window
  .partitionBy('PERSON_ID', 'name')
  .orderBy('selection_group', 'diff_abs', 'mono_id')  
)
win_rownummax = (
  Window
  .partitionBy('PERSON_ID', 'name', 'selection_group', 'diff_abs')
)
win_rownummax_2 = (
  Window
  .partitionBy('PERSON_ID', 'name', 'DATE')
)
df_all_8 = (
  df_all_7
  .withColumn('rownum', f.row_number().over(win_rownum))
  .withColumn('rownummax', f.count(f.lit(1)).over(win_rownummax))
  .withColumn('rownummax_2', f.count(f.lit(1)).over(win_rownummax_2))
  .withColumn('value_list', f.sort_array(f.collect_list(f.col('VALUE1_CONDITION')).over(win_rownummax)))
)   

# check
tmpt = tab(df_all_8, 'rownummax', 'rownummax_2'); print()
tmpt = tab(df_all_8.where(f.col('rownum') == 1), 'rownummax', 'name'); print()

# COMMAND ----------

# check
display(df_all_8.where(f.col('rownummax') > 1).orderBy('PERSON_ID', 'name', 'rownum'))

# COMMAND ----------

# check
tmpt = tab(df_all_8, 'rownum', 'name'); print()

# filter to row number equals 1 
df_all_9 = (
  df_all_8  
  .where(f.col('rownum') == 1)
)

# check
tmpt = tab(df_all_9, 'rownum', 'name'); print()
  
# tidy
df_all_10 = (
  df_all_9  
  .drop('rownum', 'rownummax_2')
  .withColumnRenamed('rownummax', 'n_distinct_values_on_date')
  .withColumn('flag_multi_distinct_values_on_date', f.when(f.col('n_distinct_values_on_date') > 1, 1).otherwise(0))
)

# temp save
#df_all_10 = temp_save(df=df_all_10, out_name=f'{proj}_tmp_covariates_markers_df_all_10'); print() 

# check
count_varlist(df_all_10, ['PERSON_ID', 'name'])
tmpt = tab(df_all_10, 'n_distinct_values_on_date', 'flag_multi_distinct_values_on_date'); print()
tmpt = tab(df_all_10, 'name', 'flag_multi_distinct_values_on_date'); print()

# min, max, and look at diff of ties, diff between 0 dp rounded values

# COMMAND ----------

# check
display(df_all_10.orderBy('PERSON_ID', 'DATE', 'name'))

# COMMAND ----------

# MAGIC %md ## 6.4 SBP check 

# COMMAND ----------

#df_all_10 = spark.table(f'{dsa}.{proj}_tmp_covariates_markers_df_all_10')
tmpt = tabstat(df_all_10, 'VALUE1_CONDITION', byvar='name'); print()

# COMMAND ----------

tmp1 = (
  df_all_10
  .where(f.col('name') == 'sbp')
  .withColumn('check', 
              f.when(f.col('VALUE2_CONDITION').isNull(), 0)
              .when((f.col('VALUE2_CONDITION').isNotNull()) & (f.col('VALUE1_CONDITION') > f.col('VALUE2_CONDITION')), 1)
              .when((f.col('VALUE2_CONDITION').isNotNull()) & (f.col('VALUE1_CONDITION') <= f.col('VALUE2_CONDITION')), 2)
              .otherwise(0)
             )
)
tmpt = tab(tmp1, 'check'); print()
tmpt = tabstat(tmp1, 'VALUE1_CONDITION', byvar='check'); print()
tmpt = tabstat(tmp1, 'VALUE2_CONDITION', byvar='check'); print()

# COMMAND ----------

# MAGIC %md # 7. Check

# COMMAND ----------

# summarise monthly
tmp1 = (
  df_all_5
  .withColumn('date_ym', f.date_format(f.col('DATE'), 'yyyy-MM'))
  .groupBy('name', 'date_ym', 'selection_group')
  .agg(f.count(f.lit(1)).alias('n'))  
  .withColumn('stage', f.lit('pre-selection'))
  .orderBy('name', 'date_ym', 'selection_group')
)
tmp2 = (
  df_all_10
  .withColumn('date_ym', f.date_format(f.col('DATE'), 'yyyy-MM'))
  .groupBy('name', 'date_ym', 'selection_group')
  .agg(f.count(f.lit(1)).alias('n'))  
  .withColumn('stage', f.lit('post-selection'))
  .orderBy('name', 'date_ym', 'selection_group')
)
tmp3 = (
  tmp1
  .unionByName(tmp2)
  .orderBy('stage', 'name', 'date_ym', 'selection_group')
)
tmpp = tmp3.toPandas()

# master date_ym index'
tmp4 = (
  tmp1
  .select('date_ym')
  .groupBy('date_ym')
  .agg(f.count(f.lit(1)).alias('_tmp'))
  .toPandas()
)

# COMMAND ----------

# check final
display(df_all_10.orderBy('PERSON_ID', 'name'))

# COMMAND ----------

# MAGIC %md # 8. Prepare

# COMMAND ----------

# MAGIC %md ## 8.1. Reshape

# COMMAND ----------

# check
tmpt = tab(df_all_10, 'name'); print()
count_varlist(df_all_10, ['PERSON_ID'])
count_varlist(df_all_10, ['PERSON_ID', 'name'])
tmpt = tabstat(df_all_10, 'VALUE1_CONDITION', byvar='name'); print()

# separate reshape - because here we reshape both date and value
df_all_11 = (
  df_all_10
  .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END', 'name', 'DATE', 'VALUE1_CONDITION', 'flag_multi_distinct_values_on_date')  
  .withColumn('name', f.concat(f.lit(f'cov_'), f.lower(f.col('name'))))
  .groupBy('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END')
  .pivot('name')
  .agg(
    f.min('DATE').alias('date')
    , f.first('VALUE1_CONDITION').alias('value')
  )
  .orderBy('PERSON_ID') 
)

# temp save
#df_all_11 = temp_save(df=df_all_11, out_name=f'{proj}_tmp_covariates_markers_df_all_11'); print() 
#.option("overwriteSchema", "true")

# check
count_varlist(df_all_11, ['PERSON_ID'])
print(len(df_all_11.columns)); print()
print(pd.DataFrame({f'_cols': df_all_11.columns}).to_string()); print()

# COMMAND ----------

#df_all_11 = spark.table(f'{dsa}.{proj}_tmp_covariates_markers_df_all_11')

tmpt = tabstat(df_all_11, 'cov_bmi_value'); print()
tmpt = tabstat(df_all_11, 'cov_creat_value'); print()
tmpt = tabstat(df_all_11, 'cov_egfr_value'); print()
tmpt = tabstat(df_all_11, 'cov_hba1c_value'); print()
tmpt = tabstat(df_all_11, 'cov_hdl_value'); print()
tmpt = tabstat(df_all_11, 'cov_sbp_value'); print()
tmpt = tabstat(df_all_11, 'cov_tchol_value'); print()
tmpt = tabstat(df_all_11, 'cov_height_value'); print()
tmpt = tabstat(df_all_11, 'cov_weight_value'); print()


display(df_all_11.orderBy('PERSON_ID'))

# COMMAND ----------

# MAGIC %md ## 8.2. Add cohort 

# COMMAND ----------

# merge cov_1 and cohort ID
df_all_12 = merge(df_all_11, individual_censor_dates, ['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print()

# check
count_var(df_all_12, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 9. Check

# COMMAND ----------

# check
display(df_all_12.orderBy('PERSON_ID'))

# COMMAND ----------

# MAGIC %md # 10. Save

# COMMAND ----------

save_table(df=df_all_12, out_name=f'{proj}_out_covariates_markers_{cohort}', save_previous=True) #cohortrefactoring