# Databricks notebook source
# MAGIC %md # CCU004_03-D06a-inclusion_exclusion
# MAGIC  
# MAGIC **Description** This notebook applies the inclusion/exclusion criteria.
# MAGIC  
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC  
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D06-inclusion_exclusion
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_tmp_inc_exc_cohort`** : cohort remaining after inclusion/exclusion criteria applied
# MAGIC - **`ccu004_03_tmp_inc_exc_flow`** : flowchart displaying total n of cohort after each inclusion/exclusion rule is applied

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
#%run "../../shds/common/functions"

# COMMAND ----------

# MAGIC %run "/Repos/sjk98@medschl.cam.ac.uk/ccu004_03/functions"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "./CCU004_03-D01-parameters"

# COMMAND ----------

# widgets
dbutils.widgets.removeAll()
dbutils.widgets.text('1 project', proj)
dbutils.widgets.text('2 cohort', cohort)
dbutils.widgets.text('3 pipeline production date', pipeline_production_date)

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

spark.sql(f'REFRESH TABLE {path_tmp_skinny}') #cohortrefactoring
spark.sql(f'REFRESH TABLE {path_cur_deaths_sing}') #cohortrefactoring
spark.sql(f'REFRESH TABLE {dsa}.{proj}_tmp_quality_assurance_{cohort}') #cohortrefactoring


skinny     = spark.table(path_tmp_skinny) #cohortrefactoring
deaths     = spark.table(path_cur_deaths_sing)
qa         = spark.table(f'{dsa}.{proj}_tmp_quality_assurance_{cohort}') #cohortrefactoring
lsoa       = spark.table(path_cur_lsoa) 


#PROJ SPECIFIC
# sgss     = extract_batch_from_archive(parameters_df_datasets, 'sgss')

# COMMAND ----------

display(skinny)

# COMMAND ----------

display(deaths)

# COMMAND ----------

#display(qa)

# COMMAND ----------

display(lsoa)

# COMMAND ----------

display(tab(lsoa,"region"))

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('skinny')
print('---------------------------------------------------------------------------------')
# reduce
skinny_prepared = skinny.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'in_gdppr')
# skinny_prepared = skinny.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'in_gdppr', 'LSOA', 'region', 'IMD_2019_DECILES')

# check
count_var(skinny_prepared, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('lsoa')
print('---------------------------------------------------------------------------------')
# reduce
lsoa_prepared = (
  lsoa
  .select('PERSON_ID', 'LSOA_date', 'LSOA_conflict', 'LSOA', 'region_conflict', 'region', 
          f.col("imd_deciles_conflict").alias('IMD_conflict'),
          'IMD_2019_DECILES')
  .withColumn('in_lsoa', f.lit(1))
  .withColumn('LSOA_1', f.substring(f.col('LSOA') , 1, 1))
)

# check
count_var(lsoa_prepared, 'PERSON_ID'); print()
tmpt = tab(lsoa_prepared, 'region', 'LSOA_1'); print()
tmpt = tab(lsoa_prepared.where(f.col('LSOA_1') == 'E'), 'region', 'LSOA_conflict'); print()


print('---------------------------------------------------------------------------------')
print('deaths')
print('---------------------------------------------------------------------------------')
# reduce
deaths_prepared = (
  deaths
  .select('PERSON_ID', f.col('REG_DATE_OF_DEATH').alias('DOD'))
  .withColumn('in_deaths', f.lit(1))
)

# check
count_var(deaths_prepared, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('quality assurance')
print('---------------------------------------------------------------------------------')
qa_prepared = (
  qa
  .withColumn('in_qa', f.lit(1))
)

# check
count_var(qa_prepared, 'PERSON_ID'); print()
tmpt = tab(qa_prepared, '_rule_concat', '_rule_total', var2_unstyled=1); print()


print('---------------------------------------------------------------------------------')
print('sgss')
print('---------------------------------------------------------------------------------')
# not required for this project



print('---------------------------------------------------------------------------------')
print('merged')
print('---------------------------------------------------------------------------------')

# merge skinny and lsoa
# _merged = merge(skinny_prepared, lsoa_prepared, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], keep_results=['both', 'left_only'], indicator=0); print() 
_merged = merge(skinny_prepared, lsoa_prepared, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# merge in deaths
_merged = merge(_merged, deaths_prepared, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# # merge skinny and deaths
# _merged = merge(skinny_prepared, deaths_prepared, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# merge in qa
_merged = merge(_merged, qa_prepared, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()

# add baseline_date (study start date)
_merged = _merged.withColumn('baseline_date', f.to_date(f.lit(study_start_date)))

# merge in sgss - not required for this project
# _merged = (
#   merge(_merged, _sgss, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'])
#   .withColumn('in_sgss', f.when(f.col('_merge') == 'both', 1).otherwise(0))
#   .drop('_merge')); print() 

# COMMAND ----------

# check
display(_merged)

# COMMAND ----------

# temp save
_merged = temp_save(df=_merged, out_name=f'{proj}_tmp_inc_exc_merged_{cohort}'); print() #cohortrefactoring

# check
count_var(_merged, 'PERSON_ID'); print()
tmpt = tab(_merged, 'baseline_date'); print()

# COMMAND ----------

# check
tmpt = tab(_merged, 'in_gdppr'); print()
tmpt = tab(_merged, 'in_deaths'); print()
tmpt = tab(_merged, 'in_qa'); print()

tmpt = tab(_merged, '_rule_total'); print()
tmpt = tab(_merged, '_rule_concat', '_rule_total', var2_unstyled=1); print()

# tmpt = tab(_merged, 'in_sgss'); print()

# COMMAND ----------

# MAGIC %md # 3. Inclusion / exclusion

# COMMAND ----------

tmp0 = _merged
tmpc = count_var(tmp0, 'PERSON_ID', ret=1, df_desc='original', indx=0); print()

# COMMAND ----------

# MAGIC %md ## 3.1 Exclude patients aged < 40 at baseline

# COMMAND ----------

# filter out individuals aged < 40 at baseline (keep those >= 40 
# # this statment will remove ~14m individuals with missing DOB from the skinny table who failed the qa

# Include indviduals with 40 <= Age <= 109 as at 1st January 2020

study_start_date_minus_40y = str(int(study_start_date[0:4]) - 40) + study_start_date[4:]
print(f'study_start_date_minus_40y  = {study_start_date_minus_40y}'); print()

tmp0 = (
  tmp0
  .withColumn('_age_ge_40', 
              f.when(f.col('DOB').isNull(), 0)
              .when(f.col('DOB') <= f.to_date(f.lit(study_start_date_minus_40y)), 1)
              .when(f.col('DOB') >  f.to_date(f.lit(study_start_date_minus_40y)), 2)
              .otherwise(999999)
             )
)

# check 
tmpt = tab(tmp0, '_age_ge_40'); print()
assert tmp0.where(~(f.col('_age_ge_40').isin([0,1,2]))).count() == 0
tmpt = tabstat(tmp0, 'DOB', byvar='_age_ge_40', date=1); print()

# filter out and tidy
tmp1 = (
  tmp0
  .where(f.col('_age_ge_40').isin([1]))
  .drop('_age_ge_40')
)

# check
tmpt = count_var(tmp1, 'PERSON_ID', ret=1, df_desc='post exclusion of patients aged < 40 at baseline', indx=1); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.2 Exclude patients aged > 109 at baseline

# COMMAND ----------

# filter out individuals aged > 109 (keep those <= 109 as at study baseline 2020-01-01 

study_start_date_minus_109y = str(int(study_start_date[0:4]) - 109) + study_start_date[4:]
print(f'study_start_date_minus_109y = {study_start_date_minus_109y}'); print()

tmp1 = (
  tmp1
  .withColumn('_age_le_109', 
              f.when(f.col('DOB').isNull(), 0)
              .when(f.col('DOB') >= f.to_date(f.lit(study_start_date_minus_109y)), 1)
              .when(f.col('DOB') <  f.to_date(f.lit(study_start_date_minus_109y)), 2)
              .otherwise(999999)
             )
)

# check 
tmpt = tab(tmp1, '_age_le_109'); print()
assert tmp1.where(~(f.col('_age_le_109').isin([0,1,2]))).count() == 0
tmpt = tabstat(tmp1, 'DOB', byvar='_age_le_109', date=1); print()

# filter out and tidy
tmp2 = (
  tmp1
  .where(f.col('_age_le_109').isin([1]))
  .drop('_age_le_109')
)

# check
tmpt = count_var(tmp2, 'PERSON_ID', ret=1, df_desc='post exclusion of patients aged > 109 at baseline', indx=2); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.3 Exclude patients who died before baseline

# COMMAND ----------

# filter out patients who died before baseline
tmp2 = (
  tmp2
  .withColumn('DOD_flag', 
              f.when(f.col('DOD').isNull(), 1)
              .when(f.col('DOD') <= f.col('baseline_date'), 2)
              .when(f.col('DOD') > f.col('baseline_date'), 3)
              .otherwise(999999)
             )
)

# check 
tmpt = tab(tmp2, 'DOD_flag'); print()
assert tmp2.where(~(f.col('DOD_flag').isin([1,2,3]))).count() == 0
tmpt = tabstat(tmp2, 'DOD', byvar='DOD_flag', date=1); print()

# filter out and tidy
tmp3 = (
  tmp2
  .where(f.col('DOD_flag').isin([1,3]))
  .drop('DOD_flag')
)

# check
tmpt = count_var(tmp3, 'PERSON_ID', ret=1, df_desc='post exlusion of patients who died before baseline', indx=3); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.4 Exclude patients not in GDPPR

# COMMAND ----------

# check
tmpt = tab(tmp3, 'in_gdppr'); print()

# filter out patients not in GDPPR
tmp4 = tmp3.where(f.col('in_gdppr') == 1)

# check
tmpt = count_var(tmp4, 'PERSON_ID', ret=1, df_desc='post exclusion of patients not in GDPPR', indx=4); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

display(tmp4)

# COMMAND ----------

# MAGIC %md ## 3.5 Exclude patients with region outside of England

# COMMAND ----------

# check
# tmpt = tab(tmp4, 'region', 'LSOA_1'); print()

# filter out patients with a region outside England (in Scotland and Wales)
tmp5 = tmp4.where((f.col('region').isNull()) | ((f.col('region').isNotNull()) & (~f.col('region').isin(['Scotland', 'Wales']))))

# check
tmpt = count_var(tmp5, 'PERSON_ID', ret=1, df_desc='post exclusion of patients in Scotland and Wales', indx=5); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.6 Exclude patients who failed the quality assurance

# COMMAND ----------

# check
tmpt = tab(tmp5, '_rule_concat', '_rule_total'); print()

# filter out patients who failed the quality assurance
tmp6 = tmp5.where(f.col('_rule_total') == 0)
  
# temp save for data checks
tmpj = tmp6.where(f.col('_rule_total') > 1)


# COMMAND ----------

tmpj = temp_save(df=tmpj, out_name=f'{proj}_tmp_inc_exc_qa_1_{cohort}'); print() #cohortrefactoring
  
# check
tmpt = count_var(tmp6, 'PERSON_ID', ret=1, df_desc='post exclusion of patients who failed the quality assurance', indx=6); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# %md ## 3.X Exclude patients who have a positive COVID-19 test before baseline

# COMMAND ----------

# filter out patients who had a positive COVID-19 test before baseline
# tmp3 = (
#   tmp3
#   .withColumn('flag_sgss_lt_baseline', f.when(f.col('Lab_Report_date_min') < f.col('baseline_date'), 1).otherwise(0))
#   .withColumn('flag_baseline_gt_20200101', f.when(f.col('baseline_date') > f.to_date(f.lit('2020-01-01')), 1).otherwise(0)))

# # check
# tmpt = tab(tmp3, 'flag_sgss_lt_baseline', 'flag_baseline_gt_20200101', var2_unstyled=1); print()

# # filter out and tidy
# tmp4 = (
#   tmp3
#   .where(f.col('flag_sgss_lt_baseline') == 0)
#   .drop('flag_sgss_lt_baseline', 'flag_baseline_gt_20200101'))

# # check
# tmpt = count_var(tmp4, 'PERSON_ID', ret=1, df_desc='post exclusion of patients with an sgss date before baseline', indx=4); print()
# tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md # 4. Flow diagram

# COMMAND ----------

# check flow table
tmpp = (
  tmpc
  .orderBy('indx')
  .select('indx', 'df_desc', 'n', 'n_id', 'n_id_distinct')
  .withColumnRenamed('df_desc', 'stage')
  .toPandas())
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

# COMMAND ----------

# MAGIC %md # 5. Save

# COMMAND ----------

# MAGIC %md ## 5.1 Cohort

# COMMAND ----------

# tmpf = tmp6.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'DOD', 'baseline_date', 'LSOA_1')
tmpf = tmp6.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'DOD', 'baseline_date', 'LSOA', 'region', 'IMD_2019_DECILES') #skinnyrefactoring

# check
count_var(tmpf, 'PERSON_ID')

# COMMAND ----------

# check 
display(tmpf)

# COMMAND ----------

save_table(df=tmpf, out_name=f'{proj}_tmp_inc_exc_cohort_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------

# # save name
# outName = f'{proj}_tmp_inc_exc_cohort'.lower()

# # save previous version for comparison purposes
# tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
#   .select('tableName')\
#   .where(f.col('tableName') == outName)\
#   .collect()
# if(len(tmpt)>0):
#   _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#   outName_pre = f'{outName}_pre{_datetimenow}'.lower()
#   print(outName_pre)
#   spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
#   spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# # save
# tmpf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md ## 5.2 Flow

# COMMAND ----------

save_table(df=tmpc, out_name=f'{proj}_tmp_inc_exc_flow_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------

# # save name
# outName = f'{proj}_tmp_inc_exc_flow'.lower()

# # save previous version for comparison purposes
# tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
#   .select('tableName')\
#   .where(f.col('tableName') == outName)\
#   .collect()
# if(len(tmpt)>0):
#   _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#   outName_pre = f'{outName}_pre{_datetimenow}'.lower()
#   print(outName_pre)
#   spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
#   spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# # save
# tmpc.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

display(spark.table(f'{dsa}.ccu004_03_tmp_inc_exc_flow_{cohort}')) #cohortrefactoring

# COMMAND ----------

display(tmpc)

# COMMAND ----------

display(spark.table(f'.ccu004_03_tmp_inc_exc_2_flow_c02_pre20241205_134034'))