# Databricks notebook source
# MAGIC %md # CCU004_03-D09-exposures_covid
# MAGIC  
# MAGIC **Description** This notebook creates the exposures, which comprise Covid-19 infection and Covid-19 vaccination.
# MAGIC  
# MAGIC **Authors** Fionna Chalmers, Anna Stevenson and adapted by Spencer Keene and Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01, earlier CCU002 sub-projects and subsequently CCU002_07-D09-exposures
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC UPDATE
# MAGIC **Exposures for analyses on COVID-19 infection:**
# MAGIC <Time in weeks since any COVID-19 exposure:
# MAGIC +ve PCR test Pillar 1 and/or Pillar 2 COVID-19 infection laboratory testing data 
# MAGIC Primary care COVID-19 diagnosis; or
# MAGIC Hospital admission using HES APC & SUS and the ICD-10 code (U07.1).
# MAGIC
# MAGIC Time in weeks since any COVID exposure with hospitalisation:
# MAGIC Hospital admission with COVID-19 in primary position, and
# MAGIC Hospital admission within the first 28 days of COVID-19.
# MAGIC
# MAGIC Time in weeks since any COVID without hospitalisation 
# MAGIC
# MAGIC COVID and no hospitalisation within 28 days  -->
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_out_exposures_covid`** : COVID-19 infection during follow-up for the cohort
# MAGIC - **`ccu004_03_out_exposures_hx_covid`** : exposures for the cohort
# MAGIC - **`ccu004_03_out_exposures_hx_vacc`** : exposures for the cohort

# COMMAND ----------

# MAGIC %md # 0. Set-up

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
from pyspark.sql.functions import expr as expr
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

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU004_03-D01-parameters"

# COMMAND ----------

# This is useful for when working in workflows, if cohort = "c01", we do not want the covid-related notebook to run - this allows the notebook to be skipped
if cohort == "c01":
    dbutils.notebook.exit()

# COMMAND ----------

# widgets
dbutils.widgets.removeAll()
dbutils.widgets.text('1 project', proj)
dbutils.widgets.text('2 cohort', cohort)
dbutils.widgets.text('3 pipeline production date', pipeline_production_date)

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------

spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_cohort_{cohort}') #cohortrefactoring
cohort_dataset = spark.table(f'{dsa}.{proj}_out_cohort_{cohort}') #cohortrefactoring

# covid  = spark.table(f'{dbc}.{proj}_cur_covid')
#spark.sql(f'REFRESH TABLE {dsa}.{proj}_cur_covid')
#covid = spark.table(f'{dsa}.{proj}_cur_covid')

#spark.sql(f'REFRESH TABLE {dsa}.{proj}_cur_hx_covid') #ccu004-03
#hx_covid = spark.table(f'{dsa}.{proj}_cur_hx_covid')
spark.sql(f"""REFRESH TABLE {dsa}.{proj}_cur_covid_vacc_final""")
vacc = spark.table(f'{dsa}.{proj}_cur_covid_vacc_final')

#spark.sql(f"""REFRESH TABLE {dsa}.{proj}_cur_vacc_reshaped_{cohort}""")   #ccu004-03 #cohortrefactoring
#vacc = spark.table(f'{dsa}.{proj}_cur_vacc_reshaped_{cohort}')  #cohortrefactoring

# COMMAND ----------

display(cohort_dataset)

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

## check
#count_var(_covid, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 4. Infection during follow-up

# COMMAND ----------

# MAGIC %md ## 4.1 Check

# COMMAND ----------

## check infection
#count_var(_covid, 'PERSON_ID'); print()
#tmpt = tabstat(_covid, 'DATE', date=1); print()
#tmp1 = _covid.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
#tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()
#tmpt = tab(_covid, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
#tmpt = tab(_covid, 'covid_phenotype', 'source', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 4.2 Prepare

# COMMAND ----------

#print('------------------------------------------------------------------------------')
#print('confirmed COVID-19 (as defined for CCU004_01)')
#print('------------------------------------------------------------------------------')

#covid_confirmed = _covid\
#  .where(\
#    (f.col('covid_phenotype').isin([
#      '01_Covid_positive_test'
#      , '01_GP_covid_diagnosis'
#      , '02_Covid_admission_any_position'
#      , '02_Covid_admission_primary_position'
#    ]))\
#    & (f.col('source').isin(['sgss', 'gdppr', 'hes_apc', 'sus']))\
#    & (f.col('covid_status').isin(['confirmed', '']))\
#  )

## check
#count_var(covid_confirmed, 'PERSON_ID'); print()
#print(covid_confirmed.limit(10).toPandas().to_string(max_colwidth=50)); print()
#tmpt = tabstat(covid_confirmed, 'DATE', date=1); print()
#tmp1 = covid_confirmed.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
#tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()
#tmpt = tab(covid_confirmed, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
#tmpt = tab(covid_confirmed, 'covid_phenotype', 'source', var2_unstyled=1); print()


#print('------------------------------------------------------------------------------')
#print('confirmed COVID-19 admission primary position (i.e., specific hospitalisation for COVID-19; as defined for CCU004_01)')
#print('------------------------------------------------------------------------------')
#covid_confirmed_adm_pri = covid_confirmed\
#  .where(f.col('covid_phenotype') == '02_Covid_admission_primary_position')\
#  .orderBy('PERSON_ID', 'DATE', 'source')

## check
#count_var(covid_confirmed_adm_pri, 'PERSON_ID'); print()
#print(covid_confirmed_adm_pri.limit(10).toPandas().to_string(max_colwidth=50)); print()
#tmpt = tabstat(covid_confirmed_adm_pri, 'DATE', date=1); print()
#tmpt = tab(covid_confirmed_adm_pri, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
#tmpt = tab(covid_confirmed_adm_pri, 'covid_phenotype', 'source', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 4.3 Create

# COMMAND ----------

#print('------------------------------------------------------------------------------')
#print('first (earliest) confirmed covid infection')
#print('------------------------------------------------------------------------------')
## window for row number
#_win_rownum = Window\
#  .partitionBy('PERSON_ID')\
#  .orderBy('date', 'covid_phenotype', 'source')


## filter to first (earliest) confirmed covid infection
## note: ignore ties in covid_phenotype for now
#covid_confirmed_1st = covid_confirmed\
#  .withColumn('_rownum', f.row_number().over(_win_rownum))\
#  .where(f.col('_rownum') == 1)\
#  .withColumnRenamed('DATE', 'exp_covid_1st_date')\
#  .orderBy('PERSON_ID')

## check
#count_var(covid_confirmed_1st, 'PERSON_ID'); print()
#tmpt = tabstat(covid_confirmed_1st, 'exp_covid_1st_date', date=1); print()
#tmpt = tab(covid_confirmed_1st, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
#tmpt = tab(covid_confirmed_1st, 'covid_phenotype', 'source', var2_unstyled=1); print()

## reduce
#covid_confirmed_1st = covid_confirmed_1st\
#  .select('PERSON_ID', 'exp_covid_1st_date', f.col('covid_phenotype').alias('exp_covid_1st_phenotype'))


#print('------------------------------------------------------------------------------')
#print('severity of the first (earliest) confirmed covid infection (hospitalised within 28 days)')
#print('------------------------------------------------------------------------------')
## inner join first (earliest) confirmed covid infection table to the hospitalisations table  
## filter to hospitalisations on or after the date of first (earliest) confirmed covid infection
## filter to first (earliest) hospitalisation
## calculate the number of days from the date of first (earliest) confirmed covid infection to first (earliest) #hospitalisation
## flag where this was within 28 days
#covid_confirmed_severity = covid_confirmed_adm_pri\
#  .join(covid_confirmed_1st.select('PERSON_ID', 'exp_covid_1st_date'), on='PERSON_ID', how='inner')\
#  .where(f.col('DATE') >= f.col('exp_covid_1st_date'))\
#  .withColumn('_rownum', f.row_number().over(_win_rownum))\
#  .where(f.col('_rownum') == 1)\
#  .withColumnRenamed('DATE', 'exp_covid_adm_date')\
#  .withColumn('exp_covid_adm_days', f.datediff(f.col('exp_covid_adm_date'), f.col('exp_covid_1st_date')))\
#  .withColumn('exp_covid_adm_days_le_28',\
#    f.when(f.col('exp_covid_adm_days') <= 28, 1)\
#    .when(f.col('exp_covid_adm_days') > 28, 0)\
#  )\
#  .select('PERSON_ID', 'exp_covid_adm_date', 'exp_covid_adm_days', 'exp_covid_adm_days_le_28')\
#  .orderBy('PERSON_ID')

## check
#count_var(covid_confirmed_severity, 'PERSON_ID'); print()
#tmpt = tabstat(covid_confirmed_severity, 'exp_covid_adm_date', date=1); print()
#tmpt = tab(covid_confirmed_severity, 'exp_covid_adm_days_le_28'); print()
#tmpt = tabstat(covid_confirmed_severity, var='exp_covid_adm_days', byvar='exp_covid_adm_days_le_28'); print()
#tmpt = tab(covid_confirmed_severity, 'exp_covid_adm_days', 'exp_covid_adm_days_le_28', var2_unstyled=1); print()


#print('------------------------------------------------------------------------------')
#print('merge')
#print('------------------------------------------------------------------------------')
#covid_confirmed_1st = merge(covid_confirmed_1st, covid_confirmed_severity, ['PERSON_ID'], validate='1:1', #assert_results=['both', 'left_only'], indicator=0); print()
  
## check
#count_var(covid_confirmed_1st, 'PERSON_ID'); print()
#print(covid_confirmed_1st.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 4.4 Check

# COMMAND ----------

# check 
#display(covid_confirmed_1st)

# COMMAND ----------

# MAGIC %md # 5. Vaccination

# COMMAND ----------

# MAGIC %md ## 5.1 Check

# COMMAND ----------

# check
display(vacc)

# COMMAND ----------

#vacc1 = vacc\
#    .withColumn('num_doses_recent', f.when(f.col('dose_sequence_max') > 4, 4).otherwise(f.col('dose_sequence_max')))
_win = Window\
  .partitionBy(['PERSON_ID'])\
  .orderBy(f.desc('dose_sequence'))

##f.lit('2022-01-01'))\
vacc1 = vacc\
  .where(f.col('DATE') <= f.lit(study_start_date))\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .drop('_rownum')\
  .drop('row_number')\
  .withColumnRenamed("dose_sequence", "max_dose_sequence")\
  .withColumn('max_dose_sequence2', 
              f.when((f.col('max_dose_sequence')==1) & (f.col('PROCEDURE_CAT') == "B"), f.col('max_dose_sequence')+2)
              .when((f.col('max_dose_sequence')==2) & (f.col('PROCEDURE_CAT') == "B"), f.col('max_dose_sequence')+1)
              .otherwise(f.col('max_dose_sequence')))   

display(vacc1)
# Other vaccination variable such as 'fully_vaccinated' (depends on age group) or 'num_dose_start' (N dose at study start) will be created in R on the combined dataset
# fully vaccianted: 5-11 :1 vaccine, 12-15: 2 vaccines, 16-74:3 vaccines, 75+, 4 vaccines

# COMMAND ----------

vacc1 = vacc1.select("PERSON_ID", "max_dose_sequence2").withColumnRenamed("max_dose_sequence2", "num_vaccinations")
display(vacc1)

# COMMAND ----------

## rename columns  i.e. date_1 -> date_vacc_1
#dict_rename = {}
#for col in vacc1.columns:
#  col_rematch_date = re.match(r'(date)\_(\d)', col)
#  if(col_rematch_date):
#    dict_rename[col] = col_rematch_date.group(1) + '_vacc_' + col_rematch_date.group(2)
    
#vacc2 = rename_columns(vacc1, dict_rename); print()

# COMMAND ----------

## rename columns i.e. type_1 -> vacc_type_1
#dict_rename = {}
#for col in vacc2.columns:
#  col_rematch_type = re.match(r'(type)\_(\d)', col)
#  if(col_rematch_type):
#    dict_rename[col] = 'vacc_' + col_rematch_type.group(1) + '_' + col_rematch_type.group(2)
#tmp = rename_columns(vacc2, dict_rename); print()

# COMMAND ----------

#tmpt = tab(tmp,'date_vacc_1'); print()

# COMMAND ----------

# MAGIC %md ## 5.2 Prepare

# COMMAND ----------

#count_var(tmp, 'PERSON_ID'); print()
#tmpt = tab(tmp, 'num_doses_recent'); print()
#tmpt = tab(tmp, 'dose_sequence_max'); print()
#tmpt = tab(tmp, 'num_doses_recent','dose_sequence_max'); print()

# COMMAND ----------

#vacc2 = tmp.select('PERSON_ID', 'date_vacc_1','date_vacc_2','date_vacc_3','date_vacc_4','date_vacc_5','date_vacc_6','num_doses_recent', 'dose_sequence_max', 'PRODUCT_1', 'PRODUCT_2', 'PRODUCT_3', 'PRODUCT_4', 'PRODUCT_5', 'PRODUCT_6')

#display(vacc2)

# COMMAND ----------

# MAGIC %md ## 5.3 Create

# COMMAND ----------

_cohort = cohort_dataset\
  .select('PERSON_ID', 'study_start_date')

display(_cohort)

# COMMAND ----------

#ccu004-03

#vacc3 = vacc2\
#    .withColumn('vacc1_after_start', f.when(f.col('date_vacc_1') > study_start_date , 1).otherwise(0))\
#    .withColumn('vacc2_after_start', f.when(f.col('date_vacc_2') > study_start_date , 1).otherwise(0))\
#    .withColumn('vacc3_after_start', f.when(f.col('date_vacc_3') > study_start_date , 1).otherwise(0))\
#    .withColumn('vacc4_after_start', f.when(f.col('date_vacc_4') > study_start_date , 1).otherwise(0))\
#    .withColumn('vacc5_after_start', f.when(f.col('date_vacc_5') > study_start_date , 1).otherwise(0))\
#    .withColumn('vacc6_after_start', f.when(f.col('date_vacc_6') > study_start_date , 1).otherwise(0))

#columns_to_add = ['vacc1_after_start', 'vacc2_after_start', 'vacc3_after_start', 'vacc4_after_start', 'vacc5_after_start', 'vacc6_after_start']

#sum_expr="+".join(columns_to_add)
#vacc3 = vacc3.withColumn("num_vacc_after_baseline", expr(sum_expr))
#vacc3 = vacc3.withColumn('num_vacc_after_baseline', sum(vacc3[col] for col in vacc3.columns))
#vacc3 = vacc3.withColumn('num_vacc_after_baseline', F.expr('+'.join(cols_to_sum)))

#columns_to_subtract = ['dose_sequence_max', 'num_vacc_after_baseline']

#sub_expr="-".join(columns_to_subtract)
#vacc3 = vacc3.withColumn("num_vacc_before_baseline", expr(sub_expr))

#vacc4 = vacc3\
#    .withColumn('date_vacc_1', f.when(f.col('date_vacc_1') > study_start_date , None).otherwise(f.col('date_vacc_1')))\
#    .withColumn('date_vacc_2', f.when(f.col('date_vacc_2') > study_start_date , None).otherwise(f.col('date_vacc_2')))\
#    .withColumn('date_vacc_3', f.when(f.col('date_vacc_3') > study_start_date , None).otherwise(f.col('date_vacc_3')))\
#    .withColumn('date_vacc_4', f.when(f.col('date_vacc_4') > study_start_date , None).otherwise(f.col('date_vacc_4')))\
#    .withColumn('date_vacc_5', f.when(f.col('date_vacc_5') > study_start_date , None).otherwise(f.col('date_vacc_5')))\
#    .withColumn('date_vacc_6', f.when(f.col('date_vacc_6') > study_start_date , None).otherwise(f.col('date_vacc_6')))


#vacc4 = vacc4.select('PERSON_ID', 'num_vacc_before_baseline', 'date_vacc_1', 'date_vacc_2', 'date_vacc_3', 'date_vacc_4', 'date_vacc_5', 'date_vacc_6', 'PRODUCT_1', 'PRODUCT_2', 'PRODUCT_3', 'PRODUCT_4', 'PRODUCT_5', 'PRODUCT_6')

#display(vacc4) 


# COMMAND ----------

#vacc4.agg({'num_vacc_before_baseline': 'max'}).show()

# COMMAND ----------

#vacc_confirmed_last = vacc4.select('PERSON_ID', 'num_vacc_before_baseline')
# check
#count_var(vacc_confirmed_last, 'PERSON_ID'); print()


# COMMAND ----------

#print('---------------------------------------------------------------------------------')
#print('merge with cohort')
#print('---------------------------------------------------------------------------------')
#_cohort = cohort\
#  .select('PERSON_ID', 'study_start_date')

#vacc_confirmed_last = vacc4.select('PERSON_ID', 'num_vacc_before_baseline')

# merge vacc and cohort - changed keep_results to assert_results and cohort to _cohort and fixed brackets
#vacc_confirmed_last = merge(vacc4, _cohort, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()

# check
# assert tmp9.where(f.col('_merge') == 'left_only').count() == 0
# may be some left_only i.e., those who have been excluded in inclusion/exclusn notebook

# tidy - changed '= tmp2' to '= tmp'
#tmp2 = tmp\ 
#  .where(f.col('_merge') == 'both')\
#  .drop('_merge')

# check
#tmpt = count_var(vacc_confirmed_last, 'PERSON_ID', ret=1, df_desc='post merge with cohort', indx=2); print()
#tmpc = tmpt.unionByName(tmpt)

#print('---------------------------------------------------------------------------------')
#print('remove records after start_date') #ccu004-03 used to be follow up end date
#print('---------------------------------------------------------------------------------')
 # AS - this will not work as we have different dates for vaccination now - they are separate variables for vaccination 1, vaccination 2 etc. All vaccinations need to be between start date and follow up date? I think we want vaccinations between 2020 and Jan 2022. Maybe we do this command column by column.
#tmp2 = tmp\
#  .where(f.col('DATE') <= f.col('study_start_date'))   

#tmp2 = tmp\ #ccu004-03 don't need this because columns were excluded
#  .withColumnRenamed('PRODUCT_1', 'vacc_1')\
#  .withColumnRenamed('PRODUCT_2', 'vacc_2')\
#  .withColumnRenamed('PRODUCT_3', 'vacc_3')\
#  .withColumnRenamed('PRODUCT_4', 'vacc_4')\
#  .withColumnRenamed('PRODUCT_5', 'vacc_5')\
#  .withColumnRenamed('PRODUCT_6', 'vacc_6')
#  .withColumnRenamed('num_doses_recent', 'num_vacc')

# ccu004-03; or just leave as study_start_date without f.col()
#.where(f.col('DATE') <= f.col('fu_end_date'))

# check
#tmpt = count_var(vacc_confirmed_last, 'PERSON_ID', ret=1, df_desc='post remove records > study_start_date', indx=5); print()
#tmpc = tmpc.unionByName(tmpt)
#tmpt = tabstat(tmp2, 'DATE_VACC', date=1); print()

# check    
#print(vacc_confirmed_last.limit(10).toPandas().to_string()); print() 

# recheck flow table
#tmpp = tmpc.toPandas()
#tmpp['n'] = tmpp['n'].astype(int)
#tmpp['n_id'] = tmpp['n_id'].astype(int)
#tmpp['n_id_distinct'] = tmpp['n_id_distinct'].astype(int)
#tmpp['diff_n'] = (tmpp['n'] - tmpp['n'].shift(1)).fillna(0).astype(int)
#tmpp['diff_n_id'] = (tmpp['n_id'] - tmpp['n_id'].shift(1)).fillna(0).astype(int)
#tmpp['diff_n_id_distinct'] = (tmpp['n_id_distinct'] - tmpp['n_id_distinct'].shift(1)).fillna(0).astype(int)
#print(tmpp.to_string())

# ease of reference below
#vacc_clean = vacc_confirmed_last

# COMMAND ----------

# MAGIC %md # 6. Save

# COMMAND ----------

# restrict to cohort ccu004-03
vacc2 = merge(vacc1, cohort_dataset.select('PERSON_ID', 'study_start_date'), ['PERSON_ID'], validate='1:1', keep_results=['both', 'right_only'], indicator=0); print()

# check
count_var(vacc2, 'PERSON_ID'); print()

# COMMAND ----------

# check final
display(vacc2)

# COMMAND ----------

save_table(df=vacc2, out_name=f'{proj}_out_exposures_hx_vacc_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------


#_vacc = vacc\
#    .withColumn('num_doses_recent', f.when(f.col('dose_sequence_max') > 5, 5).otherwise(f.col('dose_sequence_max')))
  
## Other vaccination variable such as 'fully_vaccinated' (depends on age group) or 'num_dose_start' (N dose at study start) will be created in R on the combined dataset
#print('---------------------------------------------------------------------------------')
#print('merge with cohort')
#print('---------------------------------------------------------------------------------')
## check
#count_var(_cohort, 'PERSON_ID'); print()

## check
#tmpc = count_var(_vacc, 'PERSON_ID', ret=1, df_desc='original', indx=1); print()

## merge vacc and cohort
#tmp1 = merge(_vacc, _cohort, ['PERSON_ID']); print()

## check
## assert tmp1.where(f.col('_merge') == 'left_only').count() == 0
## may be some left_only i.e., those who have been excluded in inclusion/exclusion notebook

#tmp1 = tmp1\
#  .where(f.col('_merge') == 'both')\
#  .drop('_merge')

## check
#tmpt = count_var(tmp1, 'PERSON_ID', ret=1, df_desc='post merge with cohort', indx=2); print()
#tmpc = tmpc.unionByName(tmpt)

#tmp2 = (
#  tmp1
#	.withColumn('vacc_1', f.when(f.col('date_1') <= f.col('fu_end_date'), 1).otherwise(0))                      #instead of  <= f.col('fu_end_date'  , we need  >= f.col('study_start_date'??
#	.withColumn('vacc_2', f.when(f.col('date_2') <= f.col('fu_end_date'), 2).otherwise(0))
#	.withColumn('vacc_3', f.when(f.col('date_3') <= f.col('fu_end_date'), 3).otherwise(0))
#    .withColumn('vacc_4', f.when(f.col('date_4') <= f.col('fu_end_date'), 4).otherwise(0))
#	.withColumn('vacc_max', f.greatest(f.col('vacc_1'), f.col('vacc_2'), f.col('vacc_3'),f.col('vacc_4')))
#.withColumn('vacc_flag', f.when(f.col('vacc_max') > 0, 1).otherwise(0)) 
#)