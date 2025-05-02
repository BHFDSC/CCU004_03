# Databricks notebook source
# MAGIC %md # CCU004_03-D09-outcomes
# MAGIC
# MAGIC **Description** This notebook creates the exposures and outcomes table.
# MAGIC
# MAGIC **Authors** Fionna Chalmers, Anna Stevenson and adapted by Spencer Keene and Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01, earlier CCU002 sub-projects and CCU003_05-D11-exposures_and_outcomes
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_out_outcomes`** : outcomes for the cohort
# MAGIC - **`ccu004_03_out_outcomes_wide`** : outcomes for the cohort in wide format
# MAGIC - **`ccu004_03_out_outcomes_noncvddeath`** : non-CVD death competing risk outcome for the cohort
# MAGIC - **`ccu004_03_out_outcomes_wide_noncvddeath`** : non-CVD death competing risk outcomes for the cohort in wide format

# COMMAND ----------

# MAGIC %md # 0. Set-up

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
from pyspark.sql.functions import desc
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import pyspark.pandas as ps
import numpy as np

#import the pyspark module
from pyspark.sql.functions import col,lit,when

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

spark.sql(f"""REFRESH TABLE {dsa}.{proj}_out_codelist_outcomes""") #cohortrefactoring
fatal_codelist    = spark.table(path_out_codelist_outcomes)
nonfatal_codelist = spark.table(path_out_codelist_cvd)

spark.sql(f"""REFRESH TABLE {dsa}.{proj}_out_cohort_{cohort}""") #cohortrefactoring
cohort_dataset       = spark.table(path_out_cohort)
hes_apc_long = spark.table(path_cur_hes_apc_long)
deaths_long     = spark.table(path_cur_deaths_long)

# COMMAND ----------

display(hes_apc_long)

# COMMAND ----------

display(deaths_long)

# COMMAND ----------

tmp1 = (
  deaths_long
  .where(f.col('DIAG_DIGITS') == 4)
  .where(f.col('CODE').rlike('I6(7|8)'))
)
tmpt = tab(tmp1, 'CODE'); print()

tmp2 = (
  deaths_long
  .where(f.col('DIAG_DIGITS') == 4)
  .where(f.col('CODE').rlike('I25'))
)
tmpt2 = tab(tmp2, 'CODE'); print()

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
# check
print(f'study_end_date = {study_end_date}')
# assert study_end_date == '2023-12-31' #ccu004-03 #cohortrefactoring

individual_censor_dates = (
  cohort_dataset
  .select('PERSON_ID', f.col('study_start_date').alias('CENSOR_DATE_START'))
  .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{study_end_date}'))))

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()


print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')

# filter to primary diagnosis position
# reduce and rename columns
_hes_apc = hes_apc_long\
  .where(f.col('DIAG_POSITION') == 1)\
  .select(['PERSON_ID', 'EPISTART', 'CODE', 'DIAG_POSITION'])\
  .withColumnRenamed('EPISTART', 'DATE')

# check
count_var(_hes_apc, 'PERSON_ID'); print()
tmpt = tab(_hes_apc, 'DIAG_POSITION'); print()

# add individual censor dates
_hes_apc = _hes_apc\
  .drop('DIAG_POSITION')\
  .join(individual_censor_dates, on='PERSON_ID', how='inner')

# check
count_var(_hes_apc, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_hes_apc = _hes_apc\
  .where(\
    (f.col('DATE') > f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

# check
count_var(_hes_apc, 'PERSON_ID'); print()

print('--------------------------------------------------------------------------------------')
print('deaths')
print('--------------------------------------------------------------------------------------')


# reduce
_deaths = deaths_long\
  .where(f.col('DIAG_POSITION') == 'UNDERLYING')\
  .select(['PERSON_ID', 'DATE', 'CODE', 'DIAG_POSITION'])

# check
count_var(_deaths, 'PERSON_ID'); print()
tmpt = tab(_deaths, 'DIAG_POSITION'); print()

# add individual censor dates
_deaths = _deaths\
  .drop('DIAG_POSITION')\
  .join(individual_censor_dates, on='PERSON_ID', how='inner')

# check
count_var(_deaths, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_deaths = _deaths\
  .where(\
    (f.col('DATE') > f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

 # check
count_var(_deaths, 'PERSON_ID'); print()

# print('--------------------------------------------------------------------------------------')
# print('cache')
# print('--------------------------------------------------------------------------------------')
# _hes_apc.cache()
# print(f'_hes_apc {_hes_apc.count():,}')
# _gdppr.cache()
# print(f'_gdppr  {_gdppr.count():,}')

# temp save similar covariates_CVD

# COMMAND ----------

# check non-fatal codelist
tmpt = tab(nonfatal_codelist, 'name' , 'terminology', var2_unstyled=1); print()

# COMMAND ----------

# check fatal codelist
tmpt = tab(fatal_codelist, 'name' , 'terminology', var2_unstyled=1); print()

# COMMAND ----------

#display(nonfatal_codelist)

# COMMAND ----------

#display(fatal_codelist)

# COMMAND ----------

#display(_deaths)

# COMMAND ----------

#display(_hes_apc)

# COMMAND ----------

#_deaths.where(f.col('PERSON_ID')=="").show() #  

#_hes_apc.where(f.col('PERSON_ID')=="").show()

# COMMAND ----------

# MAGIC %md # 4. Create

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
_out_in = {
  'hes_apc': ['_hes_apc', 'nonfatal_codelist',  1]
  , 'deaths':  ['_deaths',  'fatal_codelist',  2]
}

# run codelist match and codelist match summary functions
_out_outcomes, _out_outcomes_1st, _out_outcomes_1st_wide = codelist_match(_out_in, _name_prefix=f'out_')
_out_outcomes_summ_name, _out_outcomes_summ_name_code = codelist_match_summ(_out_in, _out_outcomes)

# COMMAND ----------

#ccu004-03 - need a variable for non-CVD death for competing risk adjustment. codelist_nonmatch and codelist_nonmatch_summ are ccu004-03 specific functions that use anti join rather than inner join

# dictionary - dataset, codelist, and ordering in the event of tied records
_out_in2 = {
#  'hes_apc': ['_hes_apc', 'nonfatal_codelist',  1], #sjk should I comment this out?
  'deaths':  ['_deaths',  'fatal_codelist',  1]
}

# run codelist match and codelist match summary functions
#_out_outcomes_noncvddeath, _out_outcomes_1st_noncvddeath, _out_outcomes_1st_wide_noncvddeath = codelist_nonmatch(_out_in2, _last_event=1, _name_prefix=f'out2_')
_out_outcomes_noncvddeath, _out_outcomes_1st_noncvddeath = codelist_nonmatch(_out_in2, _last_event=1, _name_prefix=f'out2_')


# COMMAND ----------

# MAGIC %md # 5. Check

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Display

# COMMAND ----------

# check result
display(_out_outcomes_1st_wide)

# COMMAND ----------

_out_outcomes_1st.agg({'DATE':'max'}).show()

# COMMAND ----------

# check result
display(_out_outcomes_1st)

# COMMAND ----------

display(_out_outcomes_1st_noncvddeath)

# COMMAND ----------

#check_noncvd_death = _out_outcomes_1st_noncvddeath\
#  .where(f.col('code').rlike('I67'))

#display(check_noncvd_death)  

#check_noncvd_death.groupby("code").count().sort(desc("count")).show() 

#check_noncvd_death.crosstab("code","_rownum").show()

#check I25 & I64 & I68 too


# COMMAND ----------

#this is too check for the excluded codes to see how many there are.

#check_noncvd_death = _out_outcomes_1st_noncvddeath\
#  .where(f.col('code').rlike('I541|I60|I62|I67'))

#display(check_noncvd_death)  

#check_noncvd_death.groupby("code").count().sort(desc("count")).show() 

#check_noncvd_death.crosstab("code","_rownum").show()



# COMMAND ----------

#tmp3 = (
#  _out_outcomes_1st_noncvddeath
#  .where(f.col('code').rlike('I'))
#)
#tmpt3 = tab(tmp3, 'code'); print()

# COMMAND ----------

#_out_outcomes_1st_noncvddeath.groupby("code").count().sort(desc("count")).show(250) 

# COMMAND ----------

_win_rownum = Window\
    .partitionBy('PERSON_ID')\
    .orderBy(f.desc('DATE')) #ccu004-03 replaced with desc

_out_outcomes_1st_noncvddeath_final = _out_outcomes_1st_noncvddeath\
  .select('PERSON_ID', 'DATE', 'code')\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .where(f.col('_rownum') == 1)\
  .select('PERSON_ID', 'DATE', 'code')\
  .withColumnRenamed('DATE', 'NONCVD_DEATH_DATE')\
  .withColumnRenamed('code', 'NONCVD_DEATH_CODE')

display(_out_outcomes_1st_noncvddeath_final)    

# COMMAND ----------

#Below plots exclude Non-CVD death.

# COMMAND ----------

# MAGIC %md ### 5.1.1 Plots - First event - Over follow-up time (years) by data source (stacked)

# COMMAND ----------

# DBTITLE 1,Independent y-axes
_tmp = _out_outcomes_1st\
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START'))/365.25)
_tmpp = _tmp\
  .toPandas()

plt.rcParams.update({'font.size': 8})
rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2.4*rows_of_5), sharex=True) # , sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 2)
names = ['hes_apc', 'deaths']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
  s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
  ax.hist([s1, s2], bins = list(np.linspace(0,3,100)), stacked=True, color=colors, label=names) # normed=True
  ax.set_title(f'{v}')
  ax.set(xlabel='\nFollow-up (years)\n')
  ax.xaxis.set_tick_params(labelbottom=True)
  if(i==0): ax.legend(loc='upper right')
    
#axes[3,1].set_axis_off()
#axes[3,2].set_axis_off()
#axes[3,3].set_axis_off()
#axes[3,4].set_axis_off()
#for i in range(0,3):
#  for j in range(0, 5):
#    axes[i,j].xaxis.set_tick_params(labelbottom=True)
    
plt.tight_layout();
display(fig)

# COMMAND ----------

# DBTITLE 1,Shared y-axes
_tmp = _out_outcomes_1st\
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START'))/365.25)
_tmpp = _tmp\
  .toPandas()

plt.rcParams.update({'font.size': 8})
rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2.4*rows_of_5), sharex=True, sharey=True) #  , dpi=100) # 
 
colors = sns.color_palette("tab10", 2)
names = ['hes_apc', 'deaths']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
  s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
  ax.hist([s1, s2], bins = list(np.linspace(0,3,100)), stacked=True, color=colors, label=names) # normed=True
  ax.set_title(f'{v}')
  ax.set(xlabel='\nFollow-up (years)\n')
  ax.xaxis.set_tick_params(labelbottom=True)
  if(i==0): ax.legend(loc='upper right')
    
#axes[3,1].set_axis_off()
#axes[3,2].set_axis_off()
#axes[3,3].set_axis_off()
#axes[3,4].set_axis_off()
#for i in range(0,3):
#  for j in range(0, 5):
#    axes[i,j].xaxis.set_tick_params(labelbottom=True)
    
plt.tight_layout();
display(fig)

# COMMAND ----------

# MAGIC %md ### 5.1.2 Plots - First event - Over calendar time by data source (stacked)

# COMMAND ----------

# DBTITLE 1,Independent y-axes
_tmpp = _out_outcomes_1st\
  .toPandas()

plt.rcParams.update({'font.size': 8})
rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2.8*rows_of_5), sharex=True) # , sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 2)
names = ['hes_apc', 'deaths']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  
  tmp2d1 = _tmpp[(_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'DATE'])
  s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'DATE'])
  ax.hist([s1, s2], bins=100, stacked=True, color=colors, label=names) # normed=True # bins = list(np.linspace(0,3,100))
  ax.set_title(f'{v}')
  ax.set(xlabel='\nDate\n')
  if(i==0): ax.legend(loc='upper right')
  
  # plt.draw()
  # ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
  ax.xaxis.set_tick_params(rotation=90) # labelbottom=True)
  
#axes[3,1].set_axis_off()
#axes[3,2].set_axis_off()
#axes[3,3].set_axis_off()
#axes[3,4].set_axis_off()
#for i in range(0,3):
#  for j in range(0, 5):
#    axes[i,j].xaxis.set_tick_params(labelbottom=True)


plt.tight_layout();
display(fig)

# COMMAND ----------

# DBTITLE 1,Shared y-axes
_tmpp = _out_outcomes_1st\
  .toPandas()

plt.rcParams.update({'font.size': 8})
rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2.8*rows_of_5), sharex=True, sharey=True) # , sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 2)
names = ['hes_apc', 'deaths']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  
  tmp2d1 = _tmpp[(_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'DATE'])
  s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'DATE'])
  ax.hist([s1, s2], bins=100, stacked=True, color=colors, label=names) # normed=True # bins = list(np.linspace(0,3,100))
  ax.set_title(f'{v}')
  ax.set(xlabel='\nDate\n')
  if(i==0): ax.legend(loc='upper right')
  
  # plt.draw()
  # ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
  ax.xaxis.set_tick_params(rotation=90) # labelbottom=True)
  
#axes[3,1].set_axis_off()
#axes[3,2].set_axis_off()
#axes[3,3].set_axis_off()
#axes[3,4].set_axis_off()
#for i in range(0,3):
#  for j in range(0, 5):
#    axes[i,j].xaxis.set_tick_params(labelbottom=True)

plt.tight_layout();
display(fig)

# COMMAND ----------

# MAGIC %md ### 5.1.3 Plots - First event - Over age at event (years) by data source (stacked)

# COMMAND ----------

# DBTITLE 1,Independent y-axes
# plot age instead of diff
_tmp = (merge(_out_outcomes_1st, cohort_dataset.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
_tmpp = _tmp\
  .toPandas()

plt.rcParams.update({'font.size': 8})
rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2.4*rows_of_5), sharex=True) # , sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 2)
names = ['hes_apc', 'deaths']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
  s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
  ax.hist([s1, s2], bins = list(np.linspace(0,100,400)), stacked=True, color=colors, label=names) # normed=True
  ax.set_title(f'{v}')
  ax.set(xlabel='\nAge (years)\n')
  ax.xaxis.set_tick_params(labelbottom=True)
  if(i==0): ax.legend(loc='upper right')
    
#axes[3,1].set_axis_off()
#axes[3,2].set_axis_off()
#axes[3,3].set_axis_off()
#axes[3,4].set_axis_off()
#for i in range(0,3):
#  for j in range(0, 5):
#    axes[i,j].xaxis.set_tick_params(labelbottom=True)

plt.tight_layout();
display(fig)

# COMMAND ----------

# DBTITLE 1,Shared y-axes
# plot age instead of diff
_tmp = (merge(_out_outcomes_1st, cohort_dataset.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
_tmpp = _tmp\
  .toPandas()

plt.rcParams.update({'font.size': 8})
rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2.4*rows_of_5), sharex=True, sharey=True) # , sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 2)
names = ['hes_apc', 'deaths']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
  s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
  ax.hist([s1, s2], bins = list(np.linspace(0,100,400)), stacked=True, color=colors, label=names) # normed=True
  ax.set_title(f'{v}')
  ax.set(xlabel='\nAge (years)\n')
  ax.xaxis.set_tick_params(labelbottom=True)
  if(i==0): ax.legend(loc='upper right')
    
#axes[3,1].set_axis_off()
#axes[3,2].set_axis_off()
#axes[3,3].set_axis_off()
#axes[3,4].set_axis_off()
#for i in range(0,3):
#  for j in range(0, 5):
#    axes[i,j].xaxis.set_tick_params(labelbottom=True)

plt.tight_layout();
display(fig)

# COMMAND ----------

# MAGIC %md ### 5.1.4 Plots - First event - Over age at event (years) by sex (overlapping)

# COMMAND ----------

# DBTITLE 1,Independent y-axes 
# plot age instead of diff
_tmp = (merge(_out_outcomes_1st, cohort_dataset.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
_tmpp = _tmp\
  .toPandas()

plt.rcParams.update({'font.size': 8})
rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2.4*rows_of_5), sharex=True) # , sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 2)
names = ['Male', 'Female']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'SEX'] == '1'][f'diff'])
  s2 = list(tmp2d1[tmp2d1[f'SEX'] == '2'][f'diff'])
  # ax.hist([s1, s2], bins = list(np.linspace(0,20,100)), stacked=True, color=colors, label=names) # normed=True 
  ax.hist(s1, bins = list(np.linspace(0,100,400)), color=colors[0], label=names[0], alpha=0.5) # normed=True 
  ax.hist(s2, bins = list(np.linspace(0,100,400)), color=colors[1], label=names[1], alpha=0.5) # normed=True 
  ax.set_title(f'{v}')
  ax.set(xlabel='\nAge (years)\n')
  ax.xaxis.set_tick_params(labelbottom=True)
  if(i==0): ax.legend(loc='upper right')
    
#axes[3,1].set_axis_off()
#axes[3,2].set_axis_off()
#axes[3,3].set_axis_off()
#axes[3,4].set_axis_off()
#for i in range(0,3):
#  for j in range(0, 5):
#    axes[i,j].xaxis.set_tick_params(labelbottom=True)
    
plt.tight_layout();
display(fig)

# COMMAND ----------

# DBTITLE 1,Shared y-axes
# plot age instead of diff
_tmp = (merge(_out_outcomes_1st, cohort_dataset.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
_tmpp = _tmp\
  .toPandas()

plt.rcParams.update({'font.size': 8})
rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2.4*rows_of_5), sharex=True, sharey=True) # , sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 2)
names = ['Male', 'Female']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'SEX'] == '1'][f'diff'])
  s2 = list(tmp2d1[tmp2d1[f'SEX'] == '2'][f'diff'])
  # ax.hist([s1, s2], bins = list(np.linspace(0,20,100)), stacked=True, color=colors, label=names) # normed=True
  ax.hist(s1, bins = list(np.linspace(0,100,400)), color=colors[0], label=names[0], alpha=0.5) # normed=True 
  ax.hist(s2, bins = list(np.linspace(0,100,400)), color=colors[1], label=names[1], alpha=0.5) # normed=True 
  ax.set_title(f'{v}')
  ax.set(xlabel='\nAge (years)\n')
  ax.xaxis.set_tick_params(labelbottom=True)
  if(i==0): ax.legend(loc='upper right')
    
#axes[3,1].set_axis_off()
#axes[3,2].set_axis_off()
#axes[3,3].set_axis_off()
#axes[3,4].set_axis_off()
#for i in range(0,3):
#  for j in range(0, 5):
#    axes[i,j].xaxis.set_tick_params(labelbottom=True)
    
plt.tight_layout();
display(fig)

# COMMAND ----------

# MAGIC %md ### 5.1.5 Numerical summaries of plots

# COMMAND ----------

# check numerical summaries 
tmpf = (merge(_out_outcomes_1st, cohort_dataset.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
        .withColumn('diff', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START'))/365.25)
        .withColumn('age', f.datediff(f.col('DATE'), f.col('DOB'))/365.25)
        .withColumn('name_source', f.concat_ws('_', 'name', 'source'))
        .withColumn('name_sex', f.concat_ws('_', 'name', 'SEX'))); print()
tmpt = tabstat(tmpf, 'diff', byvar='name_source'); print()
tmpt = tabstat(tmpf, 'DATE', byvar='name_source', date=1); print()
tmpt = tabstat(tmpf, 'age',  byvar='name_source'); print()
tmpt = tabstat(tmpf, 'age',  byvar='name_sex'); print()

# COMMAND ----------

# MAGIC %md ### 5.1.6 Codelist match summaries

# COMMAND ----------

# check codelist match summary by name and source
display(_out_outcomes_summ_name)

# COMMAND ----------

# check codelist match summary by name, source, and code
display(_out_outcomes_summ_name_code)

# COMMAND ----------

# MAGIC %md # 6. Save

# COMMAND ----------

# save
save_table(df= _out_outcomes_1st, out_name=f'{proj}_out_outcomes_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------

# save wide
save_table(df= _out_outcomes_1st_wide, out_name=f'{proj}_out_outcomes_wide_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------

# save 
save_table(df=_out_outcomes_1st_noncvddeath_final, out_name=f'{proj}_out_outcomes_noncvddeath_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------

# save wide
#save_table(df=_out_outcomes_1st_wide_noncvddeath, out_name=f'{proj}_out_outcomes_wide_noncvddeath', save_previous=True)