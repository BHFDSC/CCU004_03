# Databricks notebook source
# MAGIC %md # CCU004_03-D06b-covariates_CVD
# MAGIC  
# MAGIC **Description** This notebook creates the history of CVD, which are needed for later inclusion/exclusion criteria. 
# MAGIC  
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D07-hx_nonfatal
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_tmp_hx_nonfatal`** : history of each nonfatal group for each person

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

#spark.sql(f'REFRESH TABLE {path_out_codelist_cvd}')
spark.sql(f'REFRESH TABLE {path_out_codelist_cvd_exclusion}') #ccu004-03

spark.sql(f'REFRESH TABLE {path_tmp_inc_exc_cohort}')
spark.sql(f'REFRESH TABLE {path_cur_hes_apc_long}')

#codelist_cvd  = spark.table(path_out_codelist_cvd)
codelist_cvd  = spark.table(path_out_codelist_cvd_exclusion) #ccu004-03

cohort_dataset       = spark.table(path_tmp_inc_exc_cohort)
hes_apc_long = spark.table(path_cur_hes_apc_long)

gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

# COMMAND ----------

# 20230424 TB
# codelist_cvd excludes the following three 4-character codes: I671, I675 and I682; but currently includes the 3-character codes: I67 and I68
# to avoid accidentally matching the three 4-character codes that are to be excluded via their corresponding 3-character code, the I67* and I68* codes will need to be handled separately when codelist matching
# before proceeding, we can check to see if it is actually worthwhile coding this or whether the 3-character codes I67 and I68 can simply be removed from the codelist

#tmp1 = (
#  hes_apc_long
#  .where(f.col('DIAG_DIGITS') == 4)
#  .where(f.col('CODE').rlike('I6(7|8)'))
#)
#tmpt = tab(tmp1, 'CODE'); print()

# the above shows that there are no 3-character I67 or I68 codes within the 4-character column
# together with the fact that all 3-character column codes are equal to the first three characters of the 4-character column
# this means that there are no cases where we only have 3-character codes - we always have codes to 4-characters
# we can safely exclude the 3-character codes I67 and I68 from the codelist
# there is not a situation where only the 3-character code is available and the researcher may want to include this

# add an assert statement to catch any change to the above if the monthly batch is updated in a later version

#assert hes_apc_long.where(f.col('DIAG_DIGITS') == 4).where(f.col('CODE').isin(['I67', 'I68'])).count() == 0

# check it is the same for deaths where we may use the same codelist for outcomes

#deaths_long = spark.table(path_cur_deaths_long)
#tmp2 = (
#  deaths_long
#  .where(f.col('DIAG_DIGITS') == 4)
#  .where(f.col('CODE').rlike('I6(7|8)'))
#)
#tmpt = tab(tmp2, 'CODE'); print()


#assert deaths_long.where(f.col('DIAG_DIGITS') == 4).where(f.col('CODE').isin(['I67', 'I68'])).count() == 0

# COMMAND ----------

## check it is the same for deaths where we may use the same codelist for outcomes
#deaths_long = spark.table(path_cur_deaths_long)
#tmp2 = (
#  deaths_long
#  .where(f.col('DIAG_DIGITS') == 4)
#  .where(f.col('CODE').rlike('I51'))
#)
#tmpt = tab(tmp2, 'CODE'); print()

#assert deaths_long.where(f.col('DIAG_DIGITS') == 4).where(f.col('CODE').isin(['I51'])).count() == 0

#tmp2 = (
#  deaths_long
#  .where(f.col('DIAG_DIGITS') == 3)
#  .where(f.col('CODE').rlike('I51'))
#)
#tmpt = tab(tmp2, 'CODE'); print()

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('_codelist_cvd')
print('--------------------------------------------------------------------------------------')
# check
tmpt = tab(codelist_cvd, 'name', 'terminology', var2_unstyled=1); print()

_codelist_cvd = codelist_cvd
#.where(f.col('name').isin(['AF', 'hypertension', 'hypertension_drugs']))

# check
tmpt = tab(_codelist_cvd, 'name', 'terminology', var2_unstyled=1); print()
print(_codelist_cvd.limit(10).toPandas().to_string()); print()

# COMMAND ----------

display(_codelist_cvd)

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')

# prepare: this converts DOB to CENSOR_DATE_START, and the study start date to CENSOR_DATE_END, displaying the windw of time between birth and study start
print(f'study_start_date = {study_start_date}'); print()
individual_censor_dates = cohort_dataset\
  .select('PERSON_ID', 'DOB')\
  .withColumnRenamed('DOB', 'CENSOR_DATE_START')\
  .withColumn('CENSOR_DATE_END', f.to_date(f.lit(study_start_date)))


# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')

# reduce and rename columns
_gdppr_reduce = gdppr.select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'RECORD_DATE', 'CODE')

# check 1, counts individuals in _gdppr_reduce
count_var(_gdppr_reduce, 'PERSON_ID'); print()

# add individual censor dates
_gdppr_join = _gdppr_reduce.join(individual_censor_dates, on=['PERSON_ID'], how='inner')

# temp save
_gdppr_join = temp_save(df=_gdppr_join, out_name=f'{proj}_tmp_hx_nonfatal_gdppr_join_{cohort}'); print() #cohortrefactoring

# check 2, counts individuals in _gdppr_join, should be the less than the number in _gdppr_reduce as we selected only individuals in individual_censor_dates
count_var(_gdppr_join, 'PERSON_ID')

# check before CENSOR_DATE_END, accounting for nulls and using RECORD_DATE where needed
print("""
# 1 - both DATE and RECORD_DATE are null
# 2 - DATE is null, but RECORD_DATE is not null and RECORD_DATE <= CENSOR_DATE_END
# 3 - DATE is null, but RECORD_DATE is not null and RECORD_DATE > CENSOR_DATE_END
# 4 - DATE is not null and DATE <= CENSOR_DATE_END
# 5 - DATE is not null and DATE > CENSOR_DATE_END
""")
_gdppr_before_end_check = (
  _gdppr_join
  .withColumn('_tmp1',
    f.when((f.col('DATE').isNull()) & (f.col('RECORD_DATE').isNull()), 1)
     .when((f.col('DATE').isNull()) & (f.col('RECORD_DATE').isNotNull()) & (f.col('RECORD_DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNull()) & (f.col('RECORD_DATE').isNotNull()) & (f.col('RECORD_DATE') > f.col('CENSOR_DATE_END')), 3)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 4)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 5)
  )
)
tmpt = tab(_gdppr_before_end_check, '_tmp1'); print()
tmpt = tabstat(_gdppr_before_end_check, 'DATE', byvar='_tmp1', date=1); print()
tmpt = tabstat(_gdppr_before_end_check, 'RECORD_DATE', byvar='_tmp1', date=1); print()

# filter to before CENSOR_DATE_END 
# keep _tmp1 == 2 and 4
# replace DATE with RECORD_DATE where DATE is null
# tidy
_gdppr_before_end = (
  _gdppr_before_end_check
  .where(f.col('_tmp1').isin([2, 4]))
  .withColumn('DATE', f.when(f.col('DATE').isNull(), f.col('RECORD_DATE')).otherwise(f.col('DATE')))
  .drop('_tmp1')
)

# temp save
_gdppr_before_end = temp_save(df=_gdppr_before_end, out_name=f'{proj}_tmp_hx_nonfatal_gdppr_before_end_{cohort}'); print() #cohortrefactoring

# check 3
count_var(_gdppr_before_end, 'PERSON_ID')

# check on or after CENSOR_DATE_START, using RECORD_DATE where needed (often in the case of dummy/erroneous DATE)
# note: nulls were replaced in previous data step
print("""
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START and RECORD_DATE <  CENSOR_DATE_START
# 3 - DATE <  CENSOR_DATE_START and RECORD_DATE >= CENSOR_DATE_START and RECORD_DATE <= CENSOR_DATE_END
# 4 - DATE <  CENSOR_DATE_START and RECORD_DATE >= CENSOR_DATE_START and RECORD_DATE >  CENSOR_DATE_END
# 5 - DATE <  CENSOR_DATE_START and RECORD_DATE is null
""")
_gdppr_after_start_check = (
  _gdppr_before_end
  .withColumn('_tmp2',
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')) & (f.col('RECORD_DATE') <  f.col('CENSOR_DATE_START')), 2)
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')) & (f.col('RECORD_DATE') >= f.col('CENSOR_DATE_START')) & (f.col('RECORD_DATE') <= f.col('CENSOR_DATE_END')), 3)
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')) & (f.col('RECORD_DATE') >= f.col('CENSOR_DATE_START')) & (f.col('RECORD_DATE') >  f.col('CENSOR_DATE_END')), 4)
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')) & (f.col('RECORD_DATE').isNull()), 5)
  )
)
tmpt = tab(_gdppr_after_start_check, '_tmp2'); print()
tmpt = tabstat(_gdppr_after_start_check, 'DATE', byvar='_tmp2', date=1); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1 and 3
# replace DATE with RECORD_DATE where RECORD_DATE is more appropriate
# tidy
_gdppr_after_start = (
  _gdppr_after_start_check
  .where(f.col('_tmp2').isin([1, 3]))
  .withColumn('DATE', f.when(f.col('_tmp2') == 3, f.col('RECORD_DATE')).otherwise(f.col('DATE')))
  .drop('_tmp2', 'RECORD_DATE')
)

# temp save
_gdppr = temp_save(df=_gdppr_after_start, out_name=f'{proj}_tmp_hx_nonfatal_gdppr_{cohort}'); print() #cohortrefactoring

# check
tmpt = tabstat(_gdppr, 'DATE', date=1); print()

# check 4
count_var(_gdppr, 'PERSON_ID'); print()
print(_gdppr.limit(10).toPandas().to_string()); print()

# COMMAND ----------

_gdppr = spark.table(f'{dsa}.{proj}_tmp_hx_nonfatal_gdppr_{cohort}') #cohortrefactoring

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
_hes_apc = hes_apc_long.select(['PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE'])

# check 1
# count_var(_hes_apc, 'PERSON_ID'); print()

# merge in individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
_hes_apc = _hes_apc.join(individual_censor_dates, on=['PERSON_ID'], how='inner')

# check 2
# count_var(_hes_apc, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
print("""
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
""")
_hes_apc = (
  _hes_apc
  .withColumn('_tmp1',
    f.when((f.col('DATE').isNull()), 1)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)
  )
)
tmpt = tab(_hes_apc, '_tmp1'); print()
tmpt = tabstat(_hes_apc, 'DATE', byvar='_tmp1', date=1); print()

# filter to before CENSOR_DATE_END
# keep _tmp1 == 2
# tidy
_hes_apc = (
  _hes_apc
  .where(f.col('_tmp1').isin([2]))
  .drop('_tmp1')
)

# check 3
# count_var(_hes_apc, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
print("""
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
""")
_hes_apc = (
  _hes_apc
  .withColumn('_tmp2',
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)
  )
  .withColumn('_flag_dummy', f.when(f.col('DATE').isin(['1800-01-01', '1801-01-01']), 1).otherwise(0))
  .withColumn('_diff', (f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START')))/365.25)
)
tmpt = tab(_hes_apc, '_tmp2'); print()
tmpt = tabstat(_hes_apc, 'DATE', byvar='_tmp2', date=1); print()
tmpt = tab(_hes_apc, '_tmp2', '_flag_dummy'); print()
tmpt = tabstat(_hes_apc, '_diff', byvar='_flag_dummy'); print()
# add count of number of individuals with a non-dummy record before DOB

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
_hes_apc = (
  _hes_apc
  .where(f.col('_tmp2').isin([1]))
  .drop('_tmp2', '_flag_dummy', '_diff')
)

# temp save
_hes_apc = temp_save(df=_hes_apc, out_name=f'{proj}_tmp_hx_nonfatal_hes_apc_{cohort}'); print() #cohortrefactoring

# check
tmpt = tabstat(_hes_apc, 'DATE', date=1); print()

# check 4
count_var(_hes_apc, 'PERSON_ID'); print()
print(_hes_apc.limit(10).toPandas().to_string()); print()

# COMMAND ----------

_hes_apc = spark.table(f'{dsa}.{proj}_tmp_hx_nonfatal_hes_apc_{cohort}') #cohortrefactoring

# COMMAND ----------

# MAGIC %md # 3 Codelist match

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# check terminologies
tmpt = tab(_codelist_cvd, 'name', 'terminology', var2_unstyled=1); print()
_list_terms = (
  list(_codelist_cvd
    .select('terminology')
    .distinct()
    .toPandas()['terminology']
  )
)
assert set(_list_terms) <= set(['DMD', 'SNOMED', 'ICD10'])

# partition codelist_hx into codelists for gdppr (DMD, SNOMED) and hes_apc (ICD10)
codelist_hx_gdppr = _codelist_cvd.where(f.col('terminology').isin(['DMD', 'SNOMED']))
codelist_hx_hes_apc = _codelist_cvd.where(f.col('terminology').isin(['ICD10']))

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
_hx_in = {
    'gdppr':   ['_gdppr',   'codelist_hx_gdppr', 2]
  , 'hes_apc': ['_hes_apc', 'codelist_hx_hes_apc',  1]
}

# run codelist match and codelist match summary functions
_hx, _hx_1st, _hx_1st_wide = codelist_match(_hx_in, _name_prefix=f'cov_hx_'); print()
_hx_summ_name, _hx_summ_name_code = codelist_match_summ(_hx_in, _hx); print()

# temp save
_hx_all = _hx['all']
_hx_all = temp_save(df=_hx_all, out_name=f'{proj}_tmp_hx_nonfatal_hx_all_{cohort}'); print() #cohortrefactoring
_hx_1st = temp_save(df=_hx_1st, out_name=f'{proj}_tmp_hx_nonfatal_hx_1st_{cohort}'); print() #cohortrefactoring
save_table(df=_hx_1st_wide, out_name=f'{proj}_tmp_hx_nonfatal_hx_1st_wide_{cohort}', save_previous=True) #cohortrefactoring
_hx_1st_wide = temp_save(df=_hx_1st_wide, out_name=f'{proj}_tmp_hx_nonfatal_hx_1st_wide_{cohort}'); print() #ccu004-03 this line results in an error. I need to look at the columns for this table #cohortrefactoring

# COMMAND ----------

#_hx_1st_wide\
#     .withColumnRenamed("cov_hx_peripheral artery disease_flag","cov_hx_peripheral_artery_disease_flag")\
#     .withColumnRenamed("cov_hx_peripheral artery disease_date","cov_hx_peripheral_artery_disease_date")\
#     .show()   

# COMMAND ----------

#save_table(df=_hx_1st_wide, out_name=f'{proj}_tmp_hx_nonfatal_hx_1st_wide', save_previous=True)
#_hx_1st_wide = temp_save(df=_hx_1st_wide, out_name=f'{proj}_tmp_hx_nonfatal_hx_1st_wide'); print()

# COMMAND ----------

# _hx_1st = spark.table(f'{dsa}.{proj}_tmp_hx_nonfatal_hx_1st')
# _hx_1st_wide = spark.table(f'{dsa}.{proj}_tmp_hx_nonfatal_hx_1st_wide')

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

count_var(_hx_1st_wide, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md ### 3.3.0 Display

# COMMAND ----------

# check result
display(_hx_1st_wide.orderBy('PERSON_ID'))

# COMMAND ----------

# MAGIC %md ### 3.3.1 Numerical summaries of plots

# COMMAND ----------

# prepare
_tmp = (
  merge(_hx_1st, cohort_dataset.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'], indicator=0)
  .withColumn('fu', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_END'))/365.25)
  .withColumn('age', f.datediff(f.col('DATE'), f.col('DOB'))/365.25)
  .select('PERSON_ID', 'name', 'source', 'DATE', 'fu', 'age', 'SEX')
)

# COMMAND ----------

# check numerical summaries 
_tmps = (
  _tmp
  .withColumn('name_source', f.concat_ws('_', 'name', 'source'))
  .withColumn('name_sex', f.concat_ws('_', 'name', 'SEX'))
)
tmpt = tabstat(_tmps, 'fu', byvar='name_source'); print()
tmpt = tabstat(_tmps, 'DATE', byvar='name_source', date=1); print()
tmpt = tabstat(_tmps, 'age',  byvar='name_source'); print()
tmpt = tabstat(_tmps, 'age',  byvar='name_sex'); print()

# COMMAND ----------

# plot prepare
_tmpp = _tmp.toPandas()
# _tmpp['DATE'] = pd.to_datetime(_tmpp['DATE']).dt.date

# COMMAND ----------

# plot function
# row_height was 2.4
def plot_hx_1st(df, var, bin_min, bin_max, sharey, stacked, xlabel, byvar='source', row_height=4.8, out_com='out'):
  
  # plot parameters
  plt.rcParams.update({'font.size': 8})
  #rows_of_5 = np.ceil(len(df['name'].drop_duplicates())/5).astype(int)
  #fig, axes = plt.subplots(rows_of_5, 6, figsize=(13,row_height*rows_of_5), sharex=True, sharey=sharey) # , sharey=True , dpi=100) # 
  
  rows_of_5 = np.ceil(len(df['name'].drop_duplicates())/5).astype(int)
  fig, axes = plt.subplots(1, 2, figsize=(13,row_height*rows_of_5), sharex=True, sharey=sharey) # , sharey=True , dpi=100) # 
  
  colors = sns.color_palette("tab10", 2)
  
  # vlist
  vlist = list(df[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
  print(f'vlist = {vlist}')
  print(f'var = {var}')

  # min and max dates
  if(var == 'DATE'):
    _min = min(df[f'{var}'])
    _max = max(df[f'{var}'])
    print(_min, _max)  
  
  # loop over names
  for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
    print('  ', i, ax, v)
    
    # filter
    tmp2d1 = df[(df[f'name'] == v)] # (_tmpp[f'{var}'] > -20) &     
    
    if(byvar!='source'):
      names = ['Male', 'Female']  
      s1 = list(tmp2d1[tmp2d1[f'{byvar}'] == '1'][f'{var}'])
      s2 = list(tmp2d1[tmp2d1[f'{byvar}'] == '2'][f'{var}'])
      ax.hist(s1, bins = list(np.linspace(bin_min, bin_max, 100)), color=colors[0], label=names[0], alpha=0.5) # normed=True 
      ax.hist(s2, bins = list(np.linspace(bin_min, bin_max, 100)), color=colors[1], label=names[1], alpha=0.5) # normed=True 
    else:
      names = ['hes_apc', 'gdppr']  
      
      s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'{var}'])
      s2 = list(tmp2d1[tmp2d1[f'source'] == 'gdppr'][f'{var}'])
      
      if((bin_min == 0) & (bin_max == 0)): bins = 100
      else: bins = list(np.linspace(bin_min, bin_max, 100))

      
      if(var == 'DATE'):
        tt1 = np.linspace(pd.Timestamp(_min).value, pd.Timestamp(_max).value, 100)
        tt2 = pd.to_datetime(tt1)
        tt3 = mdates.date2num(tt2)
        bins = list(tt3)
        print(bins)      
        
        # ax.hist([s1, s2], bins = bins, stacked=stacked, color=colors, label=names) # normed=True      
        ax.hist([tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'{var}']], bins = bins, color=colors[0], label=names[0], alpha=0.5) # normed=True 
        ax.hist([tmp2d1[tmp2d1[f'source'] == 'gdppr'][f'{var}']], bins = bins, color=colors[1], label=names[1], alpha=0.5) # normed=True 
      else:
        # ax.hist([s1, s2], bins = bins, stacked=stacked, color=colors, label=names) # normed=True      
        ax.hist([s1], bins = bins, color=colors[0], label=names[0], alpha=0.5) # normed=True 
        ax.hist([s2], bins = bins, color=colors[1], label=names[1], alpha=0.5) # normed=True         

    # plot reformats  
    ax.set_title(f'{v}')
    ax.set(xlabel=f'{xlabel}')
    ax.xaxis.set_tick_params(labelbottom=True)    
    if(var == 'DATE'): 
      ax.xaxis.set_tick_params(rotation=90) # labelbottom=True)    
      ax.set(xlim=(_min, _max))  
    if(i==0): ax.legend(loc='upper left')
       
#     if(out_com=='out'):
#       axes[2,4].set_axis_off()
#       for i in range(0,2):
#         for j in range(0, 5):
#           axes[i,j].xaxis.set_tick_params(labelbottom=True)
#     elif(out_com=='com'):      
#       axes[7,3].set_axis_off()
#       axes[7,4].set_axis_off()
#       for i in range(0,7):
#         for j in range(0, 5):
#           axes[i,j].xaxis.set_tick_params(labelbottom=True)          
  plt.tight_layout();
  return fig

# COMMAND ----------

# MAGIC %md ###  3.3.2 Plots - First event - Over follow-up time (years) by data source

# COMMAND ----------

fig1 = plot_hx_1st(df=_tmpp, var='fu', bin_min=-105, bin_max=0, sharey=False, stacked=False, xlabel='\nFollow-up (years)\n', row_height=5)
display(fig1)

# COMMAND ----------

fig2 = plot_hx_1st(df=_tmpp, var='fu', bin_min=-105, bin_max=0, sharey=True, stacked=False, xlabel='\nFollow-up (years)\n', row_height=5)
display(fig2)

# COMMAND ----------

# MAGIC %md ### 3.3.3 Plots - First event - Over calendar time by data source

# COMMAND ----------

fig3 = plot_hx_1st(df=_tmpp, var='DATE', row_height=5, bin_min=0, bin_max=0, sharey=False, stacked=False, xlabel='\nDate\n')
display(fig3)

# COMMAND ----------

# fig4 = plot_hx_1st(df=_tmpp, var='DATE', row_height=2.8, bin_min=0, bin_max=0, sharey=True, stacked=True, xlabel='\nDate\n')
# display(fig4)

# COMMAND ----------

# MAGIC %md ### 3.3.4 Plots - First event - Over age at event (years) by data source

# COMMAND ----------

fig5 = plot_hx_1st(df=_tmpp, var='age', bin_min=0, bin_max=69, sharey=False, stacked=False, xlabel='\nAge (years)\n', row_height=5)
display(fig5)

# COMMAND ----------

# fig6 = plot_hx_1st(df=_tmpp, var='age', bin_min=0, bin_max=20, sharey=True, stacked=True, xlabel='\nAge (years)\n')
# display(fig6)

# COMMAND ----------

# MAGIC %md ### 3.3.5 Plots - First event - Over age at event (years) by sex

# COMMAND ----------

fig7 = plot_hx_1st(df=_tmpp, var='age', byvar='SEX', bin_min=0, bin_max=69, sharey=False, stacked=False, xlabel='\nAge (years)\n', row_height=5)
display(fig7)

# COMMAND ----------

# fig8 = plot_hx_1st(df=_tmpp, var='age', byvar='SEX', bin_min=0, bin_max=20, sharey=True, stacked=True, xlabel='\nAge (years)\n')
# display(fig8)

# COMMAND ----------

# MAGIC %md ### 3.3.6 Codelist match summaries

# COMMAND ----------

# check codelist match summary by name and source
display(_hx_summ_name)

# COMMAND ----------

# check codelist match summary by name, source, and code
display(_hx_summ_name_code)

# COMMAND ----------

#display(_hx_summ_name_code.where(f.col('name').isin(['hypertension_drugs'])).orderBy(f.desc('n')))

# COMMAND ----------

# MAGIC %md ### 3.3.7 _hx_1st_wide

# COMMAND ----------

# check
count_var(_hx_1st_wide, 'PERSON_ID'); print()
print(len(_hx_1st_wide.columns)); print()
print(pd.DataFrame({f'_cols': _hx_1st_wide.columns}).to_string()); print()

# COMMAND ----------

tmp1 = (
  _hx_1st_wide
  .na.fill(value=0, subset=['cov_hx_nonfatal_myocardial_infarction_flag', 'cov_hx_nonfatal_stroke_flag'])
  .withColumn('_concat', f.concat(f.col('cov_hx_nonfatal_myocardial_infarction_flag') , f.col('cov_hx_nonfatal_stroke_flag')))
  #.withColumn('_concat_hyp', f.concat(f.col('cov_hx_hypertension_flag'), f.col('cov_hx_hypertension_drugs_flag')))
  #.withColumn('_hx_nonfatal', f.when((f.col('cov_hx_af_flag') == 1) | (f.col('cov_hx_hypertension_flag') == 1), 1).otherwise(0))
)
tmpt = tab(tmp1, 'cov_hx_nonfatal_myocardial_infarction_flag'); print()
tmpt = tab(tmp1, 'cov_hx_nonfatal_stroke_flag'); print()

tmpt = tab(tmp1, '_concat'); print()
#tmpt = tab(tmp1, '_concat_hyp'); print()
#tmpt = tab(tmp1, '_hx_nonfatal'); print()
#tmpt = tab(tmp1, '_concat', '_hx_nonfatal', var2_unstyled=1); print()

# COMMAND ----------

display(tmp1)

# COMMAND ----------

# MAGIC %md # 4 Prepare

# COMMAND ----------

# add PERSON_ID back in
# important for verifying the number of individuals after inc/exc in the subsequent inc/exc notebook
# in future perform this step after initial inc/exc
tmpf1 = (
  merge(cohort_dataset.select('PERSON_ID'), _hx_1st_wide, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0)
  .na.fill(value=0, subset=['cov_hx_nonfatal_myocardial_infarction_flag', 'cov_hx_nonfatal_stroke_flag'])
); print()

# check 
count_var(tmpf1, 'PERSON_ID'); print()
tmpt = tab(tmpf1, 'cov_hx_nonfatal_myocardial_infarction_flag'); print()
tmpt = tab(tmpf1, 'cov_hx_nonfatal_stroke_flag'); print()
#tmpt = tab(tmpf1, 'cov_hx_hypertension_drugs_flag'); print()

# COMMAND ----------

# check final
display(tmpf1)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

save_table(df=tmpf1, out_name=f'{proj}_tmp_hx_nonfatal_{cohort}', save_previous=True) #cohortrefactoring

# COMMAND ----------

# save codelist match summary tables
list_tables = ['_hx_summ_name', '_hx_summ_name_code']
for i, table in enumerate(list_tables):
  print(i, table)
  outName = f'{proj}_tmp_hx_nonfatal{table}_{cohort}'.lower() #cohortrefactoring
  tmp1 = globals()[table]
  tmp1.write.mode('overwrite').saveAsTable(f'{dsa}.{outName}')
  #spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
  print(f'  saved {dsa}.{outName}')