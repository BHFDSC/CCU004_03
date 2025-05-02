# Databricks notebook source
# MAGIC %md # CCU004_03-D08a-covariates
# MAGIC  
# MAGIC **Description** This notebook creates smoking and medications covariates. Consultation rates (GP visits and HES APC admissions) are also derived here.
# MAGIC
# MAGIC Covariates will be defined using a selection/priority group to allow prioritisation of the different time periods around the study start date.
# MAGIC
# MAGIC **Smoking**:
# MAGIC <br> Where x is the difference in years from the DATE to the study start date the priority groups **for smoking** are defined as follows:
# MAGIC
# MAGIC **1.** -infinity <= x < 0 (**all years before**)
# MAGIC
# MAGIC **2.** 0 <= x < +0.5 (half a year after)
# MAGIC
# MAGIC That is, only where a smoking record cannot be found before study start date, will records be looked at half a year after.
# MAGIC
# MAGIC Note that smoking status can change over time and smoking upgrades will be considered (e.g. A person may have historic records indicating that they are a current smoker but more recent records (closest to baseline) indicating that they have never smoked and in this case the historical records should be considered in order to potentially upgrade the person from a non-smoker to an ex-smoker).
# MAGIC The **cloest to baseline record** will be found (prioritising finding a closest record in selection group 1 and then group 2) and then a lookback method is employed to find all smoking records that occurred before this closest to baseline date.
# MAGIC
# MAGIC
# MAGIC **Medications:**
# MAGIC <br> Where x is the difference in years from the DATE to the study start date the priority groups **for medications** are defined as follows:
# MAGIC
# MAGIC **1.**  -1.5 <= x < 0 (one and a half years before)
# MAGIC
# MAGIC
# MAGIC **Authors** Fionna Chalmers, Anna Stevenson, Spencer Keene, Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton, Alexia Sampri for CCU018_01, earlier CCU002 sub-projects and subsequently CCU003_05-D10-covariates
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`CCU004_03_out_covariates`** : covariates for the cohort

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

# MAGIC %md ##0.1 Custom Functions

# COMMAND ----------

def codelist_match_all(_dict, _name_prefix, broadcast:int = 0):

  print('--------------------------------------------------------------------------------------', flush=True)
  print('codelist_match', flush=True)
  print('--------------------------------------------------------------------------------------', flush=True)
  print(f'_name_prefix = {_name_prefix}'); print()
  
  # initialise
  _dict_codelist = {}
  _dict_codematch = {}
  
  # loop over data sources
  for i, key in enumerate(_dict):
    # extract elements from the dictionary entry 
    # (dataset, codelist, ordering [in the event of tied matches on date from different sources])
    _data = _dict[key][0]
    _codelist = _dict[key][1]
    _order = _dict[key][2]
    print(i, key, _data, _codelist, _order)

    # get codelist
    _tmp_codelist = globals()[_codelist]\
      .select(['code', 'name'])
    
    # ------------------------------------------------------------------------------------
    # match the dataset and codelist
    # ------------------------------------------------------------------------------------
    print(f'  codelist match')
    if(broadcast == 0):
      # without broadcast
      _tmp_codematch = globals()[_data]\
        .join(_tmp_codelist, on='code', how='inner')
    elif(broadcast == 1):
      # with broadcast
      _tmp_codematch = globals()[_data]\
        .join(f.broadcast(_tmp_codelist), on='code', how='inner')
    else: 
      raise ValueError(f"'broadcast' should take values: 0 or 1. The value provided was {broadcast}") 
    
    # add source and order
    _tmp_codematch = _tmp_codematch\
      .withColumn('source', f.lit(key))\
      .withColumn('sourcen', f.lit(_order))
    
    # store the results
    _dict_codelist[key] = _tmp_codelist
    _dict_codematch[key] = _tmp_codematch
  
  # append
  print(f'\nappend codelist matches from different sources/terminologies')
  _codelist_all = reduce(DataFrame.unionByName, _dict_codelist.values())  
  _codematch_all = reduce(DataFrame.unionByName, _dict_codematch.values()) 
  _dict_codematch['all'] = _codematch_all

  return _dict_codematch

# COMMAND ----------

# wide processing pulled out into own function to handle selection groups independently
def codelist_match_events(_df_all, _name_prefix, _last_event=0):  
  
  _codematch_all = _df_all

  # ------------------------------------------------------------------------------------
  # first/last event of each name
  # ------------------------------------------------------------------------------------
  if(_last_event == 1):
    print(f'filter to LAST event')
    _win = Window\
      .partitionBy(['PERSON_ID', 'name'])\
      .orderBy(f.desc('DATE'), 'sourcen', 'code')      
  else:
    print(f'filter to 1st event')
    _win = Window\
      .partitionBy(['PERSON_ID', 'name'])\
      .orderBy('DATE', 'sourcen', 'code')  
  
  # filter
  _codematch_1st = _codematch_all\
    .withColumn('_rownum', f.row_number().over(_win))\
    .where(f.col('_rownum') == 1)\
    .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END', 'DATE', 'name', 'source', 'code')\
    .orderBy('PERSON_ID', 'DATE', 'name')

  # ------------------------------------------------------------------------------------
  # identify ties by source for first event of each name
  # ------------------------------------------------------------------------------------
  print(f'identify ties from sources/terminologies for first (/last) event')
  
  _codematch_1st_tie_source = _codematch_all\
    .withColumn('_tie', f.dense_rank().over(_win))\
    .where(f.col('_tie') == 1)\
    .groupBy('PERSON_ID', 'name')\
    .agg(\
      f.countDistinct(f.col('source')).alias(f'_n_distinct_source')\
      , f.countDistinct(f.when(f.col('source').isNull(), 1)).alias(f'_null_source')\
      , f.sort_array(f.collect_set(f.col('source'))).alias('_tie_source_list')\
    )\
    .withColumn('_tie_source', f.when((f.col('_n_distinct_source') + f.col(f'_null_source')) > 1, 1).otherwise(0))\
    .select('PERSON_ID', 'name', '_tie_source', '_tie_source_list')
  
  _codematch_1st = _codematch_1st\
    .join(_codematch_1st_tie_source, on=['PERSON_ID', 'name'], how='left')

  # ------------------------------------------------------------------------------------
  # reshape
  # ------------------------------------------------------------------------------------
  print(f'reshape long to wide')
  
  # join codelist names before reshape to ensure all covariates are created (when no code matches are found)
  _codelist_all = _df_all.select('name').distinct() 

  # reshape long to wide  
  _codematch_1st_wide = _codematch_1st\
    .drop('code')\
    .join(_codelist_all, on='name', how='outer')\
    .withColumn('name', f.concat(f.lit(f'{_name_prefix}'), f.lower(f.col('name'))))\
    .groupBy('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END')\
    .pivot('name')\
    .agg(f.first('DATE'))\
    .where(f.col('PERSON_ID').isNotNull())\
    .orderBy('PERSON_ID')  
    
  # add flag and date columns
  print(f'add flag and date')
  vlist = []
  for i, v in enumerate([col for col in list(_codematch_1st_wide.columns) if re.match(f'^{_name_prefix}', col)]):
    print(' ' , i, v)
    _codematch_1st_wide = _codematch_1st_wide\
      .withColumnRenamed(v, v + '_date')\
      .withColumn(v + '_flag', f.when(f.col(v + '_date').isNotNull(), 1))
    vlist = vlist + [v + '_flag', v + '_date']
  _codematch_1st_wide = _codematch_1st_wide\
    .select(['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'] + vlist) 
  
  return _codematch_1st, _codematch_1st_wide

# COMMAND ----------

# smoking status, upgrades (will be applied to each selection group)
def smoking_status(_smoking_last_wide):
  
  # get max date and corresponding var
  # identify ties
  # define smoking status, priority ordering = current, ex, never
  # identify possible upgrades to never status based on evidence of current or ex


  vlist = ['cov_smoking_current_date', 'cov_smoking_ex_date', 'cov_smoking_never_date']
  _smoking_last_wide = _smoking_last_wide\
    .withColumn('cov_smoking_max_date', f.greatest(*[f.col(v) for v in vlist]))\
    .withColumn('cov_smoking_max_var', f.concat_ws(';', f.array([f.when(f.col(v) == f.col('cov_smoking_max_date'), v).otherwise(None) for v in vlist])))\
    .withColumn('cov_smoking_status',\
      f.when(f.col('cov_smoking_max_var').rlike('current'), 'Current')\
       .when(f.col('cov_smoking_max_var').rlike('ex'), 'Ex')\
       .when(f.col('cov_smoking_max_var').rlike('never'), 'Never')\
    )\
    .withColumn('cov_smoking_status_tie', f.when(f.col('cov_smoking_max_var').rlike(';'), 1))\
    .withColumn('cov_smoking_status_upgrade_never',\
      f.when(\
        (f.col('cov_smoking_status') == 'Never')\
        & ((f.col('cov_smoking_ex_date').isNotNull()) | (f.col('cov_smoking_current_date').isNotNull()))\
      , 1)\
    )

  # drop('cov_smoking_max_date', 'cov_smoking_max_var', 'cov_smoking_status_tie', 'cov_smoking_status_upgrade_never')
  
  return _smoking_last_wide

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

# lsoa_region   = spark.table(path_cur_lsoa_region) # these are not used in the notebook
# lsoa_imd      = spark.table(path_cur_lsoa_imd) # these are not used in the notebook
codelist_covariates = spark.table(path_out_codelist_covariates)

spark.sql(f"""REFRESH TABLE {dsa}.{proj}_out_cohort_{cohort}""") #cohortrefactoring
cohort_dataset       = spark.table(path_out_cohort)

gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

hes_apc_long = spark.table(path_cur_hes_apc_long)

pmeds = extract_batch_from_archive(parameters_df_datasets, 'pmeds')

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# check
count_varlist(codelist_covariates, ['name'])
count_varlist(codelist_covariates, ['name', 'terminology', 'code'])
tmpt = tab(codelist_covariates, 'name', 'terminology'); print()

# filter
codelist_covariates_prepared = (
  codelist_covariates
  .where(f.col('name').isin([
    # Smoking groups
    'smoking_current','smoking_ex','smoking_never',
    # Medication groups
    'statin','bp_lowering','metformin',
    # Shielding
    'shielding']))
)
 
# check
count_varlist(codelist_covariates_prepared, ['name'])
count_varlist(codelist_covariates_prepared, ['name', 'terminology', 'code'])
tmpt = tab(codelist_covariates_prepared, 'name', 'terminology'); print()
print(codelist_covariates_prepared.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ###3.1.1 Smoking

# COMMAND ----------

# filter
codelist_smoking = (
  codelist_covariates_prepared
  .where(f.col('name').rlike('^smoking_.*$'))
)

# check
tmpt = tab(codelist_smoking, 'name', 'terminology', var2_unstyled=1); print()

# COMMAND ----------

display(codelist_smoking)

# COMMAND ----------

# MAGIC %md ###3.1.2 Medications

# COMMAND ----------

# filter
codelist_medications = (
  codelist_covariates_prepared
  .where(f.col('name').isin([
    'statin','bp_lowering','metformin']))
)

# check
tmpt = tab(codelist_medications, 'name', 'terminology', var2_unstyled=1); print()

# COMMAND ----------

display(codelist_medications)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1.3 Shielding

# COMMAND ----------

# filter
codelist_shielding = (
  codelist_covariates_prepared
  .where(f.col('name').isin([
    'shielding']))
)

# check
tmpt = tab(codelist_shielding, 'name', 'terminology', var2_unstyled=1); print()

display(codelist_shielding)

# COMMAND ----------

# MAGIC %md ##3.2 Cohort

# COMMAND ----------

# MAGIC %md Setting the maximum and minimum CENSOR_DATE_END and CENSOR_DATE_START per covariate group (smoking, medications) to initially prepare GDPPR and PMEDs. That is, **all** individuals have the **same** CENSOR_DATE_END and CENSOR_DATE_START max and min dates.
# MAGIC
# MAGIC For smoking:
# MAGIC - CENSOR_DATE_START will be set at the date of the first smoking record recorded in GDPPR (minimum smoking record date an individual may have)
# MAGIC - CENSOR_DATE_END will be set at study_start_date + 6months (max smoking record date an individual may have if their closest to baseline record is not in priority group 1)
# MAGIC
# MAGIC For medications:
# MAGIC - CENSOR_DATE_START will be set at 1 year prior to study_start_date (minimum medications record date an individual may have)
# MAGIC - CENSOR_DATE_END will be set at study_start_date + 6months (max medications record date an individual may have if their closest to baseline record is not in priority group 1)
# MAGIC
# MAGIC GDPPR and PMEDs are being prepared with these initial start and end dates in order to reduce the size of each dataset.
# MAGIC
# MAGIC **Important to note:**
# MAGIC <br>**1.** The CENSOR_DATE_END will then change on an individual level depending on what selection priority group an individual has a closest to baseline record
# MAGIC <br>**2.** In the Smoking section, `smoking_current`,`smoking_ex`,`smoking_never` will not be treated independently when defining selection groups and the highest priority group in which a person falls. E.g. a person may have a `smoking_current` record that falls in priority group 1 (and no other smoking record types in this time period) and then a `smoking_ex` record that falls in group 2 (and no other smoking record types in this time period). By combining smoking groups we correctly conclude that the closest to baseline record date for any smoking types is the one in priority group 1 and in this case the `smoking_ex` record would not be considered; the CENSOR_DATE_END for all smoking groups would be set at the date of the the `smoking_current` record in group 1. **This is not the case** for medications and each medications group **will be treated independently**. That is, the closest to baseline record (using the selection group criteria) will be set individually for `statin`, `bp_lowering` and `metaformin`. E.g. following the same logic as the example before, if a person had a `statin` record in group 1 (and no other medication records) and then a `metformin` record in group 2 (and no other medication records) then the CENSOR_DATE_END would be set for `statin` to be the group 1 date and `metformin` to be the group 2 date (thus not losing the closest to baseline date of metformin in group 2). To conclude, the 3 medication groups will be treated independently from each other whilst deriving the CENSOR_DATE_END for each individual and the smoking group will be treated as one group.

# COMMAND ----------

# MAGIC %md ### 3.2.1 Smoking censor dates

# COMMAND ----------

# For smoking CENSOR_DATE_START will be the same for all persons (the DATE of the earliest smoking record in GDPPR); however, CENSOR_DATE_END will be individual to the person (the closest smoking record that person has to baseline - prioritising selection group 1 then group 2 to find the closest date)

# CENSOR_DATE_START is the start of the lookback period (all historical records)
# CENSOR_DATE_END is the end of the lookback period, so study_start_date (i.e. cohort start date) + 6 months for this project (wanting to look slightly into the future)
# GDPPR will be prepared with the above start and end dates and then CENSOR_DATE_END will be adjusted for individuals

gdppr_smoking = gdppr.join(codelist_smoking, on='code', how='inner').withColumn('DATE', f.date_format(f.col('DATE'), 'yyyy-MM-dd'))
smoking_min_date = gdppr_smoking.select(f.min("DATE")).first()[0]
print(smoking_min_date)

print(study_start_date)

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates_smoking - lookback to identify covariates')
print('--------------------------------------------------------------------------------------')
individual_censor_dates_smoking_max = (
  cohort_dataset
  .select('PERSON_ID', 'study_start_date') 
  .withColumn('CENSOR_DATE_START', f.lit(smoking_min_date)) #lookback all records
  .withColumn('CENSOR_DATE_END', f.add_months(f.col('study_start_date'), 3)) #ccu004-03 0.25year lookforward
  .drop('study_start_date')
)

# check
count_var(individual_censor_dates_smoking_max, 'PERSON_ID'); print()
tmpt = tabstat(individual_censor_dates_smoking_max, 'CENSOR_DATE_START', date=1); print()
tmpt = tabstat(individual_censor_dates_smoking_max, 'CENSOR_DATE_END', date=1); print()
print(individual_censor_dates_smoking_max.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 3.2.2 Medications censor dates

# COMMAND ----------

# For medications CENSOR_DATE_START will be the same for all persons (study_start_date - 1.5year); however, CENSOR_DATE_END will be individual to the person (the closest medication group record that person has to baseline - prioritising selection group 1 then group 2 to find the closest date)

# CENSOR_DATE_START is the start of the lookback period, study_start_date minus 1.5 year for medications
# CENSOR_DATE_END is the end of the lookback period, so study_start_date (i.e. cohort start date) (no look forward into the future)


print(study_start_date)
assert cohort_dataset.where(f.col('study_start_date') == study_start_date).count() == cohort_dataset.count()

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates_medications - lookback to identify covariates')
print('--------------------------------------------------------------------------------------')
individual_censor_dates_medications_max = (
  cohort_dataset
  .select('PERSON_ID', 'study_start_date') 
  .withColumn('CENSOR_DATE_START', f.add_months(f.col('study_start_date'), -18)) #1.5year lookback
  .withColumn('CENSOR_DATE_END', f.add_months(f.col('study_start_date'), 0)) #NO lookforward
  .drop('study_start_date')
)

# check
count_var(individual_censor_dates_medications_max, 'PERSON_ID'); print()
tmpt = tabstat(individual_censor_dates_medications_max, 'CENSOR_DATE_START', date=1); print()
tmpt = tabstat(individual_censor_dates_medications_max, 'CENSOR_DATE_END', date=1); print()
print(individual_censor_dates_medications_max.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2.3 Shielding censor dates

# COMMAND ----------

# For shielding CENSOR_DATE_START will be the same for all persons (study_start_date - 1.5year); however, CENSOR_DATE_END will be individual to the person (the closest medication group record that person has to baseline - prioritising selection group 1 then group 2 to find the closest date)

# CENSOR_DATE_START is the start of the lookback period, study_start_date minus 1.5 year for shielding
# CENSOR_DATE_END is 3 months after study_start_date (i.e. cohort start date) (no look forward into the future)


print(study_start_date)
assert cohort_dataset.where(f.col('study_start_date') == study_start_date).count() == cohort_dataset.count()

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates_medications - lookback to identify covariates')
print('--------------------------------------------------------------------------------------')
individual_censor_dates_shielding_max = (
  cohort_dataset
  .select('PERSON_ID', 'study_start_date') 
  .withColumn('CENSOR_DATE_START', f.add_months(f.col('study_start_date'), -18)) #1.5year lookback
  .withColumn('CENSOR_DATE_END', f.add_months(f.col('study_start_date'), 3)) #3 months lookforward
  .drop('study_start_date')
)

# check
count_var(individual_censor_dates_shielding_max, 'PERSON_ID'); print()
tmpt = tabstat(individual_censor_dates_shielding_max, 'CENSOR_DATE_START', date=1); print()
tmpt = tabstat(individual_censor_dates_shielding_max, 'CENSOR_DATE_END', date=1); print()
print(individual_censor_dates_shielding_max.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Source Data

# COMMAND ----------

# MAGIC %md ###3.3.1 Curate

# COMMAND ----------

# MAGIC %md ####3.3.1.1 GDPPR

# COMMAND ----------

# PREPARING GDPPR FOR SMOKING
# Note that GDPPR is being prepared here for everyone in the cohort; whether they have smoking records or not

print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
gdppr_max_prepared = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE')
)

# add individual censor dates
gdppr_max_prepared = (
  gdppr_max_prepared
  .join(individual_censor_dates_smoking_max, on='PERSON_ID', how='inner')
)

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
gdppr_max_prepared = (
  gdppr_max_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# # temp save (checkpoint)
# gdppr_max_prepared = temp_save(df=gdppr_max_prepared, out_name=f'{proj}_tmp_covariates_gdppr_max_{cohort}') #cohortrefactoring

# COMMAND ----------

# PREPARING GDPPR FOR SHIELDING
# Note that GDPPR is being prepared here for everyone in the cohort; whether they have shielding records or not

print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
gdppr_max_prepared_shielding = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE')
)

# add individual censor dates
gdppr_max_prepared_shielding = (
  gdppr_max_prepared_shielding
  .join(individual_censor_dates_shielding_max, on='PERSON_ID', how='inner')
)

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
gdppr_max_prepared_shielding = (
  gdppr_max_prepared_shielding
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# # temp save (checkpoint)
# gdppr_max_prepared_shielding = temp_save(df=gdppr_max_prepared_shielding, out_name=f'{proj}_tmp_shielding_gdppr_max_{cohort}') #cohortrefactoring

# COMMAND ----------

# MAGIC %md ####3.3.1.1 PMEDs

# COMMAND ----------

# All codes - that fall between the max censor dates
print('--------------------------------------------------------------------------------------')
print('pmeds')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
pmeds_max_prepared = pmeds\
  .select(['Person_ID_DEID', 'ProcessingPeriodDate', 'PrescribedBNFCode'])\
  .withColumnRenamed('Person_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('ProcessingPeriodDate', 'DATE')\
  .withColumnRenamed('PrescribedBNFCode', 'CODE')

# check
count_var(pmeds_max_prepared, 'PERSON_ID'); print()

# add censor dates
pmeds_max_prepared = pmeds_max_prepared\
  .join(individual_censor_dates_medications_max, on='PERSON_ID', how='inner')

# check
count_var(pmeds_max_prepared, 'PERSON_ID'); print()


# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
pmeds_max_prepared = (
pmeds_max_prepared
.withColumn('flag_1',
    f.when((f.col('DATE').isNull()), 1)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)
  )
)


pmeds_max_prepared = (
pmeds_max_prepared
  .where(f.col('flag_1').isin([2]))
  .drop('flag_1')
)

# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
pmeds_max_prepared = (
pmeds_max_prepared
  .withColumn('flag_2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)\
  )
)
# tmpt = tab(hes_apc_long_prepared, 'flag_2'); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
pmeds_max_prepared = (
pmeds_max_prepared
  .where(f.col('flag_2').isin([1]))
  .drop('flag_2')
)


# # temp save (checkpoint)
# pmeds_max_prepared = temp_save(df=pmeds_max_prepared, out_name=f'{proj}_tmp_covariates_pmeds_max_{cohort}') #cohortrefactoring

# COMMAND ----------

# MAGIC %md ###3.3.2 Read Source Data

# COMMAND ----------

# # Read back in data
# gdppr_max_prepared = spark.table(f'{dsa}.{proj}_tmp_covariates_gdppr_max_{cohort}') #prepared again in Smoking section #cohortrefactoring
# gdppr_max_prepared_shielding  = spark.table(f'{dsa}.{proj}_tmp_shielding_gdppr_max_{cohort}') #prepared again in Smoking section #cohortrefactoring
# pmeds_max_prepared = spark.table(f'{dsa}.{proj}_tmp_covariates_pmeds_max_{cohort}') #prepared again in Medications section #cohortrefactoring

# COMMAND ----------

display(gdppr_max_prepared)

# COMMAND ----------

display(pmeds_max_prepared)

# COMMAND ----------

# MAGIC %md # 4. Smoking

# COMMAND ----------

# check codelist
print(codelist_smoking.orderBy('name', 'code').toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 4.1 Codematch all

# COMMAND ----------

# MAGIC %md In this section, we extract all the smoking records for each individual between the pre-defined extreme censor start and end dates.

# COMMAND ----------

# DBTITLE 1,Curate all smoking records
# dictionary - dataset, codelist, and ordering in the event of tied records
_smoking_in_max = {
  'gdppr': ['gdppr_max_prepared', 'codelist_smoking',  1]
}

_smoking_max = codelist_match_all(_smoking_in_max, _name_prefix=f'cov_')

# COMMAND ----------

# DBTITLE 1,Save
# temp save
smoking_out_max_all = _smoking_max['all']
smoking_out_max_all = temp_save(df=smoking_out_max_all, out_name=f'{proj}_tmp_covariates_smoking_out_max_all_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

# DBTITLE 1,Read Data
smoking_max_all = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_out_max_all_{cohort}') #cohortrefactoring

# COMMAND ----------

display(smoking_max_all.orderBy("PERSON_ID","name","DATE"))

# COMMAND ----------

# MAGIC %md ##4.2 Selection groups

# COMMAND ----------

# MAGIC %md Now we assign selection groups in order to identify an individuals censor end date in relation to the closest record date (to baseline) in the highest priority group.
# MAGIC
# MAGIC **Note that smoking groups are not treated independently and we are finding an individuals censor end date across all smoking groups.**
# MAGIC
# MAGIC That is, the closest to baseline dates for each of the smoking groups (current/ex/never) are wrt the cloest to baseline date found for the smoking records overall for each individual. E.g. , there may be a `smoking_current` record that was found in group 2 but not in group 1 and then a `smoking_ex` record that was found in group 1 - the `smoking_ex` record in group 1 would define the closest to baseline date and the closest to baseline `smoking_current` date would be the one from group 3 and NOT group 2.

# COMMAND ----------

# DBTITLE 1,Assign selection groups
# add a selection group to allow prioritisation of the different time periods around the study start date
# 1: -infinity <= x < 0 (all years before)
# 2: 0 <= x < +0.5 (half a year after)
# where x is the difference in years from the DATE to the study start date

# check
print(f'study_start_date = {study_start_date}')

smoking_all_selection_groups = (
  smoking_max_all
  .withColumn('study_start_date', f.to_date(f.lit(study_start_date)))
  .withColumn('selection_group',
             f.when((f.col('DATE')  >= f.col('CENSOR_DATE_START')) & (f.col('DATE') < f.col('study_start_date')), 1)
             .when((f.col('DATE')  >= f.col('study_start_date')) & (f.col('DATE') < f.col('CENSOR_DATE_END')), 2)
             .otherwise(999999))
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('study_start_date'))/365.25)
  .withColumn('diff_abs', f.abs(f.col('diff')))
)

# temp save
smoking_all_selection_groups = temp_save(df=smoking_all_selection_groups, out_name=f'{proj}_tmp_covariates_smoking_all_selection_groups_{cohort}'); print() #cohortrefactoring

# check 
tmpt = tabstat(smoking_all_selection_groups, 'DATE', byvar='selection_group', date=1); print()
tmpt = tabstat(smoking_all_selection_groups, 'diff', byvar='selection_group'); print()
tmpt = tabstat(smoking_all_selection_groups, 'diff_abs', byvar='selection_group'); print()
tmpt = tab(smoking_all_selection_groups, 'name', 'selection_group'); print()

# COMMAND ----------

# check
display(smoking_all_selection_groups.orderBy('PERSON_ID', 'name', 'selection_group', 'DATE', 'source', 'code'))

# COMMAND ----------

# DBTITLE 1,Select closest to baseline record in each selection group in each smoking group
##########################################################################################
###   Closest record to Baseline in EACH selection_group in each smoking group (name)  ###
##########################################################################################

# Note that there may be more than one row per 'PERSON_ID','selection_group','name' and closest date if there are moe than one type of smoking group code used as at that date

win_closest_rec_in_groups_name = (
  Window
  .partitionBy('PERSON_ID','selection_group','name')
  .orderBy(f.col("PERSON_ID"),f.col('selection_group'),f.col("diff_abs").asc())  
)

smoking_all_selection_groups_closest_name = (
  smoking_all_selection_groups
  .withColumn('rownum_id_closest_date_group', f.dense_rank().over(win_closest_rec_in_groups_name))
  .where(f.col('rownum_id_closest_date_group') == 1)
  .drop('rownum_id_closest_date_group')
  .drop("CENSOR_DATE_START","CENSOR_DATE_END","study_start_date","sourcen","source")
  .orderBy("PERSON_ID","selection_group")
  .distinct()
)

display(smoking_all_selection_groups_closest_name)

# COMMAND ----------

# as above but dropping code to then have one row per person for each selection and smoking group
display(smoking_all_selection_groups_closest_name.drop("CODE").distinct())

# COMMAND ----------

# DBTITLE 1,Select closest to baseline record in each selection group regardless of smoking group
##########################################################################################
###                  Closest record to Baseline in EACH selection_group                ###
##########################################################################################

# Note that there may be more than one row per 'PERSON_ID','selection_group'and closest date if there are moe than one type of smoking code used as at that date

win_closest_rec_in_groups = (
  Window
  .partitionBy('PERSON_ID','selection_group')
  .orderBy(f.col("PERSON_ID"),f.col('selection_group'),f.col("diff_abs").asc())  
)

smoking_all_selection_groups_closest = (
  smoking_all_selection_groups_closest_name
  .drop("name","code")
  .withColumn('rownum_id_closest_date_group', f.dense_rank().over(win_closest_rec_in_groups))
  .where(f.col('rownum_id_closest_date_group') == 1)
  .drop('rownum_id_closest_date_group')
  .drop("CENSOR_DATE_START","CENSOR_DATE_END","study_start_date")
  .orderBy("PERSON_ID","selection_group")
  .distinct()
)

display(smoking_all_selection_groups_closest)

# COMMAND ----------

# DBTITLE 1,Select highest priority group
 ###########################################################################
 ###            Now select highest priority selection group              ###
 ###########################################################################
  
win_closest_rec_top = (
  Window
  .partitionBy('PERSON_ID')
  .orderBy(f.col("PERSON_ID"),f.col('selection_group'),f.col("diff_abs").asc())  
)

smoking_closest_top = (
  smoking_all_selection_groups_closest
  .drop("source","sourcen")
  .withColumn('rownum_id_top_group', f.dense_rank().over(win_closest_rec_top))
  .where(f.col('rownum_id_top_group') == 1)
  .drop('rownum_id_top_group')
  .orderBy("PERSON_ID")
)

display(smoking_closest_top)

# COMMAND ----------

# MAGIC %md ##4.3 Censor end dates

# COMMAND ----------

# MAGIC %md These are the censor end dates for each individual which will be used to further prepare GDPRR (filtering out records greater than an individuals censor end date).

# COMMAND ----------

###########################################################################
###                      Smoking censor end dates                       ###
###########################################################################
  
# for those who have a smoking record - their new censor end dates based on the selection criteria
individual_censor_dates_smoking = (
  smoking_closest_top
  .select(f.col("PERSON_ID"),f.col("DATE").alias("CENSOR_DATE_END"))
  .join((individual_censor_dates_smoking_max.drop("CENSOR_DATE_END")),on=["PERSON_ID"],how="left")
)

display(individual_censor_dates_smoking)

# COMMAND ----------

# MAGIC %md ##4.4 Smoking records

# COMMAND ----------

# MAGIC %md Finally, derive full smoking history using the **new individual censor end dates** for smoking such that the closest smoking code to the study start date (using the priority selection groups) is used as CENSOR_DATE_END and all records before (and including) this date are pulled.

# COMMAND ----------

# DBTITLE 1,Smoking all records (censored at individuals selection group end dates)
smoking_records_all = (
  smoking_max_all
  .drop("CENSOR_DATE_END","CENSOR_DATE_START")
  .join(individual_censor_dates_smoking,on=["PERSON_ID"],how="left")
  .where(f.col('DATE') <= f.col('CENSOR_DATE_END'))
  .drop("CENSOR_DATE_END","CENSOR_DATE_START")
  .orderBy("PERSON_ID","DATE","name")
       )

display(smoking_records_all)

# COMMAND ----------

# DBTITLE 1,Prepare GDPPR
# prepare gdppr again filtered to exclude records > than an individuals CENSOR_DATE_END - note gdppr here includes all codes (not just smoking)
gdppr_ind_prepared = (
  individual_censor_dates_smoking
  .join((gdppr_max_prepared.drop("CENSOR_DATE_START","CENSOR_DATE_END")),on=["PERSON_ID"],how="left")
  .where(f.col('DATE') <= f.col('CENSOR_DATE_END'))
  .orderBy("PERSON_ID","DATE")
)

display(gdppr_ind_prepared)

# COMMAND ----------

# DBTITLE 1,Codelist match - to obtain closest to baseline records for each smoking group
_smoking_in = {
  'gdppr': ['gdppr_ind_prepared', 'codelist_smoking',  1]
}

_smoking, _smoking_last, _smoking_last_wide = codelist_match(_smoking_in, _name_prefix=f'cov_', _last_event=1)
_smoking_summ_name, _smoking_summ_name_code = codelist_match_summ(_smoking_in, _smoking)

# COMMAND ----------

# DBTITLE 1,Save
# temp save
smoking_out_all = _smoking['all']
smoking_out_all = temp_save(df=smoking_out_all, out_name=f'{proj}_tmp_covariates_smoking_out_all_{cohort}'); print() #cohortrefactoring

smoking_out_last = temp_save(df=_smoking_last, out_name=f'{proj}_tmp_covariates_smoking_out_last_{cohort}'); print() #cohortrefactoring
smoking_out_last_wide = temp_save(df=_smoking_last_wide, out_name=f'{proj}_tmp_covariates_smoking_out_last_wide_{cohort}'); print() #cohortrefactoring

smoking_out_summ_name = temp_save(df=_smoking_summ_name, out_name=f'{proj}_tmp_covariates_smoking_out_summ_name_{cohort}'); print() #cohortrefactoring
smoking_out_summ_name_code = temp_save(df=_smoking_summ_name_code, out_name=f'{proj}_tmp_covariates_smoking_out_summ_name_code_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

# DBTITLE 1,Read data
smoking_all = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_out_all_{cohort}') #cohortrefactoring
smoking_last = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_out_last_{cohort}') #cohortrefactoring
smoking_last_wide = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_out_last_wide_{cohort}') #cohortrefactoring
smoking_summ_name = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_out_summ_name_{cohort}') #cohortrefactoring
smoking_summ_name_code = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_out_summ_name_code_{cohort}') #cohortrefactoring

# COMMAND ----------

# smoking all extracted from codelist_match should be the same as the full smoking records derived before (that were pulled at the max censor end date and filtered)
assert smoking_all.count() == smoking_records_all.count()

# COMMAND ----------

# MAGIC %md ##4.5 Smoking status

# COMMAND ----------

# MAGIC %md Here we derive an overall smoking status and possible smoking updates.

# COMMAND ----------

smoking_last_wide_status = smoking_status(smoking_last_wide)
display(smoking_last_wide_status)

# COMMAND ----------

# save
smoking_out_last_wide_status = temp_save(df=smoking_last_wide_status, out_name=f'{proj}_tmp_covariates_smoking_out_last_wide_status_{cohort}') #cohortrefactoring

# COMMAND ----------

# read data
smoking_last_wide_status = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_out_last_wide_status_{cohort}') #cohortrefactoring

# COMMAND ----------

# MAGIC %md ##4.6 Check

# COMMAND ----------

# MAGIC %md ###4.6.1 Summaries

# COMMAND ----------

# # check codelist match summary by name and source
# add commas
df_all_summ_name_commas = smoking_summ_name
for colx in [col for col in smoking_summ_name.columns if re.match('^n_.*$', col)]:
  df_all_summ_name_commas = (
    df_all_summ_name_commas
    .withColumn(colx, f.format_number(colx, 0))
  )
display(df_all_summ_name_commas.orderBy('name'))

# COMMAND ----------

# add commas
df_all_summ_name_code_commas = smoking_summ_name_code
for colx in [col for col in smoking_summ_name_code.columns if (re.match('^n_.*$', col)) or (re.match('^n$', col))]:
  df_all_summ_name_code_commas = (
    df_all_summ_name_code_commas
    .withColumn(colx, f.format_number(colx, 0))
  )
display(df_all_summ_name_code_commas.orderBy('name', 'terminology', 'code'))

# COMMAND ----------

display(smoking_all)

# COMMAND ----------

count_var(smoking_last_wide, 'PERSON_ID')
# check ties and possible upgrades to never status
tmpt = tab(smoking_last_wide_status, 'cov_smoking_status_tie'); print()
tmpt = tab(smoking_last_wide_status, 'cov_smoking_status', 'cov_smoking_status_upgrade_never', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ###4.6.2 Plot max dates in relation to group 2 endpoint

# COMMAND ----------

_tmp = (
  smoking_last_wide_status
  .withColumn('study_start_date', f.to_date(f.lit(study_start_date)))
  .withColumn('CENSOR_DATE_END_MAX', f.add_months(f.col('study_start_date'), 6)) #ccu004-03: Not sure if we have to change this?
  .drop('study_start_date')
  .withColumn('diff', f.datediff(f.col('cov_smoking_max_date'), f.col('CENSOR_DATE_END_MAX'))/365.25)
  .select("PERSON_ID","cov_smoking_status","diff")
)
_tmpp = _tmp.toPandas()

# COMMAND ----------

_tmpp.count()

# COMMAND ----------

# Note that due to the priority groups there is a peak at -6 months due to this being prioritised before dates 6 months after baseline (i.e.not the typical skew from baseline back)

fig, axes = plt.subplots(1, 3, figsize=(13,4), sharex=True) # sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 1)
names = ['gdppr'] # , 'gdppr_snomed', 'hes_ae', 'hes_apc', 'hes_op']  
  
vlist = ['Never', 'Ex', 'Current']  
for i, (ax, cat) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, cat)
  tmp2d1 = _tmpp[_tmpp[f'cov_smoking_status'] == cat]
  tmp2d1 = tmp2d1[tmp2d1[f'diff'] > -20]
  s1 = list(tmp2d1[f'diff'])
  
  ax.hist([s1], bins = 50, stacked=True, color=colors, label=names) # normed=True
  ax.set_title(f'{cat}')
  if(i==0): ax.legend(loc='upper left')
    
plt.tight_layout();
display(fig)

# COMMAND ----------

# MAGIC %md ###4.6.3 Plot all records in each selection group with the selected censor end date per person

# COMMAND ----------

## Checking 

plot_min_axis = "2010-01"

print('WARNING: ALL records before baseline have been considered in priority group 3 and for each persons history, ALL historical records have been considered. These plots however only show records 10 years prior to baseline. Please review if a censor start date greater than 1760-01-01 should be considered (note that since the closest to baseline date for each group is taken as opposed to the earlist record per group, that the earlist date closest to baseline is greater than 1760-01-01)')

smoking_all_selection_groups = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_all_selection_groups_{cohort}') #cohortrefactoring

# summarise monthly
tmpp1 = (
  smoking_all_selection_groups
  .filter(f.col("selection_group")!=999999)
  .withColumn('date_ym', f.date_format(f.col('DATE'), 'yyyy-MM'))
  .groupBy('name', 'date_ym', 'selection_group')
  .agg(f.count(f.lit(1)).alias('n'))  
  .withColumn('stage', f.lit('pre-selection'))
  .orderBy('name', 'date_ym', 'selection_group')
)

win_id_name_ord = (
  Window
  .partitionBy('PERSON_ID', 'name')
  .orderBy('selection_group')  
)

win_id_name_date = (
  Window
  .partitionBy('PERSON_ID', 'name', 'DATE')
)

tmpp2 = (
  smoking_all_selection_groups
  .filter(f.col("selection_group")!=999999)
  .withColumn('rownum_id_name', f.row_number().over(win_id_name_ord))
  .where(f.col('rownum_id_name') == 1)
  .withColumn('date_ym', f.date_format(f.col('DATE'), 'yyyy-MM'))
  .groupBy('name', 'date_ym', 'selection_group')
  .agg(f.count(f.lit(1)).alias('n'))  
  .withColumn('stage', f.lit('post-selection'))
  .orderBy('name', 'date_ym', 'selection_group')
)

tmpp3d = (
  tmpp1
  .unionByName(tmpp2)
  .orderBy('stage', 'name', 'date_ym', 'selection_group')
)

# master date_ym index'
tmpp4 = (
  tmpp1
  .select('date_ym')
  .filter(f.col("date_ym")>=f.lit(plot_min_axis))
  .groupBy('date_ym')
  .agg(f.count(f.lit(1)).alias('_tmp'))
  .toPandas()
)

tmpp = (
  tmpp3d
  .filter(f.col("date_ym")>=f.lit(plot_min_axis))
  .toPandas()
)

# COMMAND ----------

tmpp_all = tmpp

import matplotlib.dates as mdates
plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(3, 2, figsize=(12.2,3*5), sharex=False) #  # was 4.75 for 2

vlist = list(
  smoking_last\
    .select('name')\
    .distinct()\
    .orderBy('name')\
    .toPandas()['name']
)
name_list = vlist + vlist
name_list.sort()  
print(f'name_list = {name_list}')

colors = sns.color_palette("tab10")[0:3]

j = -1
for i, var in enumerate(name_list):
  j = j + 1 
  ax = axes.flatten()[j]
  print('############', j, var, ax)
  #ax.xaxis_date()
  if(i % 2 == 0): 
    tmpp1 = tmpp_all[(tmpp_all['name'] == var) & (tmpp_all['stage'] == 'pre-selection')].copy()
  if(i % 2 == 1): 
    print('    1st')
    tmpp1 = tmpp_all[(tmpp_all['name'] == var) & (tmpp_all['stage'] == 'post-selection')].copy()

  # tmpp1 = tmpp1[tmpp1['source'] == 'gdppr']  ## HEREEEEEEEEE
    
  # tmpp1['code_des_len'] = tmpp1['code_des'].str.len()
  # tmpp1['code_des_trunc'] = np.where(tmpp1['code_des_len'] > 70, tmpp1['code_des'].str[:71] + '...', tmpp1['code_des'])    
  tmpp1['date_ym_formatted'] = pd.to_datetime(tmpp1['date_ym'], errors='coerce', format="%Y-%m")
  tmpp1.date_ym_formatted = pd.to_datetime(tmpp1.date_ym_formatted)
  # tmpp1 = tmpp1[tmpp1['date_ym'] >= '1990-01']
  tmpp2 = pd.pivot_table(tmpp1, index=tmpp1.date_ym_formatted.dt.date, columns='selection_group', values='n', aggfunc='sum')
  
  
  tmpp4['date_ym_formatted'] = pd.to_datetime(tmpp4['date_ym'], errors='coerce', format="%Y-%m")
  tmpp4.date_ym_formatted = pd.to_datetime(tmpp4.date_ym_formatted).dt.date
  # tmp4 = tmp4[tmp4['date_ym'] >= '1990-01']
  tmpp4 = tmpp4.set_index('date_ym_formatted') 
  # tmp4 = tmp4.drop(columns=['_tmp'])

  
  
  tmpp3 = pd.concat([tmpp2, tmpp4], axis=1).sort_index() # master date index
  
  #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
  tmpp3.plot.bar(ax=ax, stacked=True, width=1, edgecolor='black', linewidth=0.5, color=colors[0:len(tmpp1.selection_group.unique())] ) # sns.color_palette("tab10", len(tmpp1.code.unique())))
#   ax.set_xticklabels([x.strftime("%Y-%m") for x in tmpp2.index])
  
  monthly_timestamps = [timestamp for idx, timestamp in enumerate(tmpp3.index)
                      if (timestamp.month != tmpp3.index[idx-1].month) | (idx == 0)]

  timestamps = monthly_timestamps[::2]

#   # Create tick labels from timestamps
  labels = [ts.strftime('%Y-%m') for idx, ts in enumerate(timestamps)]

#   # Set major ticks and labels
  ax.set_xticks([tmpp3.index.get_loc(ts) for ts in timestamps])
  ax.set_xticklabels(labels)
  
  
  
  
  
  
# #   ax.xaxis.set_tick_params(rotation=90) # labelbottom=True)    
# #   ax.set(xlim=(tmp_min - datetime.timedelta(days=1), tmp_max))  
# #  ax.xaxis.set_major_locator(matplotlib.dates.MonthLocator())
# #  ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y-%m'))
  #  ax.set_xticks(ax.get_xticks()[::2])
  
#   # remove none code_des as a result of merging common xaxis above
#   _dict = {}
#   tmpu1 = tmpp1[['_grp', 'code_des_trunc']]\
#     .sort_values(['_grp', 'code_des_trunc'])\
#     .reset_index(drop=True)\
#     .drop_duplicates(subset=['_grp'], keep='first')
#   # print(tmpu1)
#   tmpu1_len = len(tmpu1)
#   for index, row in tmpu1.iterrows():
#     # print(row['_grp'], row['code_des'])  
#     _dict[str(row['_grp'])] = row['code_des_trunc']
 
  handles, labels = ax.get_legend_handles_labels()
  labels2 = []
  for handle, label in zip(handles, labels):
    print(handle, label)
    if(label != 'selection_group'): 
      labels2 = labels2 + [label] # + [_dict[label]]
      # handle.set_marker('o')
      # handle.set_markersize(3)
    else: labels2 = labels2 + [label]
#   for handle in handles:
#     print(handle)
#     handle.set_marker('o')
      
  ax.spines['right'].set_visible(False)
  ax.spines['top'].set_visible(False)
  if(i % 2 == 0): ax.set(ylabel="Frequency\n")
  else: ax.set(ylabel="")
  ax.set(xlabel='\nDate\n')
  
  # keep ylim consistent for each row (name)    
  if(i % 2 == 0): 
    _ylim = ax.get_ylim()
    print(_ylim)
  if(i % 2 == 1): 
    _ylim = ax.set_ylim(_ylim)


 
  if(i % 2 == 1): 
    # ax.legend(loc='upper left')
    ax.legend(labels=labels2, loc='upper left', bbox_to_anchor=(0.01, 1.01), frameon=False, fontsize=8, ncol=1, borderpad=0, labelspacing=0.2, handletextpad=0.6, borderaxespad=0.5, handlelength=1.2, title='Selection group')   
  else: 
    #ax.legend([], frameon=False)
    ax.legend(labels=labels2, loc='upper left', bbox_to_anchor=(0.01, 1.01), frameon=False, fontsize=8, ncol=1, borderpad=0, labelspacing=0.2, handletextpad=0.6, borderaxespad=0.5, handlelength=1.2, title='Selection group')   # , alignment='left'
    
  ax.get_legend()._legend_box.align = "left"

  # ax.get_legend().set_title("")
  
# #   tmptxt1 = _dict1[var]['name_des']
# #   tmptxt2 = _dict1[var]['n']
# #   tmptxt3 = _dict1[var]['n_id_distinct'] 
# #   tmptxt4 = _dict1[var]['n_code']
# #   ax.set_title(label=f'{var}\n{tmptxt1}\nN_records={tmptxt2:,}; N_individuals={tmptxt3:,}; N_codes={tmptxt4:,}', loc='left', fontsize=8, fontweight='bold')
  
  tmptxt0 = var.upper()
#   if(tmptxt0 == 'stroke'): tmptxt0 = 'Stroke'
#   if(tmptxt0 == 'hypertension'): tmptxt0 = 'Hypertension'
#   ttt = _dict1[var]['name_des']
  tmptxt1 = '' # f'({ttt})'
#   if(tmptxt0 == 'Hypertension'): tmptxt1 = ''
#   # tmptxt2 = _dict1[v]['n']
#   # tmptxt3 = _dict1[v]['n_id_distinct'] 
#   # tmptxt4 = _dict1[v]['n_code']
  tmptxt5 = 'All records' # 'All events'
  if(i % 2 == 1): tmptxt5 = 'Selected record per individual' # 'First event per individual'

  if(i % 2 == 0): ax.set_title(label=f'{tmptxt0} {tmptxt1}\n\n{tmptxt5}', loc='left', fontsize=8, fontweight='bold')
  if(i % 2 == 1): ax.set_title(label=f'\n\n{tmptxt5}', loc='left', fontsize=8, fontweight='bold')  

  # plt.suptitle('My title',fontsize=24, y=1)
  ax.set_yticklabels(['{:,}'.format(int(x)) for x in ax.get_yticks().tolist()])
  
plt.tight_layout();
display(fig)


# COMMAND ----------

# MAGIC %md # 5. Medications

# COMMAND ----------

# check codelist
print(codelist_medications.orderBy('name', 'code').toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md Now we assign selection groups in order to identify an individuals censor end date in relation to the closest record date (to baseline) in the highest priority group.
# MAGIC
# MAGIC **Note that medications groups are treated independently and we are finding an individuals censor end date for each medication group.**
# MAGIC
# MAGIC That is, the cloest to baseline date is derived for each medication group. E.g. , BP Lowering has a censor end date independent of Statins and Metformin.

# COMMAND ----------

# MAGIC %md ##5.1 Codelist match

# COMMAND ----------

# DBTITLE 1,Codelist match - to obtain closest to baseline records for each smoking group
_meds_in = {
  'pmeds': ['pmeds_max_prepared', 'codelist_medications',  1]
}

_meds, _meds_last, _meds_last_wide = codelist_match(_meds_in, _name_prefix=f'cov_meds_', _last_event=1)
_meds_summ_name, _meds_summ_name_code = codelist_match_summ(_meds_in, _meds)

# COMMAND ----------

# DBTITLE 1,Save
# temp save
meds_out_all = _meds['all']
meds_out_all = temp_save(df=meds_out_all, out_name=f'{proj}_tmp_covariates_meds_out_all_{cohort}'); print() #cohortrefactoring

meds_out_last = temp_save(df=_meds_last, out_name=f'{proj}_tmp_covariates_meds_out_last_{cohort}'); print() #cohortrefactoring
meds_out_last_wide = temp_save(df=_meds_last_wide, out_name=f'{proj}_tmp_covariates_meds_out_last_wide_{cohort}'); print() #cohortrefactoring

meds_out_summ_name = temp_save(df=_meds_summ_name, out_name=f'{proj}_tmp_covariates_meds_out_summ_name_{cohort}'); print() #cohortrefactoring
meds_out_summ_name_code = temp_save(df=_meds_summ_name_code, out_name=f'{proj}_tmp_covariates_meds_out_summ_name_code_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

# DBTITLE 1,Read data
meds_all = spark.table(f'{dsa}.{proj}_tmp_covariates_meds_out_all_{cohort}') #cohortrefactoring
meds_last = spark.table(f'{dsa}.{proj}_tmp_covariates_meds_out_last_{cohort}') #cohortrefactoring
meds_last_wide = spark.table(f'{dsa}.{proj}_tmp_covariates_meds_out_last_wide_{cohort}') #cohortrefactoring
meds_summ_name = spark.table(f'{dsa}.{proj}_tmp_covariates_meds_out_summ_name_{cohort}') #cohortrefactoring
meds_summ_name_code = spark.table(f'{dsa}.{proj}_tmp_covariates_meds_out_summ_name_code_{cohort}') #cohortrefactoring

# COMMAND ----------

assert meds_last_wide.count() == meds_last_wide.select("PERSON_ID").distinct().count()

# COMMAND ----------

count_var(meds_last_wide, 'PERSON_ID')

# COMMAND ----------

display(meds_last_wide.orderBy('PERSON_ID'))

# COMMAND ----------

# MAGIC %md ##5.2 Check

# COMMAND ----------

# MAGIC %md ###5.2.1 Summaries

# COMMAND ----------

# # check codelist match summary by name and source
# add commas
df_all_summ_name_commas = meds_summ_name
for colx in [col for col in meds_summ_name.columns if re.match('^n_.*$', col)]:
  df_all_summ_name_commas = (
    df_all_summ_name_commas
    .withColumn(colx, f.format_number(colx, 0))
  )
display(df_all_summ_name_commas.orderBy('name'))

# COMMAND ----------

# add commas
df_all_summ_name_code_commas = meds_summ_name_code
for colx in [col for col in meds_summ_name_code.columns if (re.match('^n_.*$', col)) or (re.match('^n$', col))]:
  df_all_summ_name_code_commas = (
    df_all_summ_name_code_commas
    .withColumn(colx, f.format_number(colx, 0))
  )
display(df_all_summ_name_code_commas.orderBy('name', 'terminology', 'code'))

# COMMAND ----------

display(meds_all.distinct().orderBy("PERSON_ID","name","DATE"))

# COMMAND ----------

count_var(meds_last_wide, 'PERSON_ID')

# COMMAND ----------

display(meds_last_wide.orderBy("PERSON_ID"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Shielding

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Codelist match

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
_shielding_in = {
  'gdppr': ['gdppr_max_prepared_shielding', 'codelist_shielding',  1]
}

_shielding, _shielding_last, _shielding_last_wide = codelist_match(_shielding_in, _name_prefix=f'cov_shielding_', _last_event=1)

# COMMAND ----------

# temp save
shielding_out_all = _shielding['all']
shielding_out_all = temp_save(df=shielding_out_all, out_name=f'{proj}_tmp_covariates_shielding_out_all_{cohort}'); print() #cohortrefactoring
shielding_out_last = temp_save(df=_shielding_last, out_name=f'{proj}_tmp_covariates_shielding_out_last_{cohort}'); print() #cohortrefactoring
shielding_out_last_wide = temp_save(df=_shielding_last_wide, out_name=f'{proj}_tmp_covariates_shielding_out_last_wide_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

shielding_out_all = spark.table(f'{dsa}.{proj}_tmp_covariates_shielding_out_all_{cohort}') #cohortrefactoring
shielding_last = spark.table(f'{dsa}.{proj}_tmp_covariates_shielding_out_last_{cohort}') #cohortrefactoring
shielding_last_wide = spark.table(f'{dsa}.{proj}_tmp_covariates_shielding_out_last_wide_{cohort}') #cohortrefactoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Checks

# COMMAND ----------

assert shielding_last_wide.count() == shielding_last_wide.select("PERSON_ID").distinct().count()

# COMMAND ----------

count_var(shielding_last_wide, 'PERSON_ID')

# COMMAND ----------

display(shielding_last_wide.orderBy('PERSON_ID'))

# COMMAND ----------

# MAGIC %md # 7. Consultation rates

# COMMAND ----------

# MAGIC %md Counts:
# MAGIC - Number of GP visits in the last 5 years: source GDPPR
# MAGIC - Number of hospital visits in the last 5 years: source HES APC

# COMMAND ----------

individual_censor_dates_consultations = (
  cohort_dataset
  .select('PERSON_ID', 'study_start_date') 
  .withColumn('CENSOR_DATE_START', f.add_months(f.col('study_start_date'), -12*5)) #lookback 5 years
  .withColumn('CENSOR_DATE_END', f.col('study_start_date')) #study_start_date
  .drop('study_start_date')
)

# check
count_var(individual_censor_dates_consultations, 'PERSON_ID'); print()
tmpt = tabstat(individual_censor_dates_consultations, 'CENSOR_DATE_START', date=1); print()
tmpt = tabstat(individual_censor_dates_consultations, 'CENSOR_DATE_END', date=1); print()
print(individual_censor_dates_consultations.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ##7.1 GP visits

# COMMAND ----------

# Consultation rate: number of primary care contacts in the 5 years prior to study start date; GP;

# restricted to individuals with DOB before study_start_date
#   since for individuals with a DOB after this date, the baseline date is their DOB, 
#   and we would not expect to find any consultations before when they were born 

# TODO - ask AS whether it would be helpful to have 0's for those born after 20200101 (c.f., nulls)

# gdppr - reduce and filter
# note: critically, cohort includes only those "in_gdppr", so no records in GDPPR => 0 consultations (although may be a small number who move out of and into practices not included in GDPPR)
_gdppr = (gdppr
          .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'RECORD_DATE')
          .withColumn('study_start_date',f.lit(study_start_date))
          .where(f.col('PERSON_ID').isNotNull())
          .where(f.col('RECORD_DATE').isNotNull())
          .where((f.col('RECORD_DATE') > f.add_months(f.col('study_start_date'), -12*5)) & (f.col('RECORD_DATE') <= f.to_date(f.col('study_start_date'))))
          .drop('study_start_date')
          .distinct())

# check
count_var(_gdppr, 'PERSON_ID'); print()

# merge
_n_consultations_gp = (
  merge(_gdppr, individual_censor_dates_consultations.select('PERSON_ID'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0)
  .groupBy('PERSON_ID')\
  .agg(f.count(f.lit(1)).alias('cov_n_consultations_gp'))); print()

# merge
# replace nulls with zeros when merging into cohort (i.e., for those with no records in GDPPR in previous year)
_n_consultations_gp = (
  merge(_n_consultations_gp, individual_censor_dates_consultations.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0)
  .na.fill(value=0, subset=['cov_n_consultations_gp'])); print()

# check
count_var(_n_consultations_gp, 'PERSON_ID'); print()
print(_n_consultations_gp.limit(10).toPandas().to_string()); print()
tmpt = tab(_n_consultations_gp, 'cov_n_consultations_gp'); print()


# COMMAND ----------

tmpt = tabstat(_n_consultations_gp, 'cov_n_consultations_gp'); print()

# temp save
_n_consultations_gp = temp_save(df=_n_consultations_gp, out_name=f'{proj}_tmp_covariates_n_consultations_gp_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

# read data
_n_consultations_gp = spark.table(f'{dsa}.{proj}_tmp_covariates_n_consultations_gp_{cohort}') #cohortrefactoring
display(_n_consultations_gp)

# COMMAND ----------

# MAGIC %md ##7.2 HES APC admissions

# COMMAND ----------

# should be moved to parameters
hes_apc_otr = spark.table(f'{dbc}.hes_apc_otr_all_years_archive') # ccu004-03 {dsa} changed to {dbc}
# s3a://nhsd-data-refinery-compute-prod-immuta-native-workspaces//.db/hes_apc_otr_all_years

# hes_apc_otr last archived_on version in relation to pipeline_production_date
hes_apc_otr_archived_on = (
  hes_apc_otr
  .select("archived_on")
  .distinct()
  .where(f.col('archived_on') <= pipeline_production_date)
  .orderBy(f.desc('archived_on'))
  .limit(1)
)



hes_apc_otr = (hes_apc_otr
               .where(f.col('archived_on') == (hes_apc_otr_archived_on.collect()[0]['archived_on']))
               .select(f.col("PERSON_ID_DEID").alias('PERSON_ID'),"EPIKEY","SUSSPELLID")
)

# COMMAND ----------

_hes_apc = (
  hes_apc_long
  .select("PERSON_ID","EPIKEY","EPISTART")
  .withColumn('study_start_date',f.lit(study_start_date))
  .where(f.col('PERSON_ID').isNotNull())
  .distinct()
)

display(_hes_apc)

# COMMAND ----------

# add on SUSSPELLSID using apc_otr
_hes_apc_episodes_and_spells = _hes_apc.join(hes_apc_otr,on=["PERSON_ID","EPIKEY"],how="left")
assert _hes_apc.count() == _hes_apc_episodes_and_spells.count()

display(_hes_apc_episodes_and_spells)

# COMMAND ----------

# An admission, or spell, is defined as a continuous period of time spent as a patient within a trust, and may include more than one episode. 
# count no of episodes for each spell

display(_hes_apc_episodes_and_spells.select("SUSSPELLID","EPIKEY").groupBy("SUSSPELLID").count().withColumnRenamed("count", "epi_count").groupBy("epi_count").count().orderBy("epi_count"))

# COMMAND ----------

# Consultation rate: number of secondary contacts in the 5 years prior to study start date; HES APC Episodes then HES APC Spells;

# NOTE that a spell may contain more episodes that fall outside the censor start and end dates

hes_apc_episodes_and_spells_prepared = (_hes_apc_episodes_and_spells
          .where(f.col('PERSON_ID').isNotNull())
          .where((f.col('EPISTART') > f.add_months(f.col('study_start_date'), -12*5)) & (f.col('EPISTART') <= f.to_date(f.col('study_start_date'))))
          .drop('study_start_date')
          .distinct())

# check
count_var(hes_apc_episodes_and_spells_prepared, 'PERSON_ID'); print()

# EPISODES ---------------------------------------------------------------------------------------

# merge
_n_consultations_hes_episodes = (
  merge(hes_apc_episodes_and_spells_prepared, individual_censor_dates_consultations.select('PERSON_ID'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0)
  .groupBy('PERSON_ID')\
  .agg(f.count(f.lit(1)).alias('cov_n_consultations_hes_episodes'))); print()

# merge
_n_consultations_hes_episodes = (
  merge(_n_consultations_hes_episodes, individual_censor_dates_consultations.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0)
  .na.fill(value=0, subset=['cov_n_consultations_hes_episodes'])); print()

# add in checks...

# temp save
_n_consultations_hes_episodes = temp_save(df=_n_consultations_hes_episodes, out_name=f'{proj}_tmp_covariates_n_consultations_hes_episodes_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

# read data
_n_consultations_hes_episodes = spark.table(f'{dsa}.{proj}_tmp_covariates_n_consultations_hes_episodes_{cohort}') #cohortrefactoring
display(_n_consultations_hes_episodes)

# COMMAND ----------

# SPELLS -----------------------------------------------------------------------------------------
hes_apc_spells_only_prepared = (
  hes_apc_episodes_and_spells_prepared.drop("EPIKEY","EPISTART").distinct()
)

# merge
_n_consultations_hes_spells = (
  merge(hes_apc_spells_only_prepared, individual_censor_dates_consultations.select('PERSON_ID'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0)
  .groupBy('PERSON_ID')\
  .agg(f.count(f.lit(1)).alias('cov_n_consultations_hes_spells'))
)

# merge
_n_consultations_hes_spells = (
  merge(_n_consultations_hes_spells, individual_censor_dates_consultations.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0)
  .na.fill(value=0, subset=['cov_n_consultations_hes_spells'])
)

# temp save
_n_consultations_hes_spells = temp_save(df=_n_consultations_hes_spells, out_name=f'{proj}_tmp_covariates_n_consultations_hes_spells_{cohort}'); print() #cohortrefactoring

# COMMAND ----------

# read data
_n_consultations_hes_spells = spark.table(f'{dsa}.{proj}_tmp_covariates_n_consultations_hes_spells_{cohort}') #cohortrefactoring
display(_n_consultations_hes_spells)

# COMMAND ----------

# check episodes in relation to spells
hes_final = (
  _n_consultations_hes_episodes
  .join(_n_consultations_hes_spells, on=['PERSON_ID'], how='outer')
)

display(hes_final.filter(f.col("cov_n_consultations_hes_spells")!=f.col("cov_n_consultations_hes_episodes")))

# COMMAND ----------

# MAGIC %md # 7. Save

# COMMAND ----------

covariates_final = (
  _n_consultations_gp
  .join(_n_consultations_hes_episodes, on=['PERSON_ID'], how='outer')
  .join(_n_consultations_hes_spells, on=['PERSON_ID'], how='outer')
  .join(smoking_last_wide_status.drop("CENSOR_DATE_START","CENSOR_DATE_END"), on=['PERSON_ID'], how='outer')
  .join(meds_last_wide.drop("CENSOR_DATE_START","CENSOR_DATE_END"), on=['PERSON_ID'], how='outer')
  .join(shielding_last_wide.drop("CENSOR_DATE_START","CENSOR_DATE_END"), on=['PERSON_ID'], how='outer')
  .dropDuplicates()
)

# COMMAND ----------

covariates_final.select("PERSON_ID").distinct().count()

# COMMAND ----------

# check everyone here is in out cohort
count_var((covariates_final), 'PERSON_ID')

# COMMAND ----------

display(covariates_final)

# COMMAND ----------

save_table(df=covariates_final, out_name=f'{proj}_out_covariates_{cohort}', save_previous=True) #cohortrefactoring