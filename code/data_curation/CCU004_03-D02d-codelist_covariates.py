# Databricks notebook source
# MAGIC %md # CCU004_03-D02d_codelist_covariates
# MAGIC
# MAGIC **Project** CCU004_03
# MAGIC
# MAGIC **Description** This notebook creates the covariates codelist needed for CCU004_03:
# MAGIC
# MAGIC - Smoking
# MAGIC - Medications:
# MAGIC   - Statin
# MAGIC   - Blood pressure
# MAGIC   - Metformin
# MAGIC
# MAGIC **Author(s)** Tom Bolton (Health Data Science Team, BHF Data Science Centre), Carmen Petitjean and adapted by Spencer Keene 
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton, John Nolan, and earlier projects including CCU051_D02a_codelist_covid and CCU002_07.
# MAGIC
# MAGIC **Data output**
# MAGIC - **`ccu004_03_out_codelist_covariates`** : Codes for all covariates

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

bhf_phenotypes = spark.table(path_ref_bhf_phenotypes)

spark.sql(f'REFRESH TABLE dss_corporate.gdppr_cluster_refset')
gdppr_refset = spark.table(path_ref_gdppr_refset)

pmeds = spark.table(f'{dbc}.primary_care_meds_{db}_archive').where(f.col('archived_on') == '2023-01-31' )

gdppr = spark.table(f'{dbc}.gdppr_{db}_archive').where(f.col('archived_on') == '2023-02-28' )

bnf = spark.table(f'dss_corporate.bnf_code_information')

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# ------------------------------------------------------------------------------
# bhf_phenotypes
# ------------------------------------------------------------------------------
# check
tmpt = tab(bhf_phenotypes, 'name', 'terminology'); print()

# reduce (2 duplicates within - PE and liver_disease)
bhf_phenotypes = (
  bhf_phenotypes
  .select('name', 'terminology', 'code', 'term', 'code_type', 'RecordDate')
  .dropDuplicates()
)

# cache
bhf_phenotypes.cache()
print(f'{bhf_phenotypes.count():,}'); print()

# check
tmpt = tab(bhf_phenotypes, 'name', 'terminology'); print()

# COMMAND ----------

# ------------------------------------------------------------------------------
# bnf_pmeds
# ------------------------------------------------------------------------------
win = Window\
  .partitionBy('PrescribedBNFCode')\
  .orderBy('PrescribedBNFName')
pmeds_bnf = (
  pmeds
  .select('PrescribedBNFCode', 'PrescribedBNFName')
  .withColumn('rownum', f.row_number().over(win))
  .where(f.col('rownum') == 1)
  .drop('rownum')
)

#ccu004-03
# temp save
#outName = f'{proj}_tmp_pmeds_bnf'.lower() 
#outName = spark.table('.ccu004_01_tmp_pmeds_bnf')
# pmeds_bnf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
pmeds_bnf = spark.table('.ccu004_01_tmp_pmeds_bnf')

# check
count_var(pmeds_bnf, 'PrescribedBNFCode'); print()
print(pmeds_bnf.orderBy('PrescribedBNFCode').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# check
with pd.option_context('display.max_colwidth', None): 
  tmpt = tab(gdppr_refset, 'Cluster_ID', 'Cluster_Desc', var2_wide=0); print()
display(tmpt)

# COMMAND ----------

# MAGIC %md # 4. Codelists

# COMMAND ----------

# MAGIC %md ## 4.1 Smoking

# COMMAND ----------

# smoking_(current|ex|never)
# 20220707 updated 6 smoking_ex SNOMED codes that had been rounded - tb
# 20230505 below notes relate to codelists excel spreadsheet - fc
# 20230505 removed current status for 160625004 (now only has ex status) - fc
# 20230505 removed ex status for 230058003 (now only has current status) - fc
# 20230505 removed ex status for 230065006 (now only has current status) - fc
# 20230505 removed ex status for 160625004 (now only has current status) - fc

tmp_smoking_status = spark.createDataFrame(
  [
    ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
    ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
    ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
    ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
    ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
    ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
    ("230058003","Pipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
    ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
    ("230065006","Chain smoker (finding)","Current-smoker","Heavy"),
    ("266918002","Tobacco smoking consumption (observable entity)","Current-smoker","Unknown"),
    ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
    ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
    ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
    ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
    ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
    ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
    ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
    ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
    ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
    ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
    ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
    ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
    ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
    ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
    ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
    ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
    ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
    ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
    ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
    ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
    ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
    ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
    ("77176002","Smoker (finding)","Current-smoker","Unknown"),
    ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
    ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
    ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
    ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
    ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
    ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
    #("160625004","Date ceased smoking (observable entity)","Current-smoker","Unknown"),
    ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
    ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
    ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
    ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
    ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
    ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
    ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
    ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
    ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
    ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
    ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
    ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
    ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
    ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
    ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
    ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
    ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
    ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
    ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
    ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
    ("401201003","Cigarette pack-years (observable entity)","Current-smoker","Unknown"),
    ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
    ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
    ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
    ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
    ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
    ("77176002","Smoker (finding)","Current-smoker","Unknown"),
    ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
    ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("1092041000000104","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("1092091000000109","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
    ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
    ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
    ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
    ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
    ("1092031000000108","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("1092071000000105","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("1092111000000104","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("1092131000000107","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
    ("160625004","Date ceased smoking (observable entity)","Ex-smoker","Unknown"),
    ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
    ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
    ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
    ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
    ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
    ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
    ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
    ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
    ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
    ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
    #("230058003","Pipe tobacco consumption (observable entity)","Ex-smoker","Unknown"),
    #("230065006","Chain smoker (finding)","Ex-smoker","Unknown"),
    #("266918002","Tobacco smoking consumption (observable entity)","Ex-smoker","Unknown"),
    ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
    ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
    ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
    ("266919005","Never smoked tobacco (finding)","Never-smoker","NA"),
    ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
    ("266919005","Never smoked tobacco (finding)","Never-smoker","NA")
  ],
  ['code', 'term', 'smoking_status', 'severity']
)
codelist_smoking_status = (
  tmp_smoking_status
  .distinct()
  .withColumn('name',
    f.when(f.col('smoking_status') == 'Current-smoker', f.lit('smoking_current'))
     .when(f.col('smoking_status') == 'Ex-smoker', f.lit('smoking_ex'))
     .when(f.col('smoking_status') == 'Never-smoker', f.lit('smoking_never'))
     .otherwise(f.col('smoking_status'))
  )
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', 'code', 'term')
)  

# check
tmpt = tab(codelist_smoking_status, 'name', 'terminology'); print()
print(codelist_smoking_status.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# codes with a Current AND Ex status
tmp1 = (
  tmp_smoking_status
  .select("smoking_status","code")
  .distinct()
  .groupBy("code")
  .pivot("smoking_status").agg(f.lit(1)).na.fill(value=0)
  .withColumn('Current_AND_Ex_smoker',f.when((f.col('Current-smoker') == 1) & (f.col('Ex-smoker') == 1), f.lit(1)).otherwise(f.lit(0)))
  .filter(f.col("Current_AND_Ex_smoker")==1)
  .select("code")
) 

tmp2=tmp_smoking_status.select("code","term").distinct()

# previously 4 codes had a current and ex status but in the chunk they have been commented out as at the note for date 20230505
display(tmp1.join(tmp2,tmp1.code==tmp2.code,"left"))

# COMMAND ----------

# MAGIC %md ## 4.2 Medications

# COMMAND ----------

# MAGIC %md ### 4.2.1 Statin

# COMMAND ----------

# --------------------------------------------------------------------------
# statin
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/0212/
# https://www.opencodelists.org/codelist/opensafely/statin-medication/2020-04-20/

# Atorvastatin (0212000B0)
# # # Cerivastatin (0212000C0) # withdrawn # No prescriptions found
# Fenofibrate/simvastatin (0212000AJ)
# Fluvastatin sodium (0212000M0)
# Pravastatin sodium (0212000X0)
# Rosuvastatin calcium (0212000AA)
# Simvastatin (0212000Y0)
# Simvastatin and ezetimibe (0212000AC)

codelist_statin = (
  pmeds_bnf
  .where(
    (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000B0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000AJ')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000M0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000X0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000AA')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000Y0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0212000AC')
  )
  .withColumn('name', f.lit('statin'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_statin.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
print(codelist_statin.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 4.2.2 Blood pressure lowering

# COMMAND ----------

# --------------------------------------------------------------------------
# bp_lowering
# --------------------------------------------------------------------------
# 0204 -- beta blockers
#   exclude 0204000R0 -- propranolol
#   exclude 0204000Q0 -- propranolol
# 020502 -- centrally acting antihypertensives
#   exclude 0205020G -- guanfacine because it is only used for ADHD
# 020504 -- alpha blockers
# 020602 -- calcium channel blockers
# 020203 -- potassium sparing diuretics
# 020201 -- thiazide diuretics
# 020501 -- vasodilator antihypertensives
# 0205051 -- angiotensin-converting enzyme inhibitors
# 0205052 -- angiotensin-II receptor antagonists
#   exclude 0205052AE -- drugs for heart failure, not for hypertension
# 0205053A0 -- aliskiren
codelist_bp_lowering = (
  pmeds_bnf
  .where(
    (
      (f.substring(f.col('PrescribedBNFCode'), 1, 4) == '0204')
        & ~(
          (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0204000R0')
            | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0204000Q0')
        )
    )
    | (
      (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020502')
        & ~(
          (f.substring(f.col('PrescribedBNFCode'), 1, 8) == '0205020G')
        )
    )
    | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020504')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020602')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020203')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020201')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 6) == '020501')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 7) == '0205051')
    | (
       (f.substring(f.col('PrescribedBNFCode'), 1, 7) == '0205052')
        & ~(
           (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0205052AE')
        )
    )
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0205053A0')
  )
  .withColumn('name', f.lit('bp_lowering'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_bp_lowering.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
print(codelist_bp_lowering.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 4.2.3 Metformin

# COMMAND ----------

# --------------------------------------------------------------------------
# metformin
# --------------------------------------------------------------------------
# https://openprescribing.net/bnf/060102/

# Alogliptin/metformin (0601023AJ)
# Canagliflozin/metformin (0601023AP)
# Dapagliflozin/metformin (0601023AL)
# Empagliflozin/metformin (0601023AR)
# Linagliptin/metformin (0601023AF)
# Metformin hydrochloride (0601022B0)
# Metformin hydrochloride/pioglitazone (0601023W0)
# Metformin hydrochloride/rosiglitazone (0601023V0)
# Metformin hydrochloride/sitagliptin (0601023AD)
# Metformin hydrochloride/vildagliptin (0601023Z0)
# Saxagliptin/metformin (0601023AH)
codelist_metformin = (
  pmeds_bnf
  .where(
      (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AJ')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AP')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AL')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AR')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AF')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601022B0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023W0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023V0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AD')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023Z0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601023AH')    
  )
  .withColumn('name', f.lit('metformin'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

# check
tmpt = tab(codelist_metformin.withColumn('code_9', f.substring(f.col('code'), 1, 9)), 'code_9'); print()
print(codelist_metformin.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 4.2.4 Insulin

# COMMAND ----------

# --------------------------------------------------------------------------
# insulin
# --------------------------------------------------------------------------
#name	terminology	code	term
#insulin	BNF	0601012W0	Biphasic insulin aspart
#insulin	BNF	0601012F0	Biphasic insulin lispro
#insulin	BNF	0601012D0	Biphasic isophane insulin
#insulin	BNF	0601011A0	Insulin aspart
#insulin	BNF	0601012Z0	Insulin degludec***REMOVED***
#insulin	BNF	0601012X0	Insulin detemir
#insulin	BNF	0601012V0	Insulin glargine
#insulin	BNF	0601012AB	Insulin glargine/lixisenatide
#insulin	BNF	0601011P0	Insulin glulisine
#insulin	BNF	0601011R0	Insulin human
#insulin	BNF	0601011L0	Insulin Lispro
#insulin	BNF	0601012G0	Insulin zinc suspension
#insulin	BNF	0601012S0	Isophane insulin
#insulin	BNF	0601012U0	Protamine zinc insulin
#insulin	BNF	0601011N0	Soluble insulin (Neutral insulin)

tmp_codelist_insulin = (
  pmeds_bnf
  .where(
      (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012W0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012F0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012D0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011A0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012Z0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012X0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012V0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012AB')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011P0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011R0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011L0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012G0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012S0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601012U0')
    | (f.substring(f.col('PrescribedBNFCode'), 1, 9) == '0601011N0')
  )
  .withColumn('name', f.lit('insulin'))
  .withColumn('terminology', f.lit('BNF'))
  .select('name', 'terminology', f.col('PrescribedBNFCode').alias('code'), f.col('PrescribedBNFName').alias('term'))
  .orderBy('name', 'terminology', 'code', 'term')    
)

tmp_insulin_icd = spark.createDataFrame(
  [
    ('insulin','ICD10','Y423','Insulin and oral hypoglycaemic [antidiabetetic drugs]')
  ],
  ['name', 'terminology', 'code', 'term']  
)

tmp_insulin_snomed = spark.createDataFrame(
  [
    ('insulin','SNOMED','67866001','Insulin (substance)'),
    ('insulin','SNOMED','4700006','Beef insulin (substance)'),
    ('insulin','SNOMED','10329000','Zinc insulin (substance)'),
    ('insulin','SNOMED','706937006','Free insulin (substance)'),
    ('insulin','SNOMED','69805005','Insulin pump, device (physical object)'),
    ('insulin','SNOMED','67296003','Pork insulin (substance)'),
    ('insulin','SNOMED','789480007','Insulin dose (observable entity)'),
    ('insulin','SNOMED','246491008','Insulin used (attribute)'),
    ('insulin','SNOMED','96367001','Human insulin (substance)'),
    ('insulin','SNOMED','412210000','Insulin lispro (substance)'),
    ('insulin','SNOMED','325072002','Insulin aspart (substance)')
  ],
  ['name', 'terminology', 'code', 'term']
)

codelist_insulin_tmp2 = (
  tmp_codelist_insulin
  .union(tmp_insulin_icd)
)

codelist_insulin = (
    codelist_insulin_tmp2
    .union(tmp_insulin_snomed)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Shielding
# MAGIC
# MAGIC Codes to identify individuals on the shielding list during the COVID-19 pandemic. 
# MAGIC
# MAGIC Please note that there were three codes used for classifying risk and an individual could change category over time:
# MAGIC * 1300561000000107     High risk category for developing complication from coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 infection (finding)
# MAGIC * 1300571000000100     Moderate risk category for developing complication from coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 infection (finding)
# MAGIC * 1300591000000101     Low risk category for developing complication from coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 infection (finding) 

# COMMAND ----------

codelist_shielding = spark.createDataFrame(
  [
    ("shielding", "SNOMED", "1300561000000107","High risk category for developing complication from coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 infection (finding)")
  ], 
  ['name', 'terminology', 'code', 'term']
)

# COMMAND ----------

# MAGIC %md # 5. Combine

# COMMAND ----------

# append (union) codelists defined above
# harmonise columns before appending
codelist_all = []
for indx, clist in enumerate([clist for clist in globals().keys() if (bool(re.match('^codelist_.*', clist))) & (clist not in ['codelist_match', 'codelist_match_summ', 'codelist_match_stages_to_run', 'codelist_match_v2_test', 'codelist_tmp', 'codelist_all', 'codelist_nonmatch'])]):
  print(f'{0 if indx<10 else ""}' + str(indx) + ' ' + clist)
  codelist_tmp = globals()[clist]
  if(indx == 0):
    codelist_all = codelist_tmp
  else:
    # pre unionByName
    for col in [col for col in codelist_tmp.columns if col not in codelist_all.columns]:
      # print('  M - adding column: ' + col)
      codelist_all = codelist_all.withColumn(col, f.lit(None))
    for col in [col for col in codelist_all.columns if col not in codelist_tmp.columns]:
      # print('  C - adding column: ' + col)
      codelist_tmp = codelist_tmp.withColumn(col, f.lit(None))
      
    # unionByName  
    codelist_all = (
      codelist_all
      .unionByName(codelist_tmp)
    )
  
# order  
codelist = (
  codelist_all
  .orderBy('name', 'terminology', 'code')
)

# COMMAND ----------

# check
display(codelist)

# COMMAND ----------

tmpt = tab(codelist, 'name', 'terminology'); print()

# COMMAND ----------

# MAGIC %md # 6. Reformat

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
codelist = (
  codelist
  .withColumn('_code_old', f.col('code'))
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))
  .withColumn('_code_diff', f.when(f.col('code') != f.col('_code_old'), 1).otherwise(0))
)

# check
tmpt = tab(codelist, '_code_diff'); print()
print(codelist.where(f.col('_code_diff') == 1).orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# tidy
codelist = codelist.drop('_code_old', '_code_diff')

# COMMAND ----------

# MAGIC %md # 7. Exclusions

# COMMAND ----------

# 20230424 Carmen advised:
# Exclude these:			
			
# name	terminology	code	term
# sbp	SNOMED	"775671000000104"	Post exercise systolic blood pressure response normal (finding)
# sbp	SNOMED	"707303003"	Post exercise systolic blood pressure response abnormal (finding)
# sbp	SNOMED	"707304009"	Post exercise systolic blood pressure response normal (finding)

# COMMAND ----------

# check
tmpt = tab(codelist, 'name', 'terminology'); print()

# flag
codelist = (
  codelist
  .withColumn('flag_exclusion', 
              f.when((f.col('name') == 'sbp') & (f.col('code').isin(['775671000000104', '707303003', '707304009'])), 1)
              .otherwise(0)
             )
)

# check
tmpt = tab(codelist, 'flag_exclusion')
print(codelist.where(f.col('flag_exclusion') == 1).orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# filter
codelist = (
  codelist
  .where(f.col('flag_exclusion') == 0)
)

# check
tmpt = tab(codelist, 'name', 'terminology'); print()

# tidy
codelist = codelist.drop('flag_exclusion')

# check
print(codelist.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 8. Check

# COMMAND ----------

# check 
tmpt = tab(codelist, 'name', 'terminology')

# COMMAND ----------

# check
display(codelist)

# COMMAND ----------

# check
display(codelist.where(f.col('terminology') == 'ICD10'))

# COMMAND ----------

# MAGIC %md # 9. Save

# COMMAND ----------

save_table(df=codelist, out_name=f'{proj}_out_codelist_covariates', save_previous=True)