# Databricks notebook source
# MAGIC %md # CCU004_03-D02e_codelist_covariates_markers
# MAGIC
# MAGIC **Project** CCU004_03
# MAGIC
# MAGIC **Description** This notebook creates the covariate markers codelist needed for CCU004_03:
# MAGIC
# MAGIC - BMI
# MAGIC - Systolic Blood pressure
# MAGIC - Total cholesterol
# MAGIC - HDL-cholesterol
# MAGIC - HbA1c 
# MAGIC - eGFR
# MAGIC - Creatinine
# MAGIC - Height
# MAGIC - Weight
# MAGIC
# MAGIC **Author(s)** Tom Bolton (Health Data Science Team, BHF Data Science Centre), Carmen Petitjean and adapted by Spencer Keene 
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton, John Nolan, and earlier projects including CCU051_D02a_codelist_covid and CCU002_07.
# MAGIC
# MAGIC **Data output**
# MAGIC - **`ccu004_03_out_codelist_covariates_markers`** : Codes for all covariate markers

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
# pmeds_bnf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
#pmeds_bnf = spark.table(f'{dbc}.{outName}')
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

# MAGIC %md ## 4.1 Measurements

# COMMAND ----------

# BMI
# Systolic Blood pressure
# Total cholesterol
# HDL-cholesterol
# HbA1c 
# eGFR
# Creatinine
# Weight
# Height

# COMMAND ----------

# MAGIC %md ### 4.1.1 BMI

# COMMAND ----------

# to be picked by Carmen for CCU004
list_bmi = [
  '722595002'
  , '914741000000103'
  , '914731000000107'
  , '914721000000105'
  , '35425004'
  , '48499001'
  , '301331008'
  , '6497000'
  , '310252000'
  , '427090001'
  , '408512008'
  , '162864005'
  , '162863004'
  , '412768003'
  , '60621009'
  , '846931000000101'
]
tmp1_bmi = (
  gdppr_refset
  .select('ConceptId', 'ConceptId_description')
  .where(f.col('ConceptId').isin(list_bmi))
  .dropDuplicates(['ConceptId'])
  .select(f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
  .orderBy('code')
)

# check
print(tmp1_bmi.toPandas().to_string()); print()

# BMIVAL_COD
tmp2_bmi = (
  gdppr_refset
  .select('Cluster_ID', 'Cluster_Desc', 'ConceptId', 'ConceptId_description')
  .where(f.col('Cluster_ID').isin(['BMIVAL_COD']))
  .dropDuplicates(['ConceptId'])
  .select(f.col('ConceptId').alias('code'))
  .orderBy('code')
)

# check
print(tmp2_bmi.toPandas().to_string()); print()

# merge
tmp3_bmi = merge(tmp1_bmi, tmp2_bmi, ['code'], validate='1:1', assert_results=['both', 'left_only'])

# check
print(tmp3_bmi.toPandas().to_string()); print()

# prepare
codelist_bmi = (
  tmp3_bmi
  .withColumn('name', f.lit('bmi'))
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', 'code', 'term')
)  

# check
tmpt = tab(codelist_bmi, 'name', 'terminology'); print()
print(codelist_bmi.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 4.1.2 Systolic blood pressure 

# COMMAND ----------

tmp1_sbp = (
  gdppr_refset
  .where(f.col('Cluster_ID') == 'BP_COD')
  .dropDuplicates(['ConceptId'])
  .select('ConceptId', 'ConceptId_description')
  .withColumn('flag_sys', f.when(f.lower(f.col('ConceptId_description')).rlike('systolic'), 1).otherwise(0))
  .withColumn('flag_dia', f.when(f.lower(f.col('ConceptId_description')).rlike('diastolic'), 1).otherwise(0))
  .withColumn('flag', 
              f.when(f.col('flag_sys') == 1, 'sys')
              .when(f.col('flag_dia') == 1, 'dia')
              .otherwise('gen')
             )
)

# check
tmpt = tab(tmp1_sbp, 'flag_sys', 'flag_dia'); print()
assert tmp1_sbp.where((f.col('flag_sys') == 1) & (f.col('flag_dia') == 1)).count() == 0
tmpt = tab(tmp1_sbp, 'flag'); print()
print(tmp1_sbp.drop('flag_sys', 'flag_dia').orderBy('ConceptId').toPandas().to_string()); print()

# filter
tmp2_sbp = (
  tmp1_sbp
  .where(f.col('flag').isin(['sys', 'gen']))
  .drop('flag_sys', 'flag_dia')
)

# check
tmpt = tab(tmp2_sbp, 'flag'); print()

# prepare
codelist_sbp = (
  tmp2_sbp
  .withColumn('name', f.lit('sbp'))
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
)  

# check
tmpt = tab(codelist_sbp, 'name'); print()
print(codelist_sbp.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# # check
# with pd.option_context('display.max_colwidth', None): 
#   tmpt = tab(tmp3, 'Cluster_ID', 'Cluster_Desc', var2_wide=0); print()

# Question: filter further? e.g., Maximum measurements - remove?

# COMMAND ----------

# MAGIC %md ### 4.1.3 Others

# COMMAND ----------

list_cod = ['CHOL2_COD', 'HDLCCHOL_COD', 'IFCCHBAM_COD', 'EGFR_COD', 'CRE_COD', 'NDAWEIGHT_COD', 'NDAHEIGHT_COD']
tmp_meas = (
  gdppr_refset
  .select('Cluster_ID', 'Cluster_Desc', 'ConceptId', 'ConceptId_description')
  .where(f.col('Cluster_ID').isin(list_cod))
  .withColumn('name',
    f.when(f.col('Cluster_ID') == 'CHOL2_COD',    f.lit('tchol'))
     .when(f.col('Cluster_ID') == 'HDLCCHOL_COD', f.lit('hdl'))
     .when(f.col('Cluster_ID') == 'IFCCHBAM_COD', f.lit('hba1c'))
     .when(f.col('Cluster_ID') == 'EGFR_COD',     f.lit('egfr'))
     .when(f.col('Cluster_ID') == 'CRE_COD',      f.lit('creat')) #ccu004-03
     .when(f.col('Cluster_ID') == 'NDAWEIGHT_COD',      f.lit('weight')) #ccu004-03
     .when(f.col('Cluster_ID') == 'NDAHEIGHT_COD',      f.lit('height')) #ccu004-03
     .otherwise(f.col('Cluster_ID'))
  )
  .withColumn('terminology', f.lit('SNOMED'))
)  
  
# check  
tmpt = tab(tmp_meas, 'name', 'Cluster_ID'); print()
with pd.option_context('display.max_colwidth', None): 
  tmpt = tab(tmp_meas, 'Cluster_ID', 'Cluster_Desc', var2_wide=0); print()
tmpt = tab(tmp_meas, 'name', 'terminology'); print()
print(tmp_meas.drop('Cluster_ID', 'Cluster_Desc').orderBy('name', 'terminology', 'ConceptId').toPandas().to_string()); print()  
    
# prepare
codelist_meas = (
  tmp_meas
  .select('name', 'terminology', f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
)
 
# check
tmpt = tab(codelist_meas, 'name', 'terminology'); print()
print(codelist_meas.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()  

# COMMAND ----------

# The cluster IDs for weight and height don't actually correspond to measured weight and height. It is rather measures vaguely related to weight and height. Therefore, we have to make sure to remove the unrelated codes

#  Defining exclusion codes for weight: these relate to children weights, percentage measurements, BMI measurements, weight change amount, etc. Many of the leftover codes may not all carry weight measurment. For example codes like "obesity class 1" could have a weight associated with it, but it is not always necessary. We hope that the "acceptable ranges" will filter the nonsense values  
weight_excl = spark.createDataFrame(['162863004', '162864005', '408512008', '276712009', '719809005', '722051004', '722595002', '248358009', '783549006', '783719006', '785722006', '1153592008', '1153593003', '1153599004', '1153600001', '1153601002', '102492002', '1162416001', '140096001', '147237000', '147238005', '147239002', '147240000', '147241001', '147242008', '147243003', '147244009', '147245005', '147246006', '147247002', '147248007', '147249004', '147250004', ' 147251000', '147252007', '147253002', '147254008', '147255009', '147256005', '147257001', '147259003', '147260008', '147261007', '147262000', '147263005', '1556501000000100', '1556521000000109', '162879003', '170006002', '170007006', '170008001', '170009009', '170010004', '170011000', '170012007', '170013002', '170014008', '170015009', '170016005', '170017001', '170018006', '170019003', '170020009', '170021008', '170022001', '170023006', '170024000', '170025004', '170026003', '170027007', '170028002', '170029005', '170030000', '170031001', '248347000', '248348005', '248349002', '248353000', '248354006', '248355007', '248357004', '248360006', '276355006', '314669003', '314671003', '314671003', '314672005', '314673000', '314674006', '314675007', '314676008', '314677004', '314678009', '314679001', '314680003', '314681004', '314682006', '314683001', '314684007', '314685008', '314686009', '351000119100', '363805003', '363806002', '450451007', '707471000000104', '717923002', '758931000000104', '816159004', '816160009', '914721000000105', '914731000000107', '914741000000103', '917151000000106', '917161000000109', '917171000000102'], "string").toDF("exclude_weight_code")

display(weight_excl)

# Defining exclusion codes for height. Similar codes as above: relate to children, describe rates, percentage measurments etc.
height_excl = spark.createDataFrame(['1153600001', '1153601002', '1153604005', '1153605006', '1162392001', '1251557006', '140096001', '162879003', '170034009', '170035005', '170037002', '170038007', '170039004', '170040002', '170041003', '248338008', '248340003', '248358009', '248360006', '268482007', '302033003', '314687000', '314688005', '314689002', '314690006', '314691005', '314692003', '314693008', '314694002', '314695001', '314696000', '314697009', '314698004', '314700008', '314701007', '314702000', '314703005', '314756007', '758931000000104', '845541000000105', '870593000', '926081000000108'], "string").toDF("exclude_height_code")

display(height_excl)

# COMMAND ----------

col1 = 'code'
col2 = 'exclude_height_code'

codelist_meas = codelist_meas.join(height_excl, codelist_meas[col1]==height_excl[col2], how = 'left_anti')

col3 = 'code'
col4 = 'exclude_weight_code'

codelist_meas = codelist_meas.join(weight_excl, codelist_meas[col3]==weight_excl[col4], how = 'left_anti')

# COMMAND ----------

# MAGIC %md # 5. Combine

# COMMAND ----------

display(codelist_meas)

# COMMAND ----------

# append (union) codelists defined above
# harmonise columns before appending
codelist_all = []
for indx, clist in enumerate([clist for clist in globals().keys() if (bool(re.match('^codelist_.*', clist))) & (clist not in ['codelist_match', 'codelist_match_summ', 'codelist_match_stages_to_run', 'codelist_match_v2_test', 'codelist_tmp', 'codelist_all', 'codelist_nonmatch'])]): #ccu004-03 added codelist_nonmatch
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

# check
# old = spark.table(f'{dbc}.{proj}_out_codelist_covariates')
# new = codelist
# key = ['name', 'terminology', 'code']
# file1, file2, file3, file3_differences = compare_files(old, new, key, warningError=0)

# COMMAND ----------

# tmpt = tab(file1, 'name'); print()
# tmpt = tab(file2, 'name', 'name_old_TWO'); print()

# COMMAND ----------

# MAGIC %md # 9. Save

# COMMAND ----------

save_table(df=codelist, out_name=f'{proj}_out_codelist_covariates_markers', save_previous=True)