# Databricks notebook source
# MAGIC %md # CCU004_03-D02f_codelist_covariates_chronic_conditions
# MAGIC
# MAGIC **Project** CCU004_03
# MAGIC
# MAGIC **Description** This notebook creates the covariates codelist needed for CCU004_03.
# MAGIC
# MAGIC **Author(s)** Tom Bolton (Health Data Science Team, BHF Data Science Centre), Carmen Petitjean and adapted by Spencer Keene 
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton, John Nolan, and earlier projects including CCU051_D02a_codelist_covid and CCU002_07.
# MAGIC
# MAGIC **Data output**
# MAGIC - **`ccu004_03_out_codelist_covariates_chronic_conditions`** : Codes for all covariates

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

# COMMAND ----------

# ICD 10 codes lookup (for adding additional ICD10 codes in codelists section)
path_ref_icd10 = 'dss_corporate.icd10_group_chapter_v01'
icd10 = spark.table(path_ref_icd10)

# COMMAND ----------

# Obesity
phenotypes_obesity_icd = spark.table('bhf_cvd_covid_uk_byod.caliber_icd_obesity')
phenotypes_obesity_snomed = spark.table('bhf_cvd_covid_uk_byod.caliber_cprd_obesity_snomedct')

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

# MAGIC %md # 4. Codelists

# COMMAND ----------

# MAGIC %md ## 4.1 Chronic conditions

# COMMAND ----------

# MAGIC %md ### 4.1.2 Depression

# COMMAND ----------

# depression
codelist_depression = (
  bhf_phenotypes
  .where(f.col('name') == 'depression')
)

# EXCLUSIONS
codelist_depression = (
  codelist_depression.filter(~f.col("code").isin("19527009","698957003"))
)

# check
tmpt = tab(codelist_depression, 'name', 'terminology'); print()
print(codelist_depression.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 4.1.3 Cancer

# COMMAND ----------

# cancer
# 20220616 ER advised to remove code 417036008 "Liquid based cervical cytology screening (procedure)"
codelist_cancer = (
  bhf_phenotypes
  .where(f.col('name') == 'cancer')
  .where(f.col('code') != '417036008')
)


# EXCLUSIONS
codelist_cancer = (
  codelist_cancer.filter(~f.col("code").isin("190955000"))
)

# check
tmpt = tab(codelist_cancer, 'name', 'terminology'); print()
print(codelist_cancer.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 4.1.4 Asthma

# COMMAND ----------

# 20230616 - additional codes requested by Carmen J45 and J46 - using this as a lookup - FC
icd10.filter(f.col("CODE").startswith("J45")).select(f.col("ALT_CODE"),f.col("ICD10_DESCRIPTION")).collect()
icd10.filter(f.col("CODE").startswith("J46")).select(f.col("ALT_CODE"),f.col("ICD10_DESCRIPTION")).collect()

# COMMAND ----------

codelist_asthma = spark.createDataFrame(
[
("asthma","SNOMED","1064811000000103","moderate acute exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","1064821000000109","life threatening acute exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","10674711000119105","acute severe exacerbation of asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10675391000119101","severe controlled persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","10675431000119106","severe persistent allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","10675471000119109","acute severe exacerbation of severe persistent allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","10675551000119104","acute severe exacerbation of severe persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10675591000119109","severe persistent allergic asthma controlled (finding)","1","20220215"),
("asthma","SNOMED","10675631000119109","severe persistent asthma controlled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10675671000119107","severe persistent allergic asthma uncontrolled (finding)","1","20220215"),
("asthma","SNOMED","10675711000119106","severe persistent asthma uncontrolled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10675751000119107","severe uncontrolled persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","10675871000119106","mild persistent allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","10675911000119109","acute severe exacerbation of mild persistent allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","10675991000119100","acute severe exacerbation of mild persistent allergic asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10676031000119106","mild persistent allergic asthma controlled (finding)","1","20220215"),
("asthma","SNOMED","10676071000119109","mild persistent asthma controlled (finding)","1","20220215"),
("asthma","SNOMED","10676111000119102","mild persistent asthma controlled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10676151000119101","mild persistent allergic asthma uncontrolled (finding)","1","20220215"),
("asthma","SNOMED","10676191000119106","mild persistent asthma uncontrolled (finding)","1","20220215"),
("asthma","SNOMED","10676231000119102","mild persistent asthma uncontrolled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10676351000119103","moderate persistent asthma controlled (finding)","1","20220215"),
("asthma","SNOMED","10676391000119108","moderate persistent allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","10676431000119103","acute severe exacerbation of moderate persistent allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","10676511000119109","acute severe exacerbation of moderate persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10676551000119105","moderate persistent allergic asthma controlled (finding)","1","20220215"),
("asthma","SNOMED","10676591000119100","moderate persistent controlled asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10676631000119100","moderate persistent allergic asthma uncontrolled (finding)","1","20220215"),
("asthma","SNOMED","10676671000119102","moderate persistent asthma uncontrolled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","10676711000119103","moderate persistent asthma uncontrolled (finding)","1","20220215"),
("asthma","SNOMED","10692721000119102","chronic obstructive asthma co-occurrent with acute exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","1086701000000102","life threatening acute exacerbation of allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","1086711000000100","life threatening acute exacerbation of intrinsic asthma (disorder)","1","20220215"),
("asthma","SNOMED","1103911000000103","severe asthma with fungal sensitisation (disorder)","1","20220215"),
("asthma","SNOMED","11641008","millers' asthma (disorder)","1","20220215"),
("asthma","SNOMED","12428000","intrinsic asthma without status asthmaticus (disorder)","1","20220215"),
("asthma","SNOMED","124991000119109","severe persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","125001000119103","moderate persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","125011000119100","mild persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","125021000119107","intermittent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","135171000119106","acute exacerbation of moderate persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","135181000119109","acute exacerbation of mild persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","16584951000119101","oral steroid-dependent asthma (disorder)","1","20220215"),
("asthma","SNOMED","1741000119102","intermittent asthma uncontrolled (disorder)","1","20220215"),
("asthma","SNOMED","1751000119100","acute exacerbation of chronic obstructive airways disease with asthma (disorder)","1","20220215"),
("asthma","SNOMED","18041002","printers' asthma (disorder)","1","20220215"),
("asthma","SNOMED","195967001","asthma (disorder)","1","20220215"),
("asthma","SNOMED","195977004","mixed asthma (disorder)","1","20220215"),
("asthma","SNOMED","225057002","brittle asthma (disorder)","1","20220215"),
("asthma","SNOMED","233679003","late onset asthma (disorder)","1","20220215"),
("asthma","SNOMED","233683003","hay fever with asthma (disorder)","1","20220215"),
("asthma","SNOMED","233688007","sulfite-induced asthma (disorder)","1","20220215"),
("asthma","SNOMED","233691007","asthmatic pulmonary eosinophilia (disorder)","1","20220215"),
("asthma","SNOMED","2360001000004109","steroid dependent asthma (disorder)","1","20220215"),
("asthma","SNOMED","266361008","non-allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","281239006","exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","30352005","allergic-infective asthma (disorder)","1","20220215"),
("asthma","SNOMED","304527002","acute asthma (disorder)","1","20220215"),
("asthma","SNOMED","312453004","asthma - currently active (finding)","1","20220215"),
("asthma","SNOMED","31387002","exercise-induced asthma (disorder)","1","20220215"),
("asthma","SNOMED","34015007","bakers' asthma (disorder)","1","20220215"),
("asthma","SNOMED","370218001","mild asthma (disorder)","1","20220215"),
("asthma","SNOMED","370219009","moderate asthma (disorder)","1","20220215"),
("asthma","SNOMED","370220003","occasional asthma (disorder)","1","20220215"),
("asthma","SNOMED","370221004","severe asthma (disorder)","1","20220215"),
("asthma","SNOMED","389145006","allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","395022009","asthma night-time symptoms (finding)","1","20220215"),
("asthma","SNOMED","401000119107","asthma with irreversible airway obstruction (disorder)","1","20220215"),
("asthma","SNOMED","401193004","asthma confirmed (situation)","1","20220215"),
("asthma","SNOMED","404804003","platinum asthma (disorder)","1","20220215"),
("asthma","SNOMED","404806001","cheese-makers' asthma (disorder)","1","20220215"),
("asthma","SNOMED","404808000","isocyanate induced asthma (disorder)","1","20220215"),
("asthma","SNOMED","405944004","asthmatic bronchitis (disorder)","1","20220215"),
("asthma","SNOMED","407674008","aspirin-induced asthma (disorder)","1","20220215"),
("asthma","SNOMED","409663006","cough variant asthma (disorder)","1","20220215"),
("asthma","SNOMED","41553006","detergent asthma (disorder)","1","20220215"),
("asthma","SNOMED","418395004","tea-makers' asthma (disorder)","1","20220215"),
("asthma","SNOMED","423889005","non-immunoglobulin e mediated allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","424199006","substance induced asthma (disorder)","1","20220215"),
("asthma","SNOMED","424643009","immunoglobulin e-mediated allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","425969006","exacerbation of intermittent asthma (disorder)","1","20220215"),
("asthma","SNOMED","426656000","severe persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","426979002","mild persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","427295004","moderate persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","427603009","intermittent asthma (disorder)","1","20220215"),
("asthma","SNOMED","427679007","mild intermittent asthma (disorder)","1","20220215"),
("asthma","SNOMED","442025000","acute exacerbation of chronic asthmatic bronchitis (disorder)","1","20220215"),
("asthma","SNOMED","445427006","seasonal asthma (disorder)","1","20220215"),
("asthma","SNOMED","55570000","asthma without status asthmaticus (disorder)","1","20220215"),
("asthma","SNOMED","56968009","asthma caused by wood dust (disorder)","1","20220215"),
("asthma","SNOMED","57607007","occupational asthma (disorder)","1","20220215"),
("asthma","SNOMED","63088003","allergic asthma without status asthmaticus (disorder)","1","20220215"),
("asthma","SNOMED","641000119106","intermittent asthma well controlled (finding)","1","20220215"),
("asthma","SNOMED","703953004","allergic asthma caused by dermatophagoides pteronyssinus (disorder)","1","20220215"),
("asthma","SNOMED","703954005","allergic asthma caused by dermatophagoides farinae (disorder)","1","20220215"),
("asthma","SNOMED","707444001","uncomplicated asthma (disorder)","1","20220215"),
("asthma","SNOMED","707445000","exacerbation of mild persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","707446004","exacerbation of moderate persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","707447008","exacerbation of severe persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","707511009","uncomplicated mild persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","707512002","uncomplicated moderate persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","707513007","uncomplicated severe persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","707979007","acute severe exacerbation of severe persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","707980005","acute severe exacerbation of moderate persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","707981009","acute severe exacerbation of mild persistent asthma (disorder)","1","20220215"),
("asthma","SNOMED","708038006","acute exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","708090002","acute severe exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","708093000","acute exacerbation of immunoglobulin e-mediated allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","708094006","acute exacerbation of intrinsic asthma (disorder)","1","20220215"),
("asthma","SNOMED","708095007","acute severe exacerbation of immunoglobin e-mediated allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","708096008","acute severe exacerbation of intrinsic asthma (disorder)","1","20220215"),
("asthma","SNOMED","72301000119103","asthma in pregnancy (disorder)","1","20220215"),
("asthma","SNOMED","733858005","acute severe refractory exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","734904007","life threatening acute exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","734905008","moderate acute exacerbation of asthma (disorder)","1","20220215"),
("asthma","SNOMED","735587000","acute severe exacerbation of asthma co-occurrent and due to allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","735588005","uncomplicated allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","735589002","uncomplicated non-allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","762521001","exacerbation of allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","782513000","acute severe exacerbation of allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","782520007","exacerbation of allergic asthma due to infection (disorder)","1","20220215"),
("asthma","SNOMED","786836003","near fatal asthma (disorder)","1","20220215"),
("asthma","SNOMED","866881000000101","chronic asthma with fixed airflow obstruction (disorder)","1","20220215"),
("asthma","SNOMED","92807009","chemical-induced asthma (disorder)","1","20220215"),
("asthma","SNOMED","93432008","drug-induced asthma (disorder)","1","20220215"),
("asthma","SNOMED","99031000119107","acute exacerbation of asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
("asthma","SNOMED","183478001","emergency hospital admission for asthma (procedure)","1","20220215"),
("asthma","SNOMED","708358003","emergency asthma admission since last encounter (situation)","1","20220215"),
("asthma","SNOMED","170631002","asthma disturbing sleep (finding)","1","20220215"),
("asthma","SNOMED","170632009","asthma causing night waking (finding)","1","20220215"),
("asthma","SNOMED","170633004","asthma disturbs sleep weekly (finding)","1","20220215"),
("asthma","SNOMED","170634005","asthma disturbs sleep frequently (finding)","1","20220215"),
("asthma","SNOMED","170635006","asthma not disturbing sleep (finding)","1","20220215"),
("asthma","SNOMED","170636007","asthma never disturbs sleep (finding)","1","20220215"),
("asthma","SNOMED","170637003","asthma limiting activities (finding)","1","20220215"),
("asthma","SNOMED","170638008","asthma not limiting activities (finding)","1","20220215"),
("asthma","SNOMED","170655007","asthma restricts exercise (finding)","1","20220215"),
("asthma","SNOMED","170656008","asthma sometimes restricts exercise (finding)","1","20220215"),
("asthma","SNOMED","170657004","asthma severely restricts exercise (finding)","1","20220215"),
("asthma","SNOMED","170658009","asthma never restricts exercise (finding)","1","20220215"),
("asthma","SNOMED","270442000","asthma monitoring check done (finding)","1","20220215"),
("asthma","SNOMED","370202007","asthma causes daytime symptoms 1 to 2 times per month (finding)","1","20220215"),
("asthma","SNOMED","370203002","asthma causes daytime symptoms 1 to 2 times per week (finding)","1","20220215"),
("asthma","SNOMED","370204008","asthma causes daytime symptoms most days (finding)","1","20220215"),
("asthma","SNOMED","370205009","asthma causes night symptoms 1 to 2 times per month (finding)","1","20220215"),
("asthma","SNOMED","370206005","asthma limits walking on the flat (finding)","1","20220215"),
("asthma","SNOMED","370207001","asthma limits walking up hills or stairs (finding)","1","20220215"),
("asthma","SNOMED","370208006","asthma never causes daytime symptoms (finding)","1","20220215"),
("asthma","SNOMED","373899003","asthma daytime symptoms (finding)","1","20220215"),
("asthma","SNOMED","390872009","change in asthma management plan (regime/therapy)","1","20220215"),
("asthma","SNOMED","390877003","step up change in asthma management plan (regime/therapy)","1","20220215"),
("asthma","SNOMED","390878008","step down change in asthma management plan (regime/therapy)","1","20220215"),
("asthma","SNOMED","390921001","absent from work or school due to asthma (finding)","1","20220215"),
("asthma","SNOMED","394700004","asthma annual review (regime/therapy)","1","20220215"),
("asthma","SNOMED","473391009","asthma never causes night symptoms (situation)","1","20220215"),
("asthma","SNOMED","771901000000100","asthma causes night time symptoms 1 to 2 times per week (finding)","1","20220215"),
("asthma","SNOMED","771941000000102","asthma causes symptoms most nights (finding)","1","20220215"),
("asthma","SNOMED","771981000000105","asthma limits activities 1 to 2 times per month (finding)","1","20220215"),
("asthma","SNOMED","772011000000107","asthma limits activities 1 to 2 times per week (finding)","1","20220215"),
("asthma","SNOMED","772051000000106","asthma limits activities most days (finding)","1","20220215"),
("asthma","SNOMED","811151000000105","number of days absent from school due to asthma in past 6 months (observable entity)","1","20220215"),
("asthma","SNOMED","10674991000119104","intermittent allergic asthma (disorder)","1","20220215"),
("asthma","SNOMED","829976001","thunderstorm asthma (disorder)","1","20220215"),
("asthma","SNOMED","782559003","asthma never causes night symptoms (finding)","1","20220215"),

# additional codes to include (ICD10)- fc
("asthma","ICD10","J450","Predominantly allergic asthma",'',''),
("asthma","ICD10","J451","Nonallergic asthma",'',''),
("asthma","ICD10","J458","Mixed asthma",'',''),
("asthma","ICD10","J459","Asthma, unspecifieda",'',''),
("asthma","ICD10","J46X","Status asthmaticusa",'','')
  
],
['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']   
)

# COMMAND ----------

# MAGIC %md ### 4.1.5 COPD

# COMMAND ----------

# 20230616 - additional codes requested by Carmen J40-J44 - using this as a lookup - FC
icd10.filter(f.col("CODE").startswith("J44")).select(f.col("ALT_CODE"),f.col("ICD10_DESCRIPTION")).collect()

# COMMAND ----------

codelist_copd = spark.createDataFrame(
[
("copd","SNOMED","106001000119101","chronic obstructive lung disease co-occurrent with acute bronchitis (disorder)","1","20220215"),
("copd","SNOMED","135836000","end stage chronic obstructive airways disease (disorder)","1","20220215"),
("copd","SNOMED","13645005","chronic obstructive lung disease (disorder)","1","20220215"),
("copd","SNOMED","16003001","giant bullous emphysema (disorder)","1","20220215"),
("copd","SNOMED","16846004","obstructive emphysema (disorder)","1","20220215"),
("copd","SNOMED","185086009","chronic obstructive bronchitis (disorder)","1","20220215"),
("copd","SNOMED","195951007","acute exacerbation of chronic obstructive airways disease (disorder)","1","20220215"),
("copd","SNOMED","195958001","segmental bullous emphysema (disorder)","1","20220215"),
("copd","SNOMED","195959009","zonal bullous emphysema (disorder)","1","20220215"),
("copd","SNOMED","196001008","chronic obstructive pulmonary disease with acute lower respiratory infection (disorder)","1","20220215"),
("copd","SNOMED","233674008","pulmonary emphysema in alpha-1 primary immunodeficiency deficiency (disorder)","1","20220215"),
("copd","SNOMED","266355005","bullous emphysema with collapse (disorder)","1","20220215"),
("copd","SNOMED","285381006","acute infective exacerbation of chronic obstructive airways disease (disorder)","1","20220215"),
("copd","SNOMED","293241000119100","acute exacerbation of chronic obstructive bronchitis (disorder)","1","20220215"),
("copd","SNOMED","293991000000106","very severe chronic obstructive pulmonary disease (disorder)","1","20220215"),
("copd","SNOMED","313296004","mild chronic obstructive pulmonary disease (disorder)","1","20220215"),
("copd","SNOMED","313297008","moderate chronic obstructive pulmonary disease (disorder)","1","20220215"),
("copd","SNOMED","313299006","severe chronic obstructive pulmonary disease (disorder)","1","20220215"),
("copd","SNOMED","31898008","paraseptal emphysema (disorder)","1","20220215"),
("copd","SNOMED","45145000","unilateral emphysema (situation)","1","20220215"),
("copd","SNOMED","4981000","panacinar emphysema (disorder)","1","20220215"),
("copd","SNOMED","66110007","chronic diffuse emphysema caused by inhalation of chemical fumes and/or vapors (disorder)","1","20220215"),
("copd","SNOMED","68328006","centriacinar emphysema (disorder)","1","20220215"),
("copd","SNOMED","847091000000104","acute non-infective exacerbation of chronic obstructive pulmonary disease (disorder)","1","20220215"),
("copd","SNOMED","87433001","pulmonary emphysema (disorder)","1","20220215"),
("copd","SNOMED","408501008","emergency hospital admission for chronic obstructive pulmonary disease (procedure)","1","20220215"),
("copd","SNOMED","1110861000000102","quality and outcomes framework chronic obstructive pulmonary disease quality indicator-related care invitation (procedure)","1","20220215"),
("copd","SNOMED","143371000000104","quality and outcomes framework chronic obstructive pulmonary disease quality indicator-related care invitation using preferred method of communication (procedure)","1","20220215"),
("copd","SNOMED","836477007","chronic emphysema caused by vapor (disorder)","1","20220215"),
("copd","SNOMED","1010333003","emphysema of left lung (disorder)","1","20220215"),
("copd","SNOMED","1010334009","emphysema of right lung (disorder)","1","20220215"), 

# additional codes to include (ICD10)- fc
("copd","ICD10","J41","Simple and mucopurulent chronic bronchitis",'',''),
("copd","ICD10","J410","Simple chronic bronchitis",'',''),
("copd","ICD10","J411","Mucopurulent chronic bronchitis",'',''),
("copd","ICD10","J418","Mixed simple and mucopurulent chronic bronchitis",'',''),
("copd","ICD10","J42X","Unspecified chronic bronchitis",'',''),
("copd","ICD10","J43","Emphysema",'',''),
("copd","ICD10","J430","MacLeod syndrome",'',''),
("copd","ICD10","J431","Panlobular emphysema",'',''),
("copd","ICD10","J432","Centrilobular emphysema",'',''),
("copd","ICD10","J438","Other emphysema",'',''),
("copd","ICD10","J439","Emphysema, unspecified",'',''),
("copd","ICD10","J44","Other chronic obstructive pulmonary disease",'',''),
("copd","ICD10","J440","Chronic obstructive pulmonary disease with acute lower respiratory infection",'',''),
("copd","ICD10","J441","Chronic obstructive pulmonary disease with acute exacerbation, unspecified",'',''),
("copd","ICD10","J448","Other specified chronic obstructive pulmonary disease",'',''),
("copd","ICD10","J449","Chronic obstructive pulmonary disease, unspecified",'','')
],
['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']   
)

# COMMAND ----------

# MAGIC %md ### 4.1.6 ILD

# COMMAND ----------

# 20230616 - additional codes requested by Carmen J80,J81,J82,J84 - using this as a lookup - FC
icd10.filter(f.col("CODE").startswith("J84")).select(f.col("ALT_CODE"),f.col("ICD10_DESCRIPTION")).collect()

# COMMAND ----------

codelist_ild = spark.createDataFrame(
[
("ILD","SNOMED","233748006","simple pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233749003","complicated pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233750003","erionite pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233751004","metal pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233754007","cerium pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233755008","nickel pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233756009","thorium pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233757000","zirconium pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233758005","mica pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233759002","mixed mineral dust pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","233761006","subacute silicosis (disorder)","1","20220215"),
("ILD","SNOMED","233762004","chronic silicosis (disorder)","1","20220215"),
("ILD","SNOMED","233764003","wollastonite pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","26511004","pneumoconiosis caused by sisal fiber (disorder)","1","20220215"),
("ILD","SNOMED","29422001","coal workers' pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","32139003","mixed dust pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","398640008","rheumatoid pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","40122008","pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","40218008","carbon electrode makers' pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","426853005","pneumoconiosis caused by silicate (disorder)","1","20220215"),
("ILD","SNOMED","58691003","antimony pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","73144008","pneumoconiosis caused by talc (disorder)","1","20220215"),
("ILD","SNOMED","805002","pneumoconiosis caused by silica (disorder)","1","20220215"),
("ILD","SNOMED","87909002","hard metal pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","32544004","chronic obliterative bronchiolitis caused by inhalation of chemical fumes and/or vapors (disorder)","1","20220215"),
("ILD","SNOMED","37711000","cadmium pneumonitis (disorder)","1","20220215"),
("ILD","SNOMED","47515009","simple silicosis (disorder)","1","20220215"),
("ILD","SNOMED","8549006","desquamative interstitial pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","236302005","acute interstitial pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","35037009","primary atypical interstitial pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","385479009","follicular bronchiolitis (disorder)","1","20220215"),
("ILD","SNOMED","430476004","diffuse panbronchiolitis (disorder)","1","20220215"),
("ILD","SNOMED","59903001","acute obliterating bronchiolitis (disorder)","1","20220215"),
("ILD","SNOMED","64667001","interstitial pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","704345008","chronic interstitial pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","10713006","diffuse interstitial rheumatoid disease of lung (disorder)","1","20220215"),
("ILD","SNOMED","129452008","nonspecific interstitial pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","196051003","drug-induced interstitial lung disorder (disorder)","1","20220215"),
("ILD","SNOMED","196133001","lung disease with systemic sclerosis (disorder)","1","20220215"),
("ILD","SNOMED","233692000","cryptogenic pulmonary eosinophilia (disorder)","1","20220215"),
("ILD","SNOMED","233770009","stage 4 pulmonary sarcoidosis (disorder)","1","20220215"),
("ILD","SNOMED","24369008","pulmonary sarcoidosis (disorder)","1","20220215"),
("ILD","SNOMED","30042003","confluent fibrosis of lung (disorder)","1","20220215"),
("ILD","SNOMED","34371004","subacute obliterative bronchiolitis caused by inhalation of chemical fumes and/or vapors (disorder)","1","20220215"),
("ILD","SNOMED","3514002","peribronchial fibrosis of lung (disorder)","1","20220215"),
("ILD","SNOMED","405570007","pulmonary fibrosis due to and following radiotherapy (disorder)","1","20220215"),
("ILD","SNOMED","427046006","drug-induced pneumonitis (disorder)","1","20220215"),
("ILD","SNOMED","54867000","rheumatoid fibrosing alveolitis (disorder)","1","20220215"),
("ILD","SNOMED","56841008","massive fibrosis of lung (disorder)","1","20220215"),
("ILD","SNOMED","700249006","idiopathic interstitial pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","707434003","pulmonary fibrosis due to hermansky-pudlak syndrome (disorder)","1","20220215"),
("ILD","SNOMED","719218000","cryptogenic organizing pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","737181009","interstitial lung disease due to systemic disease (disorder)","1","20220215"),
("ILD","SNOMED","737182002","interstitial lung disease due to granulomatous disease (disorder)","1","20220215"),
("ILD","SNOMED","737183007","interstitial lung disease due to metabolic disease (disorder)","1","20220215"),
("ILD","SNOMED","737184001","interstitial lung disease co-occurrent and due to systemic vasculitis (disorder)","1","20220215"),
("ILD","SNOMED","155621007","rheumatoid lung (disorder)","1","20220215"),
("ILD","SNOMED","196132006","rheumatoid lung (disorder)","1","20220215"),
("ILD","SNOMED","201794001","rheumatoid lung (disorder)","1","20220215"),
("ILD","SNOMED","201813004","(rheumatoid lung) or (caplan's syndrome) or (fibrosing alveolitis associated with rheumatoid arthritis) (disorder)","1","20220215"),
("ILD","SNOMED","239794002","fibrosing alveolitis associated with rheumatoid arthritis (disorder)","1","20220215"),
("ILD","SNOMED","319841000119107","rheumatoid lung disease with rheumatoid arthritis (disorder)","1","20220215"),
("ILD","SNOMED","398726004","rheumatoid lung disease (disorder)","1","20220215"),
("ILD","SNOMED","129451001","respiratory bronchiolitis associated interstitial lung disease (disorder)","1","20220215"),
("ILD","SNOMED","129458007","bronchiolitis obliterans organizing pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","14700006","bauxite fibrosis of lung (disorder)","1","20220215"),
("ILD","SNOMED","17385007","graphite fibrosis of lung (disorder)","1","20220215"),
("ILD","SNOMED","17996008","pneumoconiosis caused by inorganic dust (disorder)","1","20220215"),
("ILD","SNOMED","192658007","giant cell interstitial pneumonitis (disorder)","1","20220215"),
("ILD","SNOMED","196027008","toxic bronchiolitis obliterans (disorder)","1","20220215"),
("ILD","SNOMED","196053000","chronic drug-induced interstitial lung disorders (disorder)","1","20220215"),
("ILD","SNOMED","196125002","diffuse interstitial pulmonary fibrosis (disorder)","1","20220215"),
("ILD","SNOMED","22607003","asbestosis (disorder)","1","20220215"),
("ILD","SNOMED","233673002","drug-induced bronchiolitis obliterans (disorder)","1","20220215"),
("ILD","SNOMED","233703007","interstitial lung disease (disorder)","1","20220215"),
("ILD","SNOMED","233713004","seasonal cryptogenic organizing pneumonia with biochemical cholestasis (disorder)","1","20220215"),
("ILD","SNOMED","233723008","bronchiolitis obliterans with usual interstitial pneumonitis (disorder)","1","20220215"),
("ILD","SNOMED","233724002","toxic diffuse interstitial pulmonary fibrosis (disorder)","1","20220215"),
("ILD","SNOMED","233725001","drug-induced diffuse interstitial pulmonary fibrosis (disorder)","1","20220215"),
("ILD","SNOMED","233760007","acute silicosis (disorder)","1","20220215"),
("ILD","SNOMED","277844007","pulmonary lymphangioleiomyomatosis (disorder)","1","20220215"),
("ILD","SNOMED","36599006","chronic fibrosis of lung (disorder)","1","20220215"),
("ILD","SNOMED","40100001","obliterative bronchiolitis (disorder)","1","20220215"),
("ILD","SNOMED","40527005","idiopathic pulmonary hemosiderosis (disorder)","1","20220215"),
("ILD","SNOMED","40640008","massive fibrosis of lung co-occurrent and due to silicosis (disorder)","1","20220215"),
("ILD","SNOMED","427123006","interstitial lung disease due to collagen vascular disease (disorder)","1","20220215"),
("ILD","SNOMED","44274007","lymphoid interstitial pneumonia (disorder)","1","20220215"),
("ILD","SNOMED","47938003","chronic obliterative bronchiolitis (disorder)","1","20220215"),
("ILD","SNOMED","51615001","fibrosis of lung (disorder)","1","20220215"),
("ILD","SNOMED","62371005","pulmonary siderosis (disorder)","1","20220215"),
("ILD","SNOMED","711379004","interstitial lung disease due to connective tissue disease (disorder)","1","20220215"),
("ILD","SNOMED","71193007","fibrosis of lung caused by radiation (disorder)","1","20220215"),
("ILD","SNOMED","72270005","collagenous pneumoconiosis (disorder)","1","20220215"),
("ILD","SNOMED","196136009","lung disease co-occurrent with polymyositis (disorder)","1","20220215"),
("ILD","SNOMED","836478002","subacute obliterative bronchiolitis caused by chemical fumes (disorder)","1","20220215"),
("ILD","SNOMED","836479005","subacute obliterative bronchiolitis caused by vapor (disorder)","1","20220215"),
("ILD","SNOMED","840350008","chronic obliterative bronchiolitis caused by chemical fumes (disorder)","1","20220215"),
("ILD","SNOMED","840351007","chronic obliterative bronchiolitis caused by vapor (disorder)","1","20220215"),
("ILD","SNOMED","866103007","interstitial lung disease due to juvenile polymyositis (disorder)","1","20220215"),
("ILD","SNOMED","239297008","lymphomatoid granulomatosis of lung (disorder)","1","20220215"),
("ILD","SNOMED","870573008","interstitial pneumonia with autoimmune features (disorder)","1","20220215"),
("ILD","SNOMED","1017196003","interstitial pulmonary fibrosis due to inhalation of substance (disorder)","1","20220215"),
("ILD","SNOMED","1017197007","interstitial pulmonary fibrosis due to inhalation of drug (disorder)","1","20220215"),
("ILD","SNOMED","700252003","subacute idiopathic pulmonary fibrosis (disorder)","1","20220215"),
("ILD","SNOMED","708537005","acute idiopathic pulmonary fibrosis (disorder)","1","20220215"),
("ILD","SNOMED","426437004","familial idiopathic pulmonary fibrosis (disorder)","1","20220215"),
("ILD","SNOMED","700250006","idiopathic pulmonary fibrosis (disorder)","1","20220215"),
("ILD","SNOMED","700251005","chronic idiopathic pulmonary fibrosis (disorder)","1","20220215"),
("ILD","SNOMED","789574002","acute exacerbation of idiopathic pulmonary fibrosis (disorder)","1","20220215"),  

# additional codes to include (ICD10)- fc
("ILD","ICD10","J84","Other interstitial pulmonary diseases",'',''),
("ILD","ICD10","J840","Alveolar and parietoalveolar conditions",'',''),
("ILD","ICD10","J841","Other interstitial pulmonary diseases with fibrosis",'',''),
("ILD","ICD10","J848","Other specified interstitial pulmonary diseases",'',''),
("ILD","ICD10","J849","Interstitial pulmonary disease, unspecified",'','')

],
['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']   
)

# COMMAND ----------

# MAGIC %md ### 4.1.7 Bronchiectasis

# COMMAND ----------

# 20230616 - additional codes requested by Carmen J47 - using this as a lookup - FC
icd10.filter(f.col("CODE").startswith("J47")).select(f.col("ALT_CODE"),f.col("ICD10_DESCRIPTION")).collect()

# COMMAND ----------

codelist_bronchiectasis = spark.createDataFrame(
[
("bronchiectasis","SNOMED","12295008","bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","195984007","recurrent bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","195985008","post-infective bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","23022004","tuberculous bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","233627004","congenital cystic bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","233628009","acquired bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","233629001","idiopathic bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","233630006","obstructive bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","233631005","toxin-induced bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","714203003","acute bronchitis co-occurrent with bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","12310001","childhood bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","13217005","fusiform bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","19325002","traction bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","445378003","acute exacerbation of bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","51068008","adult bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","52500008","saccular bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","83457000","cylindrical bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","77593006","congenital bronchiectasis (disorder)","1","20220215"),
("bronchiectasis","SNOMED","879963005","exacerbation of bronchiectasis caused by infection (disorder)","1","20220215"),

# additional codes to include (ICD10)- fc
("bronchiectasis","ICD10","J47X","Bronchiectasis",'','')
  
],
['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']   
)

# COMMAND ----------

# MAGIC %md ### 4.1.8 Obesity

# COMMAND ----------

display(phenotypes_obesity_icd
       .select(f.col("Disease").alias("name"),f.col("ICD10code").alias("code"))
       .distinct())

icd10.filter(f.col("CODE").startswith("E66")).select(f.col("ALT_CODE"),f.col("ICD10_DESCRIPTION")).collect()

# COMMAND ----------

tmp_obesity_snomed = (phenotypes_obesity_snomed
       .withColumn("name",f.lit("obesity"))
       .withColumn("terminology",f.lit("SNOMED"))
       .withColumn("code_type",f.lit(""))
       .select("name","terminology",f.col("conceptId").alias("code"),"term","code_type","RecordDate")
                  )

tmp_obesity_icd = spark.createDataFrame(
  [
    ('obesity','ICD10','E66',   'Obesity','',''),
    ('obesity','ICD10','E660',  'Obesity due to excess calories','',''),
    ('obesity','ICD10','E661',  'Drug-induced obesity','',''),
    ('obesity','ICD10','E662',  'Extreme obesity with alveolar hypoventilation','',''),
    ('obesity','ICD10','E668',  'Other obesity','',''),
    ('obesity','ICD10','E669',  'Obesity, unspecified','','')
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)


codelist_obesity = (
  tmp_obesity_snomed
  .union(tmp_obesity_icd)
)

# EXCLUSIONS
codelist_obesity = (
  codelist_obesity.filter(~f.col("code").isin("248325000","428754001"))
)

# check
tmpt = tab(codelist_obesity, 'name', 'terminology'); print()
print(codelist_obesity.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 4.1.9 Underweight

# COMMAND ----------

codelist_underweight = spark.createDataFrame(
[
("underweight","SNOMED","248342006","Underweight (finding)","",""),
("underweight", "SNOMED","162769006", "On examination - Underweight (finding)", "", ""),
("underweight", "SNOMED","427090001", "Body mass index less than 16.5 (finding)", "", "")
],
['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']   
)

# COMMAND ----------

# MAGIC %md ### 4.1.10 Hypertension

# COMMAND ----------

# include drugs?
codelist_hypertension = (
  bhf_phenotypes
  .where(f.col('name').rlike('^hypertension.*$'))
)

# COMMAND ----------

display(codelist_hypertension)

# COMMAND ----------

# MAGIC %md ### 4.1.11 Hypotension

# COMMAND ----------

icd10.filter(f.col("CODE").startswith("I95")).select(f.col("ALT_CODE"),f.col("ICD10_DESCRIPTION")).collect()

# COMMAND ----------

codelist_hypotension = spark.createDataFrame(
[
("hypotension","ICD10","I95","Hypotension","",""),
("hypotension","ICD10","I950","Idiopathic hypotension","",""),
("hypotension","ICD10","I951","Orthostatic hypotension","",""),
("hypotension","ICD10","I952","Hypotension due to drugs","",""),
("hypotension","ICD10","I958","Other hypotension","",""),
("hypotension","ICD10","I959","Hypotension, unspecified","",""),

("hypotension","SNOMED","45007003","Low blood pressure (disorder)","",""),
("hypotension","SNOMED","77545000", "Chronic hypotension (disorder)", "", "") 
],
['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']   
)

# COMMAND ----------

# MAGIC %md # 5. Combine

# COMMAND ----------

# append (union) codelists defined above
# harmonise columns before appending
codelist_all = []
for indx, clist in enumerate([clist for clist in globals().keys() if (bool(re.match('^codelist_.*', clist))) & (clist not in ['codelist_match', 'codelist_match_summ', 'codelist_match_stages_to_run', 'codelist_match_v2_test', 'codelist_tmp', 'codelist_all', 'codelist_nonmatch', 'codelist_nonmatch_death_check', 'codelist_diabetes'])]):  #ccu004-03 added codelist_nonmatch. codelist_diabetes added to not in
  print(f'{0 if indx<10 else ""}' + str(indx) + ' ' + clist)
  codelist_tmp = globals()[clist]
  #codelist_tmp = pd.DataFrame([codelist_tmp]) #ccu004-03
  #codelist_tmp = pd.DataFrame(codelist_tmp, dtype='category') #ccu004-03
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

display(codelist_all)

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

# MAGIC %md # 7. Check

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

save_table(df=codelist, out_name=f'{proj}_out_codelist_covariates_chronic_conditions', save_previous=True)

# COMMAND ----------

display(codelist)