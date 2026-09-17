# Databricks notebook source
# MAGIC %md # CCU004_03-D02g-codelist_outcomes
# MAGIC
# MAGIC **Project** CCU004_03
# MAGIC
# MAGIC **Description** This notebook creates the outcomes codelist needed for CCU004_03.
# MAGIC
# MAGIC **Author(s)** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** 
# MAGIC
# MAGIC **Data output**
# MAGIC - **`ccu004_03_out_codelist_outcomes`** : outcomes codelist

# COMMAND ----------

# MAGIC %md # 0. Setup

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

# MAGIC %md ## 2.1 ICD-10

# COMMAND ----------

# MAGIC %md
# MAGIC **Protocol - Fatal Codelist**
# MAGIC
# MAGIC | Fatal cardiovascular disease | ICD10 | 
# MAGIC | ----------- | ----------- | 
# MAGIC | Hypertensive disease | I10-16 | 
# MAGIC | Ischaemic heart disease | I20-25 |
# MAGIC | Arrhythmias, heart failure | I46-52 |
# MAGIC | Cerebrovascular disease | I60-69 |
# MAGIC | Atherosclerosis/AAA | I70-73 |
# MAGIC | Sudden death and death within 24h of symptom onset | R96.0-96.1 |
# MAGIC
# MAGIC
# MAGIC | Excluded from fatal cardiovascular disease endpoint: | ICD10 | 
# MAGIC | ----------- | ----------- | 
# MAGIC | Myocarditis, unspecified | I51.4 | 
# MAGIC | Subarachnoid hemorrhage | I60 | 
# MAGIC | Subdural hemorrhage  | I62 |
# MAGIC | Cerebral aneurysm  | I67.1 |
# MAGIC | Cerebral arteritis  | I68.2 |
# MAGIC | Moyamoya  | I67.5 |

# COMMAND ----------

# ICD 10 codes
path_ref_icd10 = 'dss_corporate.icd10_group_chapter_v01'
icd10 = spark.table(path_ref_icd10)
display(icd10.filter(f.col("CODE").startswith("I")))

# COMMAND ----------

# Fatal cardiovascular disease (ICD10) codes from protocol
codelist_fatal = spark.createDataFrame(
  [
    #hypertensive disease
    ('hypertensive_disease','ICD10',  'I10',   'Essential (primary) hypertension','',''),
    ('hypertensive_disease','ICD10',  'I11',   'Hypertensive heart disease','',''),
    ('hypertensive_disease','ICD10',  'I110',   'Hypertensive heart disease with (congestive) heart failure','',''),
    ('hypertensive_disease','ICD10',  'I119',   'Hypertensive heart disease without (congestive) heart failure','',''),
    ('hypertensive_disease','ICD10',  'I12',    'Hypertensive renal disease','',''),
    ('hypertensive_disease','ICD10',  'I120',   'Hypertensive renal disease with renal failure','',''), #ccu004-03
    ('hypertensive_disease','ICD10',  'I129',   'Hypertensive renal disease without renal failure','',''),
    ('hypertensive_disease','ICD10',  'I13',   'Hypertensive heart and renal disease','',''),
    ('hypertensive_disease','ICD10',  'I130',   'Hypertensive heart and renal disease with (congestive) heart failure','',''),
    ('hypertensive_disease','ICD10',  'I131',   'Hypertensive heart and renal disease with renal failure','',''),
    ('hypertensive_disease','ICD10',  'I132',   'Hypertensive heart and renal disease with both (congestive) heart failure and renal failure','',''),
    ('hypertensive_disease','ICD10',  'I139',   'Hypertensive heart and renal disease, unspecified','',''),
    ('hypertensive_disease','ICD10',  'I15',    'Secondary hypertension','',''),
    ('hypertensive_disease','ICD10',  'I150',   'Renovascular hypertension','',''),  #ccu004-03
    ('hypertensive_disease','ICD10',  'I151',   'Hypertension secondary to other renal disorders','',''),
    ('hypertensive_disease','ICD10',  'I152',   'Hypertension secondary to endocrine disorders','',''),
    ('hypertensive_disease','ICD10',  'I158',   'Other secondary hypertension','',''),
    ('hypertensive_disease','ICD10',  'I159',   'Secondary hypertension, unspecified','',''),
  
    # ischaemic heart disease
    ('ischaemic_heart_disease','ICD10',  'I20',   'Angina pectoris','',''),
    ('ischaemic_heart_disease','ICD10',  'I200',   'Unstable angina','',''),
    ('ischaemic_heart_disease','ICD10',  'I201',   'Angina pectoris with documented spasm','',''),
    ('ischaemic_heart_disease','ICD10',  'I208',   'Other forms of angina pectoris','',''),
    ('ischaemic_heart_disease','ICD10',  'I209',   'Angina pectoris, unspecified','',''),
    ('ischaemic_heart_disease','ICD10',  'I21',    'Acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I210',   'Acute transmural myocardial infarction of anterior wall','',''),
    ('ischaemic_heart_disease','ICD10',  'I211',   'Acute transmural myocardial infarction of inferior wall','',''),
    ('ischaemic_heart_disease','ICD10',  'I212',   'Acute transmural myocardial infarction of other sites','',''),
    ('ischaemic_heart_disease','ICD10',  'I213',   'Acute transmural myocardial infarction of unspecified sites','',''),
    ('ischaemic_heart_disease','ICD10',  'I214',   'Acute subendocardial myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I219',   'Acute myocardial infarction, unspecified','',''),
    ('ischaemic_heart_disease','ICD10',  'I22',    'Subsequent myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I220',   'Subsequent myocardial infarction of anterior wall','',''),
    ('ischaemic_heart_disease','ICD10',  'I221',   'Subsequent myocardial infarction of inferior wall','',''),
    ('ischaemic_heart_disease','ICD10',  'I228',   'Subsequent myocardial infarction of other sites','',''),
    ('ischaemic_heart_disease','ICD10',  'I229',   'Subsequent myocardial infarction of unspecified site','',''),
    ('ischaemic_heart_disease','ICD10',  'I23',    'Certain current complications following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I230',   'Haemopericardium as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I231',   'Atrial septal defect as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I232',   'Ventricular septal defect as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I233',   'Rupture of cardiac wall without haemopericardium as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I234',   'Rupture of chordae tendineae as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I235',   'Rupture of papillary muscle as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I236',   'Thrombosis of atrium, auricular appendage, and ventricle as current complications following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I238',   'Other current complications following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I24',   'Other acute ischaemic heart diseases','',''),
    ('ischaemic_heart_disease','ICD10',  'I240',   'Coronary thrombosis not resulting in myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I241',   'Dressler syndrome','',''),
    ('ischaemic_heart_disease','ICD10',  'I248',   'Other forms of acute ischaemic heart disease','',''),
    ('ischaemic_heart_disease','ICD10',  'I249',   'Acute ischaemic heart disease, unspecified','',''),
    #('ischaemic_heart_disease','ICD10',  'I25',   'Chronic ischaemic heart disease, so described','',''), #ccu004-03
    ('ischaemic_heart_disease','ICD10',  'I250',   'Atherosclerotic cardiovascular disease, so described','',''),
    ('ischaemic_heart_disease','ICD10',  'I251',   'Atherosclerotic heart disease','',''),
   #('ischaemic_heart_disease','ICD10',  'I252',   'Old myocardial infarction','',''), #ccu004-03
    ('ischaemic_heart_disease','ICD10',  'I253',   'Aneurysm of heart','',''),
    ('ischaemic_heart_disease','ICD10',  'I254',   'Coronary artery aneurysm and dissection','',''),
    ('ischaemic_heart_disease','ICD10',  'I255',   'Ischaemic cardiomyopathy','',''),
    ('ischaemic_heart_disease','ICD10',  'I256',   'Silent myocardial ischaemia','',''),
    ('ischaemic_heart_disease','ICD10',  'I258',   'Other forms of chronic ischaemic heart disease','',''),
    ('ischaemic_heart_disease','ICD10',  'I259',   'Chronic ischaemic heart disease, unspecified','',''),
    
    # Arrythmias, heart failure
    ('arrythmias_heart_failure','ICD10',  'I46',   'Cardiac arrest','',''),
    ('arrythmias_heart_failure','ICD10',  'I460',   'Cardiac arrest with successful resuscitation','',''),
    ('arrythmias_heart_failure','ICD10',  'I461',   'Sudden cardiac death, so described','',''),
    ('arrythmias_heart_failure','ICD10',  'I469',   'Cardiac arrest, unspecified','',''), #ccu004-03
    ('arrythmias_heart_failure','ICD10',  'I47',    'Paroxysmal tachycardia','',''),
    ('arrythmias_heart_failure','ICD10',  'I470',   'Re-entry ventricular arrhythmia','',''),
    ('arrythmias_heart_failure','ICD10',  'I471',   'Supraventricular tachycardia','',''),
    ('arrythmias_heart_failure','ICD10',  'I472',   'Ventricular tachycardia','',''),
    ('arrythmias_heart_failure','ICD10',  'I479',   'Paroxysmal tachycardia, unspecified','',''),
    ('arrythmias_heart_failure','ICD10',  'I48',   'Atrial fibrillation and flutter','',''),
    ('arrythmias_heart_failure','ICD10',  'I480',   'Paroxysmal atrial fibrillation','',''),   
    ('arrythmias_heart_failure','ICD10',  'I481',   'Persistent atrial fibrillation','',''),
    ('arrythmias_heart_failure','ICD10',  'I482',   'Chronic atrial fibrillation','',''),
    ('arrythmias_heart_failure','ICD10',  'I483',   'Typical atrial flutter','',''),
    ('arrythmias_heart_failure','ICD10',  'I484',   'Atypical atrial flutter','',''),
    ('arrythmias_heart_failure','ICD10',  'I489',   'Atrial fibrillation and atrial flutter, unspecified','',''),
    ('arrythmias_heart_failure','ICD10',  'I49',   'Other cardiac arrhythmias','',''),
    ('arrythmias_heart_failure','ICD10',  'I490',   'Venticular fibrillation and flutter','',''), #ccu004-03
    ('arrythmias_heart_failure','ICD10',  'I491',   'Atrial premature depolarization','',''),
    ('arrythmias_heart_failure','ICD10',  'I492',   'Junctional premature depolarization','',''),
    ('arrythmias_heart_failure','ICD10',  'I493',   'Ventricular premature depolarization','',''), #ccu004-03
    ('arrythmias_heart_failure','ICD10',  'I494',   'Other and unspecified premature depolarization','',''),
    ('arrythmias_heart_failure','ICD10',  'I495',   'Sick sinus syndrome','',''),  
    ('arrythmias_heart_failure','ICD10',  'I498',   'Other specified cardiac arrhythmias','',''),
    ('arrythmias_heart_failure','ICD10',  'I499',   'Cardiac arrhythmia, unspecified','',''),
    ('arrythmias_heart_failure','ICD10',  'I50',    'Heart failure','',''),
    ('arrythmias_heart_failure','ICD10',  'I500',   'Congestive heart failure','',''),
    ('arrythmias_heart_failure','ICD10',  'I501',   'Left ventricular failure','',''),
    ('arrythmias_heart_failure','ICD10',  'I509',   'Heart failure, unspecified','',''),
   #('arrythmias_heart_failure','ICD10',   'I51',    'Complications and ill-defined descriptions of heart disease','',''),
    ('arrythmias_heart_failure','ICD10',  'I510',   'Cardiac septal defect, acquired','',''),
    ('arrythmias_heart_failure','ICD10',  'I511',   'Rupture of chordae tendineae, not elsewhere classified','',''),
    ('arrythmias_heart_failure','ICD10',  'I512',   'Rupture of papillary muscle, not elsewhere classified','',''),
    ('arrythmias_heart_failure','ICD10',  'I513',   'Intracardiac thrombosis, not elsewhere classified','',''),
  # ('arrythmias_heart_failure','ICD10',  'I514',   'Myocarditis, unspecified','',''),
    ('arrythmias_heart_failure','ICD10',  'I515',   'Myocardial degeneration','',''),   
    ('arrythmias_heart_failure','ICD10',  'I516',   'Cardiovascular disease, unspecified','',''),
    ('arrythmias_heart_failure','ICD10',  'I517',   'Cardiomegaly','',''),
    ('arrythmias_heart_failure','ICD10',  'I518',   'Other ill-defined heart diseases','',''),
    ('arrythmias_heart_failure','ICD10',  'I519',   'Heart disease, unspecified','',''),
    ('arrythmias_heart_failure','ICD10',  'I52',    'Other heart disorders in diseases classified elsewhere','',''),
    ('arrythmias_heart_failure','ICD10',  'I520',   'Other heart disorders in bacterial diseases classified elsewhere','',''),
    ('arrythmias_heart_failure','ICD10',  'I521',   'Other heart disorders in other infectious and parasitic diseases classified elsewhere','',''),
    ('arrythmias_heart_failure','ICD10',  'I528',   'Other heart disorders in other diseases classified elsewhere','',''),

    # Cerebrovascular disease - same as non-fatal stroke?
    
#     ('cerebrovascular_disease','ICD10',  'I60',   'Subarachnoid haemorrhage','',''),
#     ('cerebrovascular_disease','ICD10',  'I600',  'Subarachnoid haemorrhage from carotid siphon and bifurcation','',''),
#     ('cerebrovascular_disease','ICD10',  'I601',  'Subarachnoid haemorrhage from middle cerebral artery','',''),
#     ('cerebrovascular_disease','ICD10',  'I602',  'Subarachnoid haemorrhage from anterior communicating artery','',''),
#     ('cerebrovascular_disease','ICD10',  'I603',  'Subarachnoid haemorrhage from posterior communicating artery','',''),
#     ('cerebrovascular_disease','ICD10',  'I604',  'Subarachnoid haemorrhage from basilar artery','',''),
#     ('cerebrovascular_diseasee','ICD10',  'I605',  'Subarachnoid haemorrhage from vertebral artery','',''),
#     ('cerebrovascular_disease','ICD10',  'I606',  'Subarachnoid haemorrhage from other intracranial arteries','',''),
#     ('cerebrovascular_disease','ICD10',  'I607',  'Subarachnoid haemorrhage from intracranial artery, unspecified','',''),
#     ('cerebrovascular_disease','ICD10',  'I608',  'Other subarachnoid haemorrhage','',''),
#     ('cerebrovascular_disease','ICD10',  'I609',  'Subarachnoid haemorrhage, unspecified','',''),
    ('cerebrovascular_disease','ICD10',  'I61',   'Intracerebral haemorrhage','',''),
    ('cerebrovascular_disease','ICD10',  'I610',  'Intracerebral haemorrhage in hemisphere, subcortical','',''),
    ('cerebrovascular_disease','ICD10',  'I611',  'Intracerebral haemorrhage in hemisphere, cortical','',''),
    ('cerebrovascular_disease','ICD10',  'I612',  'Intracerebral haemorrhage in hemisphere, unspecified','',''),
    ('cerebrovascular_disease','ICD10',  'I613',  'Intracerebral haemorrhage in brain stem','',''),
    ('cerebrovascular_disease','ICD10',  'I614',  'Intracerebral haemorrhage in cerebellum','',''),
    ('cerebrovascular_disease','ICD10',  'I615',  'Intracerebral haemorrhage, intraventricular','',''),
    ('cerebrovascular_disease','ICD10',  'I616',  'Intracerebral haemorrhage, multiple localized','',''),
    ('cerebrovascular_disease','ICD10',  'I618',  'Other intracerebral haemorrhage','',''),
    ('cerebrovascular_disease','ICD10',  'I619',  'Intracerebral haemorrhage, unspecified','',''),
#     ('cerebrovascular_disease','ICD10',  'I62',   'Other nontraumatic intracranial haemorrhage','',''),
#     ('cerebrovascular_disease','ICD10',  'I620',  'Subdural haemorrhage (acute)(nontraumatic)','',''),
#     ('cerebrovascular_disease','ICD10',  'I621',  'Nontraumatic extradural haemorrhage','',''),
#     ('cerebrovascular_disease','ICD10',  'I629',  'Intracranial haemorrhage (nontraumatic), unspecified','',''),
    ('cerebrovascular_disease','ICD10',  'I63',   'Cerebral infarction','',''),
    ('cerebrovascular_disease','ICD10',  'I630',  'Cerebral infarction due to thrombosis of precerebral arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I631',  'Cerebral infarction due to embolism of precerebral arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I632',  'Cerebral infarction due to unspecified occlusion or stenosis of precerebral arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I633',  'Cerebral infarction due to thrombosis of cerebral arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I634',  'Cerebral infarction due to embolism of cerebral arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I635',  'Cerebral infarction due to unspecified occlusion or stenosis of cerebral arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I636',  'Cerebral infarction due to cerebral venous thrombosis, nonpyogenic','',''),
    ('cerebrovascular_disease','ICD10',  'I638',  'Other cerebral infarction','',''),
    ('cerebrovascular_disease','ICD10',  'I639',  'Cerebral infarction, unspecified','',''),
    ('cerebrovascular_disease','ICD10',  'I64',   'Stroke, not specified as haemorrhage or infarction','',''), #ccu004-03
    ('cerebrovascular_disease','ICD10',  'I65',   'Occlusion and stenosis of precerebral arteries, not resulting in cerebral infarction','',''),
    ('cerebrovascular_disease','ICD10',  'I650',  'Occlusion and stenosis of vertebral artery','',''),
    ('cerebrovascular_disease','ICD10',  'I651',  'Occlusion and stenosis of basilar artery','',''),
    ('cerebrovascular_disease','ICD10',  'I652',  'Occlusion and stenosis of carotid artery','',''),
    ('cerebrovascular_disease','ICD10',  'I653',  'Occlusion and stenosis of multiple and bilateral precerebral arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I658',  'Occlusion and stenosis of other precerebral artery','',''),
    ('cerebrovascular_disease','ICD10',  'I659',  'Occlusion and stenosis of unspecified precerebral artery','',''),
    ('cerebrovascular_disease','ICD10',  'I66',   'Occlusion and stenosis of cerebral arteries, not resulting in cerebral infarction','',''),
    ('cerebrovascular_disease','ICD10',  'I660',  'Occlusion and stenosis of middle cerebral artery','',''),
    ('cerebrovascular_disease','ICD10',  'I661',  'Occlusion and stenosis of anterior cerebral artery','',''),
    ('cerebrovascular_disease','ICD10',  'I662',  'Occlusion and stenosis of posterior cerebral artery','',''),
    ('cerebrovascular_disease','ICD10',  'I663',  'Occlusion and stenosis of cerebellar arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I664',  'Occlusion and stenosis of multiple and bilateral cerebral arteries','',''),
    ('cerebrovascular_disease','ICD10',  'I668',  'Occlusion and stenosis of other cerebral artery','',''),
    ('cerebrovascular_disease','ICD10',  'I669',  'Occlusion and stenosis of unspecified cerebral artery','',''),
    #('cerebrovascular_disease','ICD10',  'I67',   'Other cerebrovascular diseases','',''), #ccu004-03
    ('cerebrovascular_disease','ICD10',  'I670',  'Dissection of cerebral arteries, nonruptured','',''),
   #('cerebrovascular_disease','ICD10',  'I671',  'Cerebral aneurysm, nonruptured','',''),
    ('cerebrovascular_disease','ICD10',  'I672',  'Cerebral atherosclerosis','',''),
    ('cerebrovascular_disease','ICD10',  'I673',  'Progressive vascular leukoencephalopathy','',''),
    ('cerebrovascular_disease','ICD10',  'I674',  'Hypertensive encephalopathy','',''),
   #('cerebrovascular_disease','ICD10',  'I675',  'Moyamoya disease','',''),
    ('cerebrovascular_disease','ICD10',  'I676',  'Nonpyogenic thrombosis of intracranial venous system','',''),
    ('cerebrovascular_disease','ICD10',  'I677',  'Cerebral arteritis, not elsewhere classified','',''),
    ('cerebrovascular_disease','ICD10',  'I678',  'Other specified cerebrovascular diseases','',''),
    ('cerebrovascular_disease','ICD10',  'I679',  'Cerebrovascular disease, unspecified','',''),
    #('cerebrovascular_disease','ICD10',  'I68',   'Cerebrovascular disorders in diseases classified elsewhere','',''), #ccu004-03
    ('cerebrovascular_disease','ICD10',  'I680',  'Cerebral amyloid angiopathy','',''),
    ('cerebrovascular_disease','ICD10',  'I681',  'Cerebral arteritis in infectious and parasitic diseases classified elsewhere','',''),
    #('cerebrovascular_disease','ICD10',  'I682',  'Cerebral arteritis in other diseases classified elsewhere','',''),
    ('cerebrovascular_disease','ICD10',  'I688',  'Other cerebrovascular disorders in diseases classified elsewhere','',''),
    ('cerebrovascular_disease','ICD10',  'I69',   'Sequelae of cerebrovascular disease','',''),
    ('cerebrovascular_disease','ICD10',  'I690',  'Sequelae of subarachnoid haemorrhage','',''),
    ('cerebrovascular_disease','ICD10',  'I691',  'Sequelae of intracerebral haemorrhage','',''),
    ('cerebrovascular_disease','ICD10',  'I692',  'Sequelae of other nontraumatic intracranial haemorrhage','',''),
    ('cerebrovascular_disease','ICD10',  'I693',  'Sequelae of cerebral infarction','',''),
    ('cerebrovascular_disease','ICD10',  'I694',  'Sequelae of stroke, not specified as haemorrhage or infarction','',''),
    ('cerebrovascular_disease','ICD10',  'I698',  'Sequelae of other and unspecified cerebrovascular diseases','',''),

   # Athelerosclerosis/AAA
    ('atherosclerosis','ICD10',  'I70',   'Atherosclerosis','',''),
    ('atherosclerosis','ICD10',  'I700',   'Atherosclerosis of aorta','',''),   
    ('atherosclerosis','ICD10',  'I7000',   'Atherosclerosis of aorta','',''),
    ('atherosclerosis','ICD10',  'I7001',   'Atherosclerosis of aorta','',''),   
    ('atherosclerosis','ICD10',  'I701',   'Atherosclerosis of renal artery','',''),
    ('atherosclerosis','ICD10',  'I7010',   'Atherosclerosis of renal artery','',''),   
    ('atherosclerosis','ICD10',  'I7011',   'Atherosclerosis of renal artery','',''),
    ('atherosclerosis','ICD10',  'I702',   'Atherosclerosis of arteries of extremities','',''),   
    ('atherosclerosis','ICD10',  'I7020',   'Atherosclerosis of arteries of extremities','',''),
    ('atherosclerosis','ICD10',  'I7021',   'Atherosclerosis of arteries of extremities','',''),   
    ('atherosclerosis','ICD10',  'I708',   'Atherosclerosis of other arteries','',''),
    ('atherosclerosis','ICD10',  'I7080',   'Atherosclerosis of other arteries','',''),
    ('atherosclerosis','ICD10',  'I7081',   'Atherosclerosis of other arteries','',''),   
    ('atherosclerosis','ICD10',  'I709',   'Generalized and unspecified atherosclerosis','',''),
    ('atherosclerosis','ICD10',  'I7090',   'Generalized and unspecified atherosclerosis','',''),   
    ('atherosclerosis','ICD10',  'I7091',   'Generalized and unspecified atherosclerosis','',''),
    ('atherosclerosis','ICD10',  'I71',   'Aortic aneurysm and dissection','',''),
    ('atherosclerosis','ICD10',  'I710',   'Dissection of aorta [any part]','',''),
    ('atherosclerosis','ICD10',  'I711',   'Thoracic aortic aneurysm, ruptured','',''),   
    ('atherosclerosis','ICD10',  'I712',   'Thoracic aortic aneurysm, without mention of rupture','',''),
    ('atherosclerosis','ICD10',  'I713',   'Abdominal aortic aneurysm, ruptured','',''),   
    ('atherosclerosis','ICD10',  'I714',   'Abdominal aortic aneurysm, without mention of rupture','',''),
    ('atherosclerosis','ICD10',  'I715',   'Thoracoabdominal aortic aneurysm, ruptured','',''),
    ('atherosclerosis','ICD10',  'I716',   'Thoracoabdominal aortic aneurysm, without mention of rupture','',''),   
    ('atherosclerosis','ICD10',  'I718',   'Aortic aneurysm of unspecified site, ruptured','',''),
    ('atherosclerosis','ICD10',  'I719',   'Aortic aneurysm of unspecified site, without mention of rupture','',''),  
    ('atherosclerosis','ICD10',  'I72',   'Other aneurysm and dissection','',''),   
    ('atherosclerosis','ICD10',  'I720',   'Aneurysm and dissection of carotid artery','',''),
    ('atherosclerosis','ICD10',  'I721',   'Aneurysm and dissection of artery of upper extremity','',''),  
    ('atherosclerosis','ICD10',  'I722',   'Aneurysm and dissection of renal artery','',''), 
    ('atherosclerosis','ICD10',  'I723',   'Aneurysm and dissection of artery of iliac artery','',''), #ccu004-03
    ('atherosclerosis','ICD10',  'I724',   'Aneurysm and dissection of artery of lower extremity','',''),
    ('atherosclerosis','ICD10',  'I725',   'Aneurysm and dissection of other precerebral arteries','',''),  
    ('atherosclerosis','ICD10',  'I726',   'Aneurysm and dissection of vertebral artery','',''),   
    ('atherosclerosis','ICD10',  'I728',   'Aneurysm and dissection of other specified arteries','',''),
    ('atherosclerosis','ICD10',  'I729',   'Aneurysm and dissection of unspecified site','',''),  
    ('atherosclerosis','ICD10',  'I73',   'Other peripheral vascular diseases','',''),   
    ('atherosclerosis','ICD10',  'I730',   'Raynaud syndrome','',''),
    ('atherosclerosis','ICD10',  'I731',   'Thromboangiitis obliterans [Buerger]','',''),  
    ('atherosclerosis','ICD10',  'I738',   'Other specified peripheral vascular diseases','',''),
    ('atherosclerosis','ICD10',  'I739',   'Peripheral vascular disease, unspecified','',''), 
        
    # Sudden death and death within 24h of symptom onset
    ('sudden_death','ICD10',  'R96',   'Other sudden death, cause unknown','',''),   
    ('sudden_death','ICD10',  'R960',   'Instantaneous death','',''),
    ('sudden_death','ICD10',  'R961',   'Death occurring less than 24 hours from onset of symptoms, not otherwise explained','','') 
    
  ],
  
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

display(codelist_fatal)

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# rename
codelist_fatal = codelist_fatal.withColumn('name', f.concat(f.lit("fatal_"), f.col('name')))

# check
tmpt = tab(codelist_fatal, 'name'); print()

# COMMAND ----------

# MAGIC %md # 4. Save

# COMMAND ----------

save_table(df=codelist_fatal, out_name=f'{proj}_out_codelist_outcomes', save_previous=True)