# Databricks notebook source
# MAGIC %md # CCU004_03-D02c-codelist_CVD
# MAGIC
# MAGIC **Project** CCU004_03
# MAGIC
# MAGIC **Description** This notebook creates the codelist for the exposures and outcomes: non-fatal MI and stroke ICD10 and SNOMED codes.
# MAGIC
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D02b-codelist_exposures_and_outcomes
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu004_03_out_codelist_cvd`** : Codes for all CVD exposures and outcomes

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
# MAGIC

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

# MAGIC %md ## 2.1 SNOMED

# COMMAND ----------

# BHF DSC Phenotype Lookup
bhf_phenotypes = spark.table(path_ref_bhf_phenotypes)
display(bhf_phenotypes)

# COMMAND ----------

# Note code_type = 0 indicates that this is covaraite only and not a code that you would apply to an incident event. Thus covarite only codes should be filtered out for outcomes.
display(bhf_phenotypes.where(f.col('code_type') == 0))

# COMMAND ----------

display(bhf_phenotypes.where(f.col('name') == 'stroke_TIA').where(f.col('terminology') == 'SNOMED'))

# COMMAND ----------

# check
tmpt = tab(bhf_phenotypes, 'name', 'terminology')

# COMMAND ----------

codelist_bhf = (bhf_phenotypes
                .where(f.col('name').isin(
                  'AMI',
                  'stroke_HS',
                  'stroke_IS',
                  'stroke_NOS',
                  'stroke_TIA'
                ))
               .where(f.col("terminology").isin("SNOMED"))
                      )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 ICD 10

# COMMAND ----------

# MAGIC %md
# MAGIC **Protocol Non-fatal Codelist**
# MAGIC
# MAGIC | Non-fatal cardiovascular disease | ICD10 | 
# MAGIC | ----------- | ----------- | 
# MAGIC | Non-fatal myocardial infarction | I21-I23 | 
# MAGIC | Non-fatal stroke | I60-69 |
# MAGIC
# MAGIC | Excluded from the non-fatal stroke endpoint: | ICD10 | 
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

#icd10.filter(f.col("CODE").startswith("I6")).select(f.col("ALT_CODE"),f.col("ICD10_DESCRIPTION")).collect()

# COMMAND ----------

# Non-fatal cardiovascular disease (ICD10) codes from protocol
codelist_nonfatal = spark.createDataFrame(
  [
    ('nonfatal_myocardial_infarction','ICD10',  'I21',   'Acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I210',  'Acute transmural myocardial infarction of anterior wall','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I211',  'Acute transmural myocardial infarction of inferior wall','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I212',  'Acute transmural myocardial infarction of other sites','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I213',  'Acute transmural myocardial infarction of unspecified site','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I214',  'Acute subendocardial myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I219',  'Acute myocardial infarction, unspecified','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I22',   'Subsequent myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I220',  'Subsequent myocardial infarction of anterior wall','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I221',  'Subsequent myocardial infarction of inferior wall','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I228',  'Subsequent myocardial infarction of other sites','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I229',  'Subsequent myocardial infarction of unspecified site','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I23',   'Certain current complications following acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I230',  'Haemopericardium as current complication following acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I231',  'Atrial septal defect as current complication following acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I232',  'Ventricular septal defect as current complication following acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I233',  'Rupture of cardiac wall without haemopericardium as current complication following acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I234',  'Rupture of chordae tendineae as current complication following acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I235',  'Rupture of papillary muscle as current complication following acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I236',  'Thrombosis of atrium, auricular appendage, and ventricle as current complications following acute myocardial infarction','',''),
    ('nonfatal_myocardial_infarction','ICD10',  'I238',  'Other current complications following acute myocardial infarction','',''),

#     ('nonfatal_stroke','ICD10',  'I60',   'Subarachnoid haemorrhage','',''),
#     ('nonfatal_stroke','ICD10',  'I600',  'Subarachnoid haemorrhage from carotid siphon and bifurcation','',''),
#     ('nonfatal_stroke','ICD10',  'I601',  'Subarachnoid haemorrhage from middle cerebral artery','',''),
#     ('nonfatal_stroke','ICD10',  'I602',  'Subarachnoid haemorrhage from anterior communicating artery','',''),
#     ('nonfatal_stroke','ICD10',  'I603',  'Subarachnoid haemorrhage from posterior communicating artery','',''),
#     ('nonfatal_stroke','ICD10',  'I604',  'Subarachnoid haemorrhage from basilar artery','',''),
#     ('nonfatal_stroke','ICD10',  'I605',  'Subarachnoid haemorrhage from vertebral artery','',''),
#     ('nonfatal_stroke','ICD10',  'I606',  'Subarachnoid haemorrhage from other intracranial arteries','',''),
#     ('nonfatal_stroke','ICD10',  'I607',  'Subarachnoid haemorrhage from intracranial artery, unspecified','',''),
#     ('nonfatal_stroke','ICD10',  'I608',  'Other subarachnoid haemorrhage','',''),
#     ('nonfatal_stroke','ICD10',  'I609',  'Subarachnoid haemorrhage, unspecified','',''),
    ('nonfatal_stroke','ICD10',  'I61',   'Intracerebral haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I610',  'Intracerebral haemorrhage in hemisphere, subcortical','',''),
    ('nonfatal_stroke','ICD10',  'I611',  'Intracerebral haemorrhage in hemisphere, cortical','',''),
    ('nonfatal_stroke','ICD10',  'I612',  'Intracerebral haemorrhage in hemisphere, unspecified','',''),
    ('nonfatal_stroke','ICD10',  'I613',  'Intracerebral haemorrhage in brain stem','',''),
    ('nonfatal_stroke','ICD10',  'I614',  'Intracerebral haemorrhage in cerebellum','',''),
    ('nonfatal_stroke','ICD10',  'I615',  'Intracerebral haemorrhage, intraventricular','',''),
    ('nonfatal_stroke','ICD10',  'I616',  'Intracerebral haemorrhage, multiple localized','',''),
    ('nonfatal_stroke','ICD10',  'I618',  'Other intracerebral haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I619',  'Intracerebral haemorrhage, unspecified','',''),
#     ('nonfatal_stroke','ICD10',  'I62',   'Other nontraumatic intracranial haemorrhage','',''),
#     ('nonfatal_stroke','ICD10',  'I620',  'Subdural haemorrhage (acute)(nontraumatic)','',''),
#     ('nonfatal_stroke','ICD10',  'I621',  'Nontraumatic extradural haemorrhage','',''),
#     ('nonfatal_stroke','ICD10',  'I629',  'Intracranial haemorrhage (nontraumatic), unspecified','',''),
    ('nonfatal_stroke','ICD10',  'I63',   'Cerebral infarction','',''),
    ('nonfatal_stroke','ICD10',  'I630',  'Cerebral infarction due to thrombosis of precerebral arteries','',''),
    ('nonfatal_stroke','ICD10',  'I631',  'Cerebral infarction due to embolism of precerebral arteries','',''),
    ('nonfatal_stroke','ICD10',  'I632',  'Cerebral infarction due to unspecified occlusion or stenosis of precerebral arteries','',''),
    ('nonfatal_stroke','ICD10',  'I633',  'Cerebral infarction due to thrombosis of cerebral arteries','',''),
    ('nonfatal_stroke','ICD10',  'I634',  'Cerebral infarction due to embolism of cerebral arteries','',''),
    ('nonfatal_stroke','ICD10',  'I635',  'Cerebral infarction due to unspecified occlusion or stenosis of cerebral arteries','',''),
    ('nonfatal_stroke','ICD10',  'I636',  'Cerebral infarction due to cerebral venous thrombosis, nonpyogenic','',''),
    ('nonfatal_stroke','ICD10',  'I638',  'Other cerebral infarction','',''),
    ('nonfatal_stroke','ICD10',  'I639',  'Cerebral infarction, unspecified','',''),
    ('nonfatal_stroke','ICD10',  'I64',   'Stroke, not specified as haemorrhage or infarction','',''), #ccu004-03
    ('nonfatal_stroke','ICD10',  'I65',   'Occlusion and stenosis of precerebral arteries, not resulting in cerebral infarction','',''),
    ('nonfatal_stroke','ICD10',  'I650',  'Occlusion and stenosis of vertebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I651',  'Occlusion and stenosis of basilar artery','',''),
    ('nonfatal_stroke','ICD10',  'I652',  'Occlusion and stenosis of carotid artery','',''),
    ('nonfatal_stroke','ICD10',  'I653',  'Occlusion and stenosis of multiple and bilateral precerebral arteries','',''),
    ('nonfatal_stroke','ICD10',  'I658',  'Occlusion and stenosis of other precerebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I659',  'Occlusion and stenosis of unspecified precerebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I66',   'Occlusion and stenosis of cerebral arteries, not resulting in cerebral infarction','',''),
    ('nonfatal_stroke','ICD10',  'I660',  'Occlusion and stenosis of middle cerebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I661',  'Occlusion and stenosis of anterior cerebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I662',  'Occlusion and stenosis of posterior cerebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I663',  'Occlusion and stenosis of cerebellar arteries','',''),
    ('nonfatal_stroke','ICD10',  'I664',  'Occlusion and stenosis of multiple and bilateral cerebral arteries','',''),
    ('nonfatal_stroke','ICD10',  'I668',  'Occlusion and stenosis of other cerebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I669',  'Occlusion and stenosis of unspecified cerebral artery','',''),
    #('nonfatal_stroke','ICD10',  'I67',   'Other cerebrovascular diseases','',''), #ccu004-03
    ('nonfatal_stroke','ICD10',  'I670',  'Dissection of cerebral arteries, nonruptured','',''),
#     ('nonfatal_stroke','ICD10',  'I671',  'Cerebral aneurysm, nonruptured','',''),
    ('nonfatal_stroke','ICD10',  'I672',  'Cerebral atherosclerosis','',''),
    ('nonfatal_stroke','ICD10',  'I673',  'Progressive vascular leukoencephalopathy','',''),
    ('nonfatal_stroke','ICD10',  'I674',  'Hypertensive encephalopathy','',''),
#     ('nonfatal_stroke','ICD10',  'I675',  'Moyamoya disease','',''),
    ('nonfatal_stroke','ICD10',  'I676',  'Nonpyogenic thrombosis of intracranial venous system','',''),
    ('nonfatal_stroke','ICD10',  'I677',  'Cerebral arteritis, not elsewhere classified','',''),
    ('nonfatal_stroke','ICD10',  'I678',  'Other specified cerebrovascular diseases','',''),
    ('nonfatal_stroke','ICD10',  'I679',  'Cerebrovascular disease, unspecified','',''),
    #('nonfatal_stroke','ICD10',  'I68',   'Cerebrovascular disorders in diseases classified elsewhere','',''), #ccu004-03
    ('nonfatal_stroke','ICD10',  'I680',  'Cerebral amyloid angiopathy','',''),
    ('nonfatal_stroke','ICD10',  'I681',  'Cerebral arteritis in infectious and parasitic diseases classified elsewhere','',''), 
#     ('nonfatal_stroke','ICD10',  'I682',  'Cerebral arteritis in other diseases classified elsewhere','',''), 
    ('nonfatal_stroke','ICD10',  'I688',  'Other cerebrovascular disorders in diseases classified elsewhere','',''),
    ('nonfatal_stroke','ICD10',  'I69',   'Sequelae of cerebrovascular disease','',''),
    ('nonfatal_stroke','ICD10',  'I690',  'Sequelae of subarachnoid haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I691',  'Sequelae of intracerebral haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I692',  'Sequelae of other nontraumatic intracranial haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I693',  'Sequelae of cerebral infarction','',''),
    ('nonfatal_stroke','ICD10',  'I694',  'Sequelae of stroke, not specified as haemorrhage or infarction','',''),
    ('nonfatal_stroke','ICD10',  'I698',  'Sequelae of other and unspecified cerebrovascular diseases','','')
  
  ],
  
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# Non-fatal cardiovascular disease (ICD10) codes from protocol and corresponding SNOMED codes - additional codes for exclusion criteria
codelist_hx_nonfatal = spark.createDataFrame(
  [
    ('nonfatal_angina','ICD10',  'I20',  'Angina pectoris','',''),
    ('nonfatal_angina','ICD10',  'I200',  'Unstable angina','',''),
    ('nonfatal_angina','ICD10',  'I201',  'Angina pectoris with documented spasm','',''),
    ('nonfatal_angina','ICD10',  'I208',  'Other forms of angina pectoris','',''),
    ('nonfatal_angina','ICD10',  'I209',  'Angina pectoris, unspecified','',''),
    
    # Non-fatal Angina SNOMED codes (Carmen 25.11.2024)
    ('nonfatal_angina','SNOMED','314116003','Post infarct angina','',''),
    ('nonfatal_angina','SNOMED','4557003','Preinfarction syndrome (disorder)','',''),
    ('nonfatal_angina','SNOMED','19057007','Status anginosus (disorder)','',''),
    ('nonfatal_angina','SNOMED','21470009','Syncope anginosa (disorder)','',''),
    ('nonfatal_angina','SNOMED','25106000','Impending infarction (disorder)','',''),
    ('nonfatal_angina','SNOMED','35928006','Nocturnal angina (disorder)','',''),
    ('nonfatal_angina','SNOMED','46109009','Subendocardial ischemia (disorder)','',''),
    ('nonfatal_angina','SNOMED','59021001','Angina decubitus (disorder)','',''),
    ('nonfatal_angina','SNOMED','87343002','Prinzmetal angina (disorder)','',''),
    ('nonfatal_angina','SNOMED','194828000','Angina (disorder)','',''),
    ('nonfatal_angina','SNOMED','225566008','Ischemic chest pain (finding)','',''),
    ('nonfatal_angina','SNOMED','233819005','Stable angina (disorder)','',''),
    ('nonfatal_angina','SNOMED','233821000','New onset angina (disorder)','',''),
    ('nonfatal_angina','SNOMED','300995000','Exercise-induced angina (disorder)','',''),
    ('nonfatal_angina','SNOMED','315025001','Refractory angina (disorder)','',''),
    ('nonfatal_angina','SNOMED','429559004','Typical angina (disorder)','',''),
    ('nonfatal_angina','SNOMED','15960141000119102','Angina co-occurrent and due to coronary arteriosclerosis','',''),
    ('nonfatal_angina','SNOMED','15960061000119102','Unstable angina co-occurrent and due to coronary arteriosclerosis','',''),
    ('nonfatal_angina','SNOMED','15960461000119105','Unstable angina due to arteriosclerosis of autologous arterial coronary artery bypass graft','',''),
    ('nonfatal_angina','SNOMED','15960541000119107','Unstable angina due to arteriosclerosis of autologous vein coronary artery bypass graft','',''),
    ('nonfatal_angina','SNOMED','15960341000119104','Unstable angina due to arteriosclerosis of coronary artery bypass graft of transplanted heart','',''),
    ('nonfatal_angina','SNOMED','371806006','Progressive angina','',''),
    ('nonfatal_angina','SNOMED','371810009','Recurrent angina status post coronary artery bypass graft','',''),
    ('nonfatal_angina','SNOMED','371809004','Recurrent angina status post coronary stent placement','',''),
    ('nonfatal_angina','SNOMED','371812001','Recurrent angina status post directional coronary atherectomy','',''),
    ('nonfatal_angina','SNOMED','371808007','Recurrent angina status post percutaneous transluminal coronary angioplasty','',''),
    ('nonfatal_angina','SNOMED','371811008','Recurrent angina status post rotational atherectomy','',''),
    ('nonfatal_angina','SNOMED','16754391000119100','Stable angina due to coronary arteriosclerosis','',''),
    ('nonfatal_angina','SNOMED','791000119109','Angina due to type 2 diabetes mellitus','',''),
    ('nonfatal_angina','SNOMED','15960381000119109','Angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorde)','',''),
    ('nonfatal_angina','SNOMED','15960581000119102','Angina co-occurrent and due to arteriosclerosis of autologous vein coronary artery bypass graft','',''),
    ('nonfatal_angina','SNOMED','15960661000119107','Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft','',''),
    ('nonfatal_angina','SNOMED','23687008','Coronary artery spasm','',''),
    ('nonfatal_angina','SNOMED','15960061000119100',' Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)"        ','',''),
    ('nonfatal_angina','SNOMED','15960141000119100',' Angina co-occurrent and due to coronary arteriosclerosis (disorder)"                 ','',''),
    ('nonfatal_angina','SNOMED','15960381000119100',' Angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (..."','',''),
    ('nonfatal_angina','SNOMED','15960581000119100',' Angina co-occurrent and due to arteriosclerosis of autologous vein coronary arter..."','',''),
    ('nonfatal_angina','SNOMED','15960661000119100',' Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypas..."','',''),
    ('nonfatal_angina','SNOMED','41334000','Angina, class II (disorder)','',''),
    ('nonfatal_angina','SNOMED','61490001','Angina, class I (disorder)','',''),
    ('nonfatal_angina','SNOMED','85284003','Angina, class III (disorder)','',''),
    ('nonfatal_angina','SNOMED','89323001','Angina, class IV (disorder)','',''),

    ('TIA','ICD10',  'G45',  'Transient cerebral ischaemic attacks and related syndromes','',''),
    ('TIA','ICD10',  'G450',  'Vertebro-basilar artery syndrome','',''),
    ('TIA','ICD10',  'G451',  'Carotid artery syndrome (hemispheric)','',''),
    ('TIA','ICD10',  'G452',  'Multiple and bilateral precerebral artery syndromes','',''),
    ('TIA','ICD10',  'G453',  'Amaurosis fugax','',''),
    ('TIA','ICD10',  'G454',  'Transient global amnesia','',''),
    ('TIA','ICD10',  'G458',  'Other transient cerebral ischaemic attacks and related syndromes','',''),
    ('TIA','ICD10',  'G459',  'Transient cerebral ischaemic attack, unspecified','',''),

    # TIA SNOMED codes which combine BHF pehnotype codelist and a few additional codes
    ('TIA','SNOMED','88032003','Amaurosis fugax (disorder)','',''),
    ('TIA','SNOMED','230716006','Carotid territory transient ischemic attack (disorder)','',''),
    ('TIA','SNOMED','230717002','Vertebrobasilar territory transient ischemic attack (disorder)','',''),
    ('TIA','SNOMED','266257000','Transient ischemic attack (disorder)','',''),
    ('TIA','SNOMED','426814001','Transient cerebral ischemia due to atrial fibrillation (disorder)','',''),
    ('TIA','SNOMED','444172003','Recurrent transient cerebral ischemic attack (disorder)','',''),
    ('TIA','SNOMED','710575003','Transient ischemic attack due to embolism (disorder)','',''),
    ('TIA','SNOMED','1208871009','Transient ischemic attack co-occurrent with subarachnoid hemorrhage (disorder)','',''),
    ('TIA','SNOMED','12237911000119109','Amaurosis fugax of left eye (disorder)','',''),
    ('TIA','SNOMED','12237951000119105','Amaurosis fugax of right eye (disorder)','',''),
    ('TIA','SNOMED','161511000','History of transient ischemic attack','',''),
    ('TIA','SNOMED','13016361000119101','History of amaurosis fugax','',''),
    ('TIA','SNOMED','140221000119109','History of transient ischemic attack due to embolism','',''),
    ('TIA','SNOMED','195206000','Intermittent cerebral ischaemia','',''),
    ('TIA','SNOMED','751371000000107','Personal history of transient ischaemic attack (situation)','',''),
    ('TIA','SNOMED','15258001','Subclavian steal syndrome (disorder)','',''),
    ('TIA','SNOMED','195199008','Vertebrobasilar artery syndrome (disorder)','',''),
    ('TIA','SNOMED','195205001','Impending cerebral ischemia (disorder)','',''),
    ('TIA','SNOMED','34781003','Vertebral artery syndrome (disorder)','',''),
    ('TIA','SNOMED','64009001','Basilar artery syndrome (disorder)','',''),

    ('Peripheral_artery_disease','ICD10',  'I70',  'Atherosclerosis','',''),
    ('Peripheral_artery_disease','ICD10',  'I700',  'Atherosclerosis of aorta','',''),
    ('Peripheral_artery_disease','ICD10',  'I701',  'Atherosclerosis of renal artery','',''),
    ('Peripheral_artery_disease','ICD10',  'I702',  'Atherosclerosis of arteries of extremities','',''),
    ('Peripheral_artery_disease','ICD10',  'I708',  'Atherosclerosis of other arteries','',''),
    ('Peripheral_artery_disease','ICD10',  'I709',  'Generalized and unspecified atherosclerosis','',''),
    ('Peripheral_artery_disease','ICD10',  'I71',  'Aortic aneurysm and dissection','',''),
    ('Peripheral_artery_disease','ICD10',  'I710',  'Dissection of aorta [any part]','',''),
    ('Peripheral_artery_disease','ICD10',  'I711',  'Thoracic aortic aneurysm, ruptured','',''),
    ('Peripheral_artery_disease','ICD10',  'I712',  'Thoracic aortic aneurysm, without mention of rupture','',''),
    ('Peripheral_artery_disease','ICD10',  'I713',  'Abdominal aortic aneurysm, ruptured','',''),
    ('Peripheral_artery_disease','ICD10',  'I714',  'Abdominal aortic aneurysm, without mention of rupture','',''),
    ('Peripheral_artery_disease','ICD10',  'I715',  'Thoracoabdominal aortic aneurysm, ruptured','',''),
    ('Peripheral_artery_disease','ICD10',  'I716',  'Thoracoabdominal aortic, without mention of rupture','',''),
    ('Peripheral_artery_disease','ICD10',  'I718',  'Aortic aneurysm of unspecified site, ruptured','',''),
    ('Peripheral_artery_disease','ICD10',  'I719',  'Aortic aneurysm of unspecified site, without mention of rupture','',''),

    # Non-fatal PAD SNOMED codes (Carmen 25.11.2024)
    ('Peripheral_artery_disease','SNOMED','301755001','Ischemic foot (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','233962007','Critical lower limb ischemia (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','307408003','Ischemic toe (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','840580004','Peripheral arterial disease (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','713412006','Ischemic foot with rest pain (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','312822006','Critical ischemia of foot (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','300917007','Ischemia of feet (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','399957001','Peripheral arterial occlusive disease (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','233958001','Peripheral ischemia (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','233961000','Lower limb ischemia (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','723870003','Acute occlusion of artery of lower limb co-occurrent and due to thromboembolus (disorder)','',''),
    ('Peripheral_artery_disease','SNOMED','307406004','Trash foot (disorder)','',''),

    ('nonfatal_stroke','ICD10',  'I62',   'Other nontraumatic intracranial haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I620',  'Subdural haemorrhage (acute)(nontraumatic)','',''),
    ('nonfatal_stroke','ICD10',  'I621',  'Nontraumatic extradural haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I629',  'Intracranial haemorrhage (nontraumatic), unspecified','',''),
    ('nonfatal_stroke','ICD10',  'I60',   'Subarachnoid haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I600',  'Subarachnoid haemorrhage from carotid siphon and bifurcation','',''),
    ('nonfatal_stroke','ICD10',  'I601',  'Subarachnoid haemorrhage from middle cerebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I602',  'Subarachnoid haemorrhage from anterior communicating artery','',''),
    ('nonfatal_stroke','ICD10',  'I603',  'Subarachnoid haemorrhage from posterior communicating artery','',''),
    ('nonfatal_stroke','ICD10',  'I604',  'Subarachnoid haemorrhage from basilar artery','',''),
    ('nonfatal_stroke','ICD10',  'I605',  'Subarachnoid haemorrhage from vertebral artery','',''),
    ('nonfatal_stroke','ICD10',  'I606',  'Subarachnoid haemorrhage from other intracranial arteries','',''),
    ('nonfatal_stroke','ICD10',  'I607',  'Subarachnoid haemorrhage from intracranial artery, unspecified','',''),
    ('nonfatal_stroke','ICD10',  'I608',  'Other subarachnoid haemorrhage','',''),
    ('nonfatal_stroke','ICD10',  'I609',  'Subarachnoid haemorrhage, unspecified','',''),
    ('nonfatal_stroke','ICD10',  'I671',  'Cerebral aneurysm, nonruptured','',''),
    ('nonfatal_stroke','ICD10',  'I675',  'Moyamoya disease','',''),
    ('nonfatal_stroke','ICD10',  'I682',  'Cerebral arteritis in other diseases classified elsewhere','',''),

    ('ischaemic_heart_disease','ICD10',  'I24',   'Other acute ischaemic heart diseases','',''),
    ('ischaemic_heart_disease','ICD10',  'I240',   'Coronary thrombosis not resulting in myocardial infarction','',''),
    ('ischaemic_heart_disease','ICD10',  'I241',   'Dressler syndrome','',''),
    ('ischaemic_heart_disease','ICD10',  'I248',   'Other forms of acute ischaemic heart disease','',''),
    ('ischaemic_heart_disease','ICD10',  'I249',   'Acute ischaemic heart disease, unspecified','',''),
    ('ischaemic_heart_disease','ICD10',  'I25',   'Chronic ischaemic heart disease, so described','',''), #ccu004-03
    ('ischaemic_heart_disease','ICD10',  'I250',   'Atherosclerotic cardiovascular disease, so described','',''),
    ('ischaemic_heart_disease','ICD10',  'I251',   'Atherosclerotic heart disease','',''),
    ('ischaemic_heart_disease','ICD10',  'I252',   'Old myocardial infarction','',''), #ccu004-03
    ('ischaemic_heart_disease','ICD10',  'I253',   'Aneurysm of heart','',''),
    ('ischaemic_heart_disease','ICD10',  'I254',   'Coronary artery aneurysm and dissection','',''),
    ('ischaemic_heart_disease','ICD10',  'I255',   'Ischaemic cardiomyopathy','',''),
    ('ischaemic_heart_disease','ICD10',  'I256',   'Silent myocardial ischaemia','',''),
    ('ischaemic_heart_disease','ICD10',  'I258',   'Other forms of chronic ischaemic heart disease','',''),
    ('ischaemic_heart_disease','ICD10',  'I259',   'Chronic ischaemic heart disease, unspecified','',''),

    # Non-fatal IHD SNOMED codes (Carmen 25.11.2024)
    ('ischaemic_heart_disease','SNOMED','10273003','Acute infarction of papillary muscle','',''),
    ('ischaemic_heart_disease','SNOMED','1089471000000109','Acute transmural myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','12238151000119107','Acute ST segment elevation myocardial infarction of inferoposterior wall (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','129574000','Postoperative myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','15712881000119105','Acute ST segment elevation myocardial infarction of anterolateral wall (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','15712921000119103','Acute ST segment elevation myocardial infarction of lateral wall (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','15990001','Acute myocardial infarction of posterolateral wall','',''),
    ('ischaemic_heart_disease','SNOMED','161502000','H/O: myocardial infarct at less than 60','',''),
    ('ischaemic_heart_disease','SNOMED','161503005','H/O: myocardial infarct at greater than 60','',''),
    ('ischaemic_heart_disease','SNOMED','1755008','Old myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194802003','True posterior myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194809007','Acute atrial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194856005','Subsequent myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194857001','Subsequent myocardial infarction of anterior wall','',''),
    ('ischaemic_heart_disease','SNOMED','194858006','Subsequent myocardial infarction of inferior wall','',''),
    ('ischaemic_heart_disease','SNOMED','194862000','Haemopericardium as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194863005','Atrial septal defect as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194865003','Rupture of cardiac wall without hemopericardium as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194866002','Rupture of chordae tendinae as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194867006','Rupture of papillary muscle as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','194868001','Thrombosis of atrium, auricular appendage, and ventricle as current complications following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','22298006','Myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','233825009','Acute Q wave infarction - anteroseptal (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233826005','Acute non-Q wave infarction - anteroseptal (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233827001','Acute Q wave infarction - anterolateral (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233828006','Acute non-Q wave infarction - anterolateral (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233829003','Acute Q wave infarction - inferior (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233830008','Acute non-Q wave infarction - inferior (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233831007','Acute Q wave infarction - inferolateral (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233832000','Acute non-Q wave infarction - inferolateral (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233833005','Acute Q wave infarction - lateral (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233834004','Acute non-Q wave infarction - lateral (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233835003','Acute widespread myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233838001','Acute posterior myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','233843008','Silent myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','233846000','Ventricular septal defect as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','233847009','Cardiac rupture after acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','285991000119100','Acute ST segment elevation myocardial infarction involving left main coronary artery (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','304914007','Acute Q wave myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','307140009','Acute non-Q wave infarction','',''),
    ('ischaemic_heart_disease','SNOMED','308065005','H/O: Myocardial infarction in last year','',''),
    ('ischaemic_heart_disease','SNOMED','311792005','Postoperative transmural myocardial infarction of anterior wall','',''),
    ('ischaemic_heart_disease','SNOMED','311793000','Postoperative transmural myocardial infarction of inferior wall','',''),
    ('ischaemic_heart_disease','SNOMED','311796008','Postoperative subendocardial myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','314207007','Non-Q wave myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','371068009','Myocardial infarction with complication (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','394659003','Acute coronary syndrome (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','394710008','First myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','398274000','Coronary artery thrombosis (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','399211009','History of - myocardial infarction (context-dependent category)','',''),
    ('ischaemic_heart_disease','SNOMED','401303003','Acute ST segment elevation myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','401314000','Acute non-ST segment elevation myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','42531007','Microinfarct of heart','',''),
    ('ischaemic_heart_disease','SNOMED','428196007','Mixed myocardial ischemia and infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','428752002','Recent myocardial infarction (situation)','',''),
    ('ischaemic_heart_disease','SNOMED','52035003','Acute anteroapical myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','54329005','Acute myocardial infarction of anterior wall','',''),
    ('ischaemic_heart_disease','SNOMED','57054005','Acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','58612006','Acute myocardial infarction of lateral wall','',''),
    ('ischaemic_heart_disease','SNOMED','62695002','Acute anteroseptal myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','65547006','Acute myocardial infarction of inferolateral wall','',''),
    ('ischaemic_heart_disease','SNOMED','66189004','Postmyocardial infarction syndrome','',''),
    ('ischaemic_heart_disease','SNOMED','698593009','History of non-ST segment elevation myocardial infarction (situation)','',''),
    ('ischaemic_heart_disease','SNOMED','70211005','Acute myocardial infarction of anterolateral wall','',''),
    ('ischaemic_heart_disease','SNOMED','703164000','Acute ST segment elevation myocardial infarction of anterior wall (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','703209002','Subsequent ST segment elevation myocardial infarction of inferior wall (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','703211006','Subsequent ST segment elevation myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','703213009','Acute ST segment elevation myocardial infarction of inferior wall (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','703360004','Subsequent non-ST segment elevation myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','70422006','Acute subendocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','73795002','Acute myocardial infarction of inferior wall','',''),
    ('ischaemic_heart_disease','SNOMED','76593002','Acute myocardial infarction of inferoposterior wall','',''),
    ('ischaemic_heart_disease','SNOMED','79009004','Acute myocardial infarction of septum','',''),
    ('ischaemic_heart_disease','SNOMED','53741008','Coronary arteriosclerosis (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','92517006','Calcific coronary arteriosclerosis (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','194821006','Coronary thrombosis not resulting in myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','194823009','Acute coronary insufficiency (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','194842008','Single coronary vessel disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','194843003','Double coronary vessel disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233817007','Triple vessel disease of the heart (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233839009','Old anterior myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233840006','Old inferior myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233841005','Old lateral myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233842003','Old posterior myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','233844002','Accelerated coronary artery disease in transplanted heart (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','315348000','Asymptomatic coronary heart disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','371803003','Multi vessel coronary artery disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','371804009','Left main coronary artery disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','413439005','Acute ischemic heart disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','413838009','Chronic ischemic heart disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','413844008','Chronic myocardial ischemia (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','414545008','Ischemic heart disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','414795007','Myocardial ischemia (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','443502000','Atherosclerosis of coronary artery (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','719678003','Non-obstructive atherosclerosis of coronary artery (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','810681000000101','Coronary microvascular disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','1077002','Septal infarction by electrocardiogram','',''),
    ('ischaemic_heart_disease','SNOMED','1089431000000107','Postoperative nontransmural myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','1089441000000103','Postoperative transmural myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','1089451000000100','Acute nontransmural myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','12238111000119106','Acute ST segment elevation myocardial infarction of inferolateral wall','',''),
    ('ischaemic_heart_disease','SNOMED','15712841000119100','Acute ST segment elevation myocardial infarction of posterolateral wall','',''),
    ('ischaemic_heart_disease','SNOMED','15712961000119108','Acute ST segment elevation myocardial infarction of anteroseptal wall','',''),
    ('ischaemic_heart_disease','SNOMED','15713041000119103','Acute ST segment elevation myocardial infarction of posterior wall','',''),
    ('ischaemic_heart_disease','SNOMED','15713081000119108','Acute ST segment elevation myocardial infarction due to left coronary artery occlusion','',''),
    ('ischaemic_heart_disease','SNOMED','15713121000119105','Acute ST segment elevation myocardial infarction due to right coronary artery occlusion','',''),
    ('ischaemic_heart_disease','SNOMED','15713161000119100','Acute ST segment elevation myocardial infarction of septum','',''),
    ('ischaemic_heart_disease','SNOMED','15713201000119105','Acute ST segment elevation myocardial infarction of posterobasal wall','',''),
    ('ischaemic_heart_disease','SNOMED','15960981000119105','Mural thrombus of left ventricle following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','16837681000119104','Acute myocardial infarction of posterolateral wall','',''),
    ('ischaemic_heart_disease','SNOMED','17531000119105','Myocardial infarction due to demand ischemia','',''),
    ('ischaemic_heart_disease','SNOMED','23311000119105','Atrial septal defect due to and following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','233836002','Rupture of cardiac wall without hemopericardium as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','233837006','Rupture of chordae tendinae due to and following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','282006','Rupture of papillary muscle as current complication following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','285981000119103','Acute ST segment elevation myocardial infarction involving left anterior descending coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','30277009','Myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','413444003','Acute Q wave infarction - anteroseptal','',''),
    ('ischaemic_heart_disease','SNOMED','41466009','Acute non-Q wave infarction - anteroseptal','',''),
    ('ischaemic_heart_disease','SNOMED','418044006','Acute Q wave infarction - anterolateral','',''),
    ('ischaemic_heart_disease','SNOMED','429391004','Acute Q wave infarction - inferior','',''),
    ('ischaemic_heart_disease','SNOMED','59063002','Acute non-Q wave infarction - lateral','',''),
    ('ischaemic_heart_disease','SNOMED','63670007','Acute Q wave infarction - widespread','',''),
    ('ischaemic_heart_disease','SNOMED','64627002','Acute non-Q wave infarction - widespread','',''),
    ('ischaemic_heart_disease','SNOMED','703165004','Cardiac rupture due to and following acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','703210007','Acute ST segment elevation myocardial infarction involving left anterior descending coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','703212004','Rupture of ventricle due to acute myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','703251009','Acute Q wave myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','703252002','Acute non-Q wave infarction','',''),
    ('ischaemic_heart_disease','SNOMED','703253007','Postoperative transmural myocardial infarction of anterior wall','',''),
    ('ischaemic_heart_disease','SNOMED','703326006','Postoperative transmural myocardial infarction of inferior wall','',''),
    ('ischaemic_heart_disease','SNOMED','703328007','Postoperative subendocardial myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','703330009','Non-Q wave myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','70998009','Myocardial infarction with complication','',''),
    ('ischaemic_heart_disease','SNOMED','71023004','Acute coronary syndrome','',''),
    ('ischaemic_heart_disease','SNOMED','736978009','First myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','836294006','Myocardial infarction in recovery phase','',''),
    ('ischaemic_heart_disease','SNOMED','836295007','Acute myocardial infarction of inferolateral wall','',''),
    ('ischaemic_heart_disease','SNOMED','840309000','Acute ST segment elevation myocardial infarction due to occlusion of proximal portion of anterior descending branch of left coronary artery ','',''),
    ('ischaemic_heart_disease','SNOMED','840312002','Acute ST segment elevation myocardial infarction due to occlusion of mid portion of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','840316004','Acute ST segment elevation myocardial infarction due to occlusion of distal portion of anterior descending branch of left coronary artery ','',''),
    ('ischaemic_heart_disease','SNOMED','840609007','Acute ST segment elevation myocardial infarction due to occlusion of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','840680009','Acute ST segment elevation myocardial infarction due to occlusion of septal branch of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','846668006','Acute ST segment elevation myocardial infarction due to occlusion of diagonal branch of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','846683001','Acute ST segment elevation myocardial infarction due to occlusion of intermediate artery','',''),
    ('ischaemic_heart_disease','SNOMED','868217004','Acute ST segment elevation myocardial infarction due to occlusion of distal portion of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','868220007','Acute ST segment elevation myocardial infarction due to occlusion of mid portion of right coronary artery ','',''),
    ('ischaemic_heart_disease','SNOMED','868224003','Acute ST segment elevation myocardial infarction due to occlusion of marginal branch of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','868225002','Acute ST segment elevation myocardial infarction due to occlusion of posterior descending branch of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','868226001','Acute ST segment elevation myocardial infarction due to occlusion of posterior lateral branch of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','896691006','Acute ST segment elevation myocardial infarction due to occlusion of circumflex branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','896696001','Acute ST segment elevation myocardial infarction of apex of heart','',''),
    ('ischaemic_heart_disease','SNOMED','896697005','Acute ST segment elevation myocardial infarction of right ventricle','',''),
    ('ischaemic_heart_disease','SNOMED','896689003','Acute myocardial infarction due to occlusion of circumflex branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','726499301000119105','Myocardial infarction due to atherothrombotic coronary artery disease','',''),
    ('ischaemic_heart_disease','SNOMED','879955009','Myocardial infarction with non-obstructive coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','233885007','Post-infarction pericarditis','',''),
    ('ischaemic_heart_disease','SNOMED','429673002','Arteriosclerosis of coronary artery bypass graft','',''),
    ('ischaemic_heart_disease','SNOMED','724431008','Atherosclerosis of autologous coronary artery bypass graft','',''),
    ('ischaemic_heart_disease','SNOMED','723862008','Atherosclerosis of non-autologous coronary artery bypass graft','',''),
    ('ischaemic_heart_disease','SNOMED','11018701000119109','Coronary arteriosclerosis after percutaneous coronary angioplasty','',''),
    ('ischaemic_heart_disease','SNOMED','139011000119104','Coronary arteriosclerosis following coronary artery bypass graft','',''),
    ('ischaemic_heart_disease','SNOMED','16891191000119108','Coronary artery disease due to type 1 diabetes mellitus','',''),
    ('ischaemic_heart_disease','SNOMED','16891151000119103','Coronary artery disease due to type 2 diabetes mellitus','',''),
    ('ischaemic_heart_disease','SNOMED','420006002','Obliterative coronary artery disease','',''),
    ('ischaemic_heart_disease','SNOMED','421327009','Coronary artery stent thrombosis','',''),
    ('ischaemic_heart_disease','SNOMED','28248000','Left anterior descending coronary artery thrombosis','',''),
    ('ischaemic_heart_disease','SNOMED','56276002','Left main coronary artery thrombosis','',''),
    ('ischaemic_heart_disease','SNOMED','10365005','Right main coronary artery thrombosis','',''),
    ('ischaemic_heart_disease','SNOMED','103011000119106','Coronary arteriosclerosis in patient with history of previous myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','285721000119104','History of acute ST segment elevation myocardial infarction','',''),
    ('ischaemic_heart_disease','SNOMED','387785661000119105','History of myocardial infarction due to atherothrombotic coronary artery disease','',''),
    ('ischaemic_heart_disease','SNOMED','776219771000119107','History of myocardial infarction due to demand ischemia','',''),
    ('ischaemic_heart_disease','SNOMED','50570003','Aneurysm of coronary vessels','',''),
    ('ischaemic_heart_disease','SNOMED','16025471000119107','Aneurysm of coronary artery due to and following acute febrile mucocutaneous lymph node syndrome','',''),
    ('ischaemic_heart_disease','SNOMED','735565009','Perforation of coronary artery co-occurrent and due to aneurysm of coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','735566005','Rupture of coronary artery co-occurrent and due to aneurysm of coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','445512009','Calcification of coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','429245005','Recurrent coronary arteriosclerosis after percutaneous transluminal coronary angioplasty','',''),
    ('ischaemic_heart_disease','SNOMED','427919004','Coronary arteriosclerosis caused by radiation','',''),
    ('ischaemic_heart_disease','SNOMED','371805005','Significant coronary bypass graft disease ','',''),
    ('ischaemic_heart_disease','SNOMED','62827000','Coronary arteritis ','',''),
    ('ischaemic_heart_disease','SNOMED','29899005','Coronary artery embolism','',''),
    ('ischaemic_heart_disease','SNOMED','286391000119108','Acquired coronary artery fistula','',''),
    ('ischaemic_heart_disease','SNOMED','703386009','Iatrogenic coronary artery fistula ','',''),
    ('ischaemic_heart_disease','SNOMED','234010000','Coronary artery perforation','',''),
    ('ischaemic_heart_disease','SNOMED','28931004','Coronary artery rupture','',''),
    ('ischaemic_heart_disease','SNOMED','233970002','Coronary artery stenosis','',''),
    ('ischaemic_heart_disease','SNOMED','251024009','Coronary graft stenosis','',''),
    ('ischaemic_heart_disease','SNOMED','59062007','Coronary stricture','',''),
    ('ischaemic_heart_disease','SNOMED','876859003','Stenosis of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1254663008','Stenosis of distal portion of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1255266003','Stenosis of mid portion of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1255623001','Stenosis of ostium of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1255265004','Stenosis of proximal portion of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','876858006','Stenosis of circumflex branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1255258007','Stenosis of distal portion of circumflex branch of left coronary artery ','',''),
    ('ischaemic_heart_disease','SNOMED','1255257002','Stenosis of mid portion of circumflex branch of left coronary artery ','',''),
    ('ischaemic_heart_disease','SNOMED','1255624007','Stenosis of ostium of circumflex branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1255256006','Stenosis of proximal portion of circumflex branch of left coronary artery ','',''),
    ('ischaemic_heart_disease','SNOMED','1255264000','Stenosis of coronary intermediate artery ','',''),
    ('ischaemic_heart_disease','SNOMED','1254664002','Stenosis of diagonal branch of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','876857001','Stenosis of left coronary artery main stem ','',''),
    ('ischaemic_heart_disease','SNOMED','1255621004','Stenosis of distal segment of left coronary artery main stem','',''),
    ('ischaemic_heart_disease','SNOMED','1255622006','Stenosis of mid segment of left coronary artery main stem','',''),
    ('ischaemic_heart_disease','SNOMED','1255189004','Stenosis of left posterior descending circumflex coronary arter','',''),
    ('ischaemic_heart_disease','SNOMED','1255188007','Stenosis of left posterior lateral branch of circumflex branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1255187002','Stenosis of obtuse marginal branch of circumflex branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','876856005','Stenosis of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1254659001','Stenosis of distal portion of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1254658009','Stenosis of mid portion of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1254662003','Stenosis of posterior descending branch of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1254661005','Stenosis of posterior lateral branch of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1254657004','Stenosis of proximal portion of right coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','1254665001','Stenosis of septal branch of anterior descending branch of left coronary artery','',''),
    ('ischaemic_heart_disease','SNOMED','63739005','Coronary occlusion','',''),
    ('ischaemic_heart_disease','SNOMED','62207008',' Syphilitic ostial coronary disease (disorder)"                                                ','',''),
    ('ischaemic_heart_disease','SNOMED','123641001',' Left coronary artery occlusion (disorder)"                                                   ','',''),
    ('ischaemic_heart_disease','SNOMED','123642008',' Right coronary artery occlusion (disorder)"                                                  ','',''),
    ('ischaemic_heart_disease','SNOMED','213037002',' Mechanical complication of coronary bypass (disorder)"                                       ','',''),
    ('ischaemic_heart_disease','SNOMED','240567009',' Syphilitic coronary artery disease (disorder)"                                               ','',''),
    ('ischaemic_heart_disease','SNOMED','408546009',' Coronary artery bypass graft occlusion (disorder)"                                           ','',''),
    ('ischaemic_heart_disease','SNOMED','442224005',' Arteriosclerosis of autologous vein coronary artery bypass graft (disorder)"                 ','',''),
    ('ischaemic_heart_disease','SNOMED','442240008',' Arteriosclerosis of nonautologous coronary artery bypass graft (disorder)"                   ','',''),
    ('ischaemic_heart_disease','SNOMED','442421004',' Arteriosclerosis of arterial coronary artery bypass graft (disorder)"                        ','',''),
    ('ischaemic_heart_disease','SNOMED','444855007',' Arteriosclerosis of coronary artery bypass graft of transplanted heart (disorder)"           ','',''),
    ('ischaemic_heart_disease','SNOMED','444856008',' Arteriosclerosis of internal mammary artery coronary artery bypass graft (disorder)"         ','',''),
    ('ischaemic_heart_disease','SNOMED','703356002',' Coronary artery occlusion due to neoplastic disease (disorder)"                              ','',''),
    ('ischaemic_heart_disease','SNOMED','732230001',' Dissection of coronary artery (disorder)"                                                    ','',''),
    ('ischaemic_heart_disease','SNOMED','840310005',' Occlusion of proximal portion of anterior descending branch of left coronary artery (diso..."','',''),
    ('ischaemic_heart_disease','SNOMED','840313007',' Occlusion of mid portion of anterior descending branch of left coronary artery (disorder)"   ','',''),
    ('ischaemic_heart_disease','SNOMED','840315000',' Occlusion of distal portion of anterior descending branch of left coronary artery (disorder)"','',''),
    ('ischaemic_heart_disease','SNOMED','840608004',' Occlusion of anterior descending branch of left coronary artery (disorder)"                  ','',''),
    ('ischaemic_heart_disease','SNOMED','840679006',' Occlusion of septal branch of anterior descending branch of left coronary artery (disorder)" ','',''),
    ('ischaemic_heart_disease','SNOMED','846667001',' Occlusion of diagonal branch of anterior descending branch of left coronary artery (disor..."','',''),
    ('ischaemic_heart_disease','SNOMED','846684007',' Occlusion of intermediate artery (disorder)"                                                 ','',''),
    ('ischaemic_heart_disease','SNOMED','868215007',' Occlusion of proximal portion of right coronary artery (disorder)"                           ','',''),
    ('ischaemic_heart_disease','SNOMED','868216008',' Occlusion of distal portion of right coronary artery (disorder)"                             ','',''),
    ('ischaemic_heart_disease','SNOMED','868219001',' Occlusion of mid portion of right coronary artery (disorder)"                                ','',''),
    ('ischaemic_heart_disease','SNOMED','868221006',' Occlusion of marginal branch of right coronary artery (disorder)"                            ','',''),
    ('ischaemic_heart_disease','SNOMED','868222004',' Occlusion of posterior descending branch of right coronary artery (disorder)"                ','',''),
    ('ischaemic_heart_disease','SNOMED','868223009',' Occlusion of posterior lateral branch of right coronary artery (disorder)"                   ','',''),
    ('ischaemic_heart_disease','SNOMED','896690007',' Occlusion of circumflex branch of left coronary artery (disorder)"                           ','',''),
    ('ischaemic_heart_disease','SNOMED','1197364005',' Idiopathic spontaneous coronary artery dissection (disorder)"                               ','',''),
    ('ischaemic_heart_disease','SNOMED','1204127004',' Chronic dissection of coronary artery (disorder)"                                           ','',''),
    ('ischaemic_heart_disease','SNOMED','1204153007',' Acute dissection of coronary artery (disorder)"                                             ','',''),
    ('ischaemic_heart_disease','SNOMED','1254660006',' Stenosis of marginal branch of right coronary artery (disorder)"                            ','',''),
    ('ischaemic_heart_disease','SNOMED','1269257006',' Arteriosclerosis of nonautologous biological coronary artery bypass graft (disorder)"       ','',''),
    ('ischaemic_heart_disease','SNOMED','44761000087104',' Occlusion of distal portion of circumflex branch of left coronary artery (disorder)"    ','',''),
    ('ischaemic_heart_disease','SNOMED','44771000087108',' Occlusion of mid portion of circumflex branch of left coronary artery (disorder)"       ','',''),
    ('ischaemic_heart_disease','SNOMED','44781000087105',' Occlusion of obtuse marginal branch of circumflex branch of left coronary artery (di..."','',''),
    ('ischaemic_heart_disease','SNOMED','44791000087107',' Occlusion of posterior lateral branch of circumflex branch of left coronary artery (..."','',''),
    ('ischaemic_heart_disease','SNOMED','44801000087106',' Occlusion of proximal portion of circumflex branch of left coronary artery (disorder)"  ','',''),
    ('ischaemic_heart_disease','SNOMED','117051000119103',' Chronic total occlusion of coronary artery (disorder)"                                 ','',''),
    ('ischaemic_heart_disease','SNOMED','139971000119106',' Thrombosis of coronary artery bypass graft (disorder)"                                 ','',''),
    ('ischaemic_heart_disease','SNOMED','285141000119106',' Arteriosclerosis of autologous arterial coronary artery bypass graft (disorder)"       ','',''),
    ('ischaemic_heart_disease','SNOMED','285151000119108',' Arteriosclerosis of autologous coronary artery bypass graft (disorder)"                ','',''),
    ('ischaemic_heart_disease','SNOMED','11018701000119100',' Coronary arteriosclerosis after percutaneous coronary angioplasty (disorder)"        ','',''),
    ('ischaemic_heart_disease','SNOMED','16025471000119100',' Aneurysm of coronary artery due to and following acute febrile mucocutaneous lymp..."','',''),
    ('ischaemic_heart_disease','SNOMED','16891151000119100',' Coronary artery disease due to type 2 diabetes mellitus (disorder)"                  ','',''),
    ('ischaemic_heart_disease','SNOMED','16891191000119100',' Coronary artery disease due to type 1 diabetes mellitus (disorder)"   ','',''),
    ('ischaemic_heart_disease','SNOMED','441541008','Takotsubo cardiomyopathy','',''),
    ('ischaemic_heart_disease','SNOMED','32574007','Past myocardial infarction diagnosed on electrocardiogram AND/OR other special investigation, but currently presenting no symptoms (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','281091000','Ischemic myocardial dysfunction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','697976003','Microvascular ischemia of myocardium (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','712866001','Resting ischemia co-occurrent and due to ischemic heart disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','713405002','Subacute ischemic heart disease (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','723858002','Ventricular aneurysm due to and following acute myocardial infarction (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','868214006','Acute ST segment elevation myocardial infarction due to occlusion of proximal portion of right coronary artery (disorder)','',''),
    ('ischaemic_heart_disease','SNOMED','16528621000119102','Rupture of interventricular septum following acute myocardial infarction (disorder)','','')
 

  
  ],
  
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)


# COMMAND ----------

# codelist_cvd excludes the following three 4-character codes: I671, I675 and I682; but currently includes the 3-character codes: I67 and I68
# to avoid accidentally matching the three 4-character codes that are to be excluded via their corresponding 3-character code, the I67* and I68* codes will need to be handled separately when codelist matching

#ccu004-03 commented this out
#assert codelist_nonfatal.where(f.col('term').isin(['I67', 'I68'])).count() == 0

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

#append non-fatal CVD outcomes to extra CVD codes (for exclusions)

#codelist_hx_nonfatal = codelist_nonfatal.append(codelist_hx_nonfatal) #ccu004-03
#codelist_hx_nonfatal = pd.concat([codelist_hx_nonfatal, pd.DataFrame([codelist_nonfatal])], ignore_index=True)
#codelist_hx_nonfatal = pd.concat([codelist_hx_nonfatal, codelist_nonfatal])
codelist_hx_nonfatal = codelist_hx_nonfatal.union(codelist_nonfatal)

# COMMAND ----------

display(codelist_hx_nonfatal)

# COMMAND ----------

codelist_nonfatal_outcome = (
  # ICD10 codes
  codelist_nonfatal
  # SNOMED codes
  .unionByName(codelist_bhf)
)

# COMMAND ----------

#ccu004-03
codelist_hx_nonfatal_exclusion = ( 
  # ICD10 codes
  codelist_hx_nonfatal
  # SNOMED codes
  .unionByName(codelist_bhf)
)

# COMMAND ----------

# check
tmpt = tab(codelist_nonfatal_outcome, 'name', 'terminology'); print()
print(codelist_nonfatal_outcome.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()


# COMMAND ----------

#ccu004-03
# check
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'name', 'terminology'); print()
print(codelist_hx_nonfatal_exclusion.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()


# COMMAND ----------

# codelist = codelist.where(f.col('name').isin(
#   'nonfatal_myocardial_infarction',
#   'nonfatal_stroke',
#   'stroke_HS',
#   'stroke_IS',
#   'stroke_NOS',
#   'stroke_SAH',
#   'stroke_TIA',
#   'AMI',
#   'AF',
#   'hypertension',
#   'hypertension_drugs'))

# COMMAND ----------

# check
tmpt = tab(codelist_nonfatal_outcome, 'name'); print()
tmpt = tab(codelist_nonfatal_outcome, 'terminology'); print()
tmpt = tab(codelist_nonfatal_outcome, 'name', 'terminology'); print()
print(codelist_nonfatal_outcome.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

#ccu004-03

# check
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'name'); print()
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'terminology'); print()
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'name', 'terminology'); print()
print(codelist_hx_nonfatal_exclusion.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

display(codelist_nonfatal_outcome)

# COMMAND ----------

display(codelist_hx_nonfatal_exclusion)

# COMMAND ----------

# MAGIC %md ## 3.1 Recode

# COMMAND ----------

# MAGIC %md ### 3.1.1 For Outcomes

# COMMAND ----------

# check
tmpt = tab(codelist_nonfatal_outcome, 'name'); print()

codelist_nonfatal_outcome = (
codelist_nonfatal_outcome
  .withColumnRenamed('name', 'name_old')
  .withColumn('name', f.when(f.col('name_old').isin(['stroke_HS', 'stroke_IS', 'stroke_NOS', 'stroke_TIA']), f.lit('nonfatal_stroke')).otherwise(f.col('name_old')))
  .withColumn('name', f.when(f.col('name_old').isin(['AMI']), f.lit('nonfatal_myocardial_infarction')).otherwise(f.col('name')))
)

# check
tmpt = tab(codelist_nonfatal_outcome, 'name', 'name_old'); print()

# COMMAND ----------

# check
tmpt = tab(codelist_nonfatal_outcome, 'name'); print()
tmpt = tab(codelist_nonfatal_outcome, 'terminology'); print()
tmpt = tab(codelist_nonfatal_outcome, 'name', 'terminology'); print()
print(codelist_nonfatal_outcome.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ### 3.1.2 For Exclusions

# COMMAND ----------

#ccu004-03

# check
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'name'); print()

codelist_hx_nonfatal_exclusion = (
codelist_hx_nonfatal_exclusion
  .withColumnRenamed('name', 'name_old')
  .withColumn('name', f.when(f.col('name_old').isin(['stroke_HS', 'stroke_IS', 'stroke_NOS', 'stroke_TIA']), f.lit('nonfatal_stroke')).otherwise(f.col('name_old')))
  .withColumn('name', f.when(f.col('name_old').isin(['AMI']), f.lit('nonfatal_myocardial_infarction')).otherwise(f.col('name')))
)

# check
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'name', 'name_old'); print()

# COMMAND ----------

#ccu004-03

# check
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'name'); print()
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'terminology'); print()
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'name', 'terminology'); print()
print(codelist_hx_nonfatal_exclusion.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 4. Reformat

# COMMAND ----------

# MAGIC %md ### 4.1.1 For Outcomes

# COMMAND ----------

# DBTITLE 1,Reformatted codes _code_diff == 1
# remove trailing X's, decimal points, dashes, and spaces
codelist_nonfatal_outcome = (
  codelist_nonfatal_outcome
  .withColumn('_code_old', f.col('code'))
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))
  .withColumn('_code_diff', f.when(f.col('code') != f.col('_code_old'), 1).otherwise(0))
)

# check
tmpt = tab(codelist_nonfatal_outcome, '_code_diff'); print()
print(codelist_nonfatal_outcome.where(f.col('_code_diff') == 1).orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

# check codes that have been reformatted
display(codelist_nonfatal_outcome.where(f.col('_code_diff') == 1))

# COMMAND ----------

# tidy
codelist_nonfatal_outcome = codelist_nonfatal_outcome.drop('_code_old', '_code_diff')

# COMMAND ----------

# MAGIC %md ### 4.1.2 For Exclusions

# COMMAND ----------

#ccu004-03

# remove trailing X's, decimal points, dashes, and spaces
codelist_hx_nonfatal_exclusion = (
  codelist_hx_nonfatal_exclusion
  .withColumn('_code_old', f.col('code'))
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))
  .withColumn('_code_diff', f.when(f.col('code') != f.col('_code_old'), 1).otherwise(0))
)

# check
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, '_code_diff'); print()
print(codelist_hx_nonfatal_exclusion.where(f.col('_code_diff') == 1).orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

#ccu004-03
# check codes that have been reformatted
display(codelist_hx_nonfatal_exclusion.where(f.col('_code_diff') == 1))

# COMMAND ----------

#ccu004-03
# tidy
codelist_hx_nonfatal_exclusion = codelist_hx_nonfatal_exclusion.drop('_code_old', '_code_diff')

# COMMAND ----------

# MAGIC %md # 5. Check

# COMMAND ----------

# MAGIC %md ### 5.1.1 For Outcomes

# COMMAND ----------

# check 
tmpt = tab(codelist_nonfatal_outcome, 'name', 'terminology')

# COMMAND ----------

# check
display(codelist_nonfatal_outcome)

# COMMAND ----------

# check
display(codelist_nonfatal_outcome.where(f.col('terminology') == 'ICD10'))

# COMMAND ----------

display(codelist_nonfatal_outcome.where(f.col('terminology') == 'SNOMED'))

# COMMAND ----------

# MAGIC %md ### 5.1.2 For Exclusions

# COMMAND ----------

#ccu004-03

# check 
tmpt_excl = tab(codelist_hx_nonfatal_exclusion, 'name', 'terminology')

# COMMAND ----------

#ccu004-03

# check
display(codelist_hx_nonfatal_exclusion)

# COMMAND ----------

#ccu004-03

# check
display(codelist_hx_nonfatal_exclusion.where(f.col('terminology') == 'ICD10'))

# COMMAND ----------

#ccu004-3

display(codelist_hx_nonfatal_exclusion.where(f.col('terminology') == 'SNOMED'))

# COMMAND ----------

# DBTITLE 1,Comparing prior codelist
#old = spark.table(f'{dbc}.{proj}_out_codelist_CVD')
#new = codelist
#key = ['name', 'terminology', 'code']
#file1, file2, file3, file3_differences = compare_files(old, new, key, warningError=0)

# COMMAND ----------

#tmpt = tab(file1, 'name'); print()
#tmpt = tab(file2, 'name', 'name_old_TWO'); print()

# COMMAND ----------

# MAGIC %md # 6. Save

# COMMAND ----------

save_table(df=codelist_nonfatal_outcome, out_name=f'{proj}_out_codelist_cvd', save_previous=True) 

# COMMAND ----------

#ccu004-03
save_table(df=codelist_hx_nonfatal_exclusion, out_name=f'{proj}_out_codelist_cvd_exclusion', save_previous=True) 