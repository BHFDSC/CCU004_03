# Databricks notebook source
# MAGIC %md # CCU004_03-D04b-LSOA
# MAGIC  
# MAGIC **Description** This notebook creates the covariates based on LSOA. LSOA will be used to derive LSOA, region and index of multiple deprivation.
# MAGIC
# MAGIC *Previous versions of this notebook incorporated LSOA batches from only the first and latest GDPPR batches. This updated version will employ a **staggered derivation** and LSOA will be obtained as at the closest date to the baseline batch. That is, the LSOA was taken from the first archived version of GDPPR (which has up until this point typically been the closest batch to baseline) and where persons were not included in this first batch of GDPPR, their LSOA would be extracted from the latest version of LSOA. For persons who had only 1 LSOA across their full history or records this was sufficient but for people who change practice in between the first and last batch used, this detail is overlooked. For example if a person was not included in batch 1 (the closest typically to baseline) and then also had multiple LSOAs across different GDPPR versions then taking the latest LSOA may not be the most appropriate LSOA to use. There in fact may be a different LSOA with a closer REPORTING_PERIOD_END_DATE in an earlier batch.*
# MAGIC
# MAGIC This LSOA derivation notebook consists of two main parts:<br>
# MAGIC <br>**1. Full LSOA history**
# MAGIC <br>Derivation of an LSOA histories table. This table tabulates a persons **full LSOA history** existing **across all archived versions** of GDPPR. By accessing all archived versions of GDPPR (full GDPPR table) a full history of a persons `LSOA` by `REPORTING_PERIOD_END_DATE` is captured. With access to all LSOAs with `REPORTING_PERIOD_END_DATE`, `LSOA` can be validated/confirmed and verified throughout the full history rather than only at the latest archived version. This will allow the most appropriate `LSOA` to be chosen for a given project study start date in part 2<br>
# MAGIC <br>**2. Collapsed LSOA history**
# MAGIC <br>Collapsed history (one row per person) extracting the most appropriate LSOA for the study. A selection/priority group has been defined for this project to allow prioritisation of the different time periods around the study start date.
# MAGIC
# MAGIC Where x is the difference in years from the `REPORTING_PERIOD_END_DATE` to the study start date the priority groups **for LSOA** are defined as follows:
# MAGIC <br>**1.**. -0.5 <= x < 0 (half a year before)
# MAGIC <br>**2.** 0 <= x < +0.5 (half a year after)
# MAGIC <br>**3.** -5 <= x < -0.5 (**five years before**)
# MAGIC
# MAGIC **Please note** that the earlist `REPORTING_PERIOD_END_DATE` occurs as at 2020-05-18 which falls into selection group 2. This means the closest to baseline LSOAs all fall in group 2 or *after*. Thus, group 2 end date has been relaxed such that 0 <= x < +infinity (all years after) are considered and the closest to baseline LSOA is found for all individuals. Those who have a closest LSOA to baseline outside of the half year after baseline will be flagged.
# MAGIC
# MAGIC LSOA conflicts (different `LSOA` values which have the same `REPORTING_PERIOD_END_DATE`) will be highlighted.
# MAGIC
# MAGIC **Finally**, the LSOA values will be compared with their archived values. These archived values employed the first and last GDPPR archived versions (as described at the top) to find LSOA for each individual which meant that the closest to baseline LSOA was not necessarily being found. We will derive the difference between the archived and new LSOA approaches.
# MAGIC  
# MAGIC <br>**Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre) and adapted by Spencer Keene and Carmen Petitjean
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work for CCU003_05, CCU018_01 (Tom Bolton, John Nolan), earlier CCU002 sub-projects amd subsequently CCU002_07-D7a-covariates_LSOA
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Data output**
# MAGIC - **`CCU004_03_gdppr_lsoa_rped`** : Full `LSOA` and `REPORTING_PERIOD_END_DATE` history across all versions of GDPPR for all individuals in the population
# MAGIC - **`CCU004_03_gdppr_lsoa_rped_collapsed`** : Collapsed `LSOA` and `REPORTING_PERIOD_END_DATE` (one row per person) such that the closest to baseline `REPORTING_PERIOD_END_DATE` is chosen for LSOA. Note that this dataset includes those who have conflicts as at the closest to baseline `REPORTING_PERIOD_END_DATE`. These can be queried using `lsoa_conflict` == 1; for those who have no LSOA conflict `lsoa_conflict` == 0 (do note that for these persons there will be more than one row per person).
# MAGIC - **`CCU004_03_gdppr_lsoa_rped_conflicts_full`** : For those who have a conflict, as described above, their full `LSOA` and `REPORTING_PERIOD_END_DATE` history with region and IMD has been compiled for review.
# MAGIC - **`CCU004_03_lsoa`** : Collapsed `LSOA` and `REPORTING_PERIOD_END_DATE` (one row per person) such that the closest to baseline `REPORTING_PERIOD_END_DATE` is chosen for LSOA. Unlike `CCU004_03_gdppr_lsoa_rped_collapsed`, this dataset is strictly one row per person and conflicts here have been nulled. Note that an LSOA conflict does not imply that there will be a Region/IMD Decile/IMD Quintile conflict and in these cases these will be carried forward despite the LSOA conflict. See 'Final' section for more detail.

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Setup

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

# DBTITLE 1,lsoa_full_history
def lsoa_full_history(gdppr, pipeline_production_date):
  
  lsoa_rped_full = (
  #´all archived versions of gdppr
  gdppr
    .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), f.col('REPORTING_PERIOD_END_DATE'), 'LSOA')
    .where((f.col("LSOA").isNotNull()) & (f.col("PERSON_ID").isNotNull()))
    .distinct()
    .where(f.col('archived_on') <= pipeline_production_date) # ensure not working with a version > pipeline production date
    .orderBy(f.col("PERSON_ID"),f.col("REPORTING_PERIOD_END_DATE"))
  )
  
  return lsoa_rped_full

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

# -----------------------------------------------------------------------------
# Curated assets by the HDS team
# -----------------------------------------------------------------------------

# LSOA lookup tables
lsoa_region = spark.table(".hds_cur_lsoa_region_lookup")
lsoa_imd    = spark.table(".hds_cur_lsoa_2011_imd_lookup")

# Multisource LSOA tables
curated_lsoa = spark.table('.hds_curated_assets__lsoa_multisource_2024_10_24')

# COMMAND ----------

# # Carmen: commenting this out as we are now using curated assets by the HDS team (29.11.2024)
# # -----------------------------------------------------------------------------
# # LSOA Curated Data
# # -----------------------------------------------------------------------------
# lsoa_region = spark.table(path_cur_lsoa_region) 
# lsoa_imd    = spark.table(path_cur_lsoa_imd)

# # -----------------------------------------------------------------------------
# # GDPPR Paths, Dates & Data
# # -----------------------------------------------------------------------------
# parameters_df_gdppr = parameters_df_datasets.loc[parameters_df_datasets['dataset'] == 'gdppr']
# gdppr_path = (parameters_df_gdppr['database'].values[0] + '.' + parameters_df_gdppr['table'].values[0])
# gdppr_latest_archived_on = parameters_df_gdppr['archived_on'][0]

# gdppr = spark.table(gdppr_path)

# COMMAND ----------

# MAGIC %md ##2.1 Prepare

# COMMAND ----------

#lsoa_region = (lsoa_region.select(f.col('lsoa_code').alias('LSOA'), 'lsoa_name', f.col('region_name').alias('region')))
lsoa_region = lsoa_region.withColumnRenamed('lsoa_code', 'LSOA').withColumnRenamed('region_code', 'region')
lsoa_imd = lsoa_imd.withColumnRenamed('LSOA_2011', 'LSOA')

# COMMAND ----------

# MAGIC %md # 3. Full LSOA history

# COMMAND ----------

# DBTITLE 1,Run and Save Full LSOA history
# ##ccu004-03: This takes too long! 5.77 hours Reuse this table and begin from this cell.

# # compile all distinct LSOA and REPORTING_PERIOD_END_DATEs for each individual for all GDPPR archived_on versions
# # not filtering for RPEDs before study_start_date as a proj selection criteria may look slightly beyond baseline (or in most cases they may have to use an LSOA > study_start_date) as RPEDs start 2020-05

# lsoa_rped_full = lsoa_full_history(gdppr, pipeline_production_date)


# COMMAND ----------

# save_table(df=lsoa_rped_full, out_name=f'{proj}_gdppr_lsoa_rped_{cohort}', save_previous=True) #cohortrefactoring 

# COMMAND ----------

# # read back in data
# lsoa_rped_full = (
#   spark.table(f'{dsa}.{proj}_gdppr_lsoa_rped_{cohort}')
# #  spark.table(f'{dsa}.ccu004_01_gdppr_lsoa_rped') ccu004-03: maybe is still okay to use this until the above command can run
#   .orderBy("PERSON_ID","REPORTING_PERIOD_END_DATE","LSOA")
#   .withColumn('LSOA_1', f.substring(f.col('LSOA'), 1, 1))
# )

# display(lsoa_rped_full)

# COMMAND ----------

# MAGIC %md # 4. Collapsed LSOA history

# COMMAND ----------

# MAGIC %md ##4.1 Selection criteria

# COMMAND ----------

# Defining which LSOA record we prioritise and selecting the corresponding record
# If more than one records are identified (e.g. if there are ties, one is taken at random)
start_date = f.lit(study_start_date)
start_plus_6 = f.add_months(start_date, 6)
start_minus_6 = f.add_months(start_date, -6)
col_date = f.col('record_date')

selection_group = (f
    # Group 1: start_date-6m <= date < start_date (half a year before start)
    .when((col_date >= start_minus_6) & (col_date < start_date), 1)
    # Group 2: start_date <= date < start_date+6m (half a year after start)
    .when((col_date >= start_date) & (col_date < start_plus_6), 2)
    # Group 3: date < start_date (all years before start)
    .when(col_date < start_date, 3)
    # Group 4: start_date < date (all years after start)
    .when(col_date >= start_date, 4)
)

lsoa = (
    curated_lsoa
    .withColumn('_selection_group', selection_group)
    .withColumn('_diff', f.abs(f.datediff(f.col('record_date'), start_date)))
    .withColumn('_rank', f.row_number().over(Window.partitionBy('PERSON_ID').orderBy(f.asc('_selection_group'), f.asc('_diff'))))
    .where(f.col('_rank') == 1)
    .select('PERSON_ID', 'record_date', 'lsoa')
)

# COMMAND ----------

# Reformat column names to make it compatible with the rest of the pipeline
lsoa_reformat = (
    lsoa.withColumnRenamed('lsoa', 'LSOA')
    .withColumn('LSOA_1', f.substring('LSOA', 1, 1))
    )

# COMMAND ----------

display(lsoa_reformat.limit(10))

# COMMAND ----------

# MAGIC %md ##4.2 Add Region & IMD

# COMMAND ----------

# MAGIC %md Add region and IMD on to LSOAs chosen as at the closest to baseline `REPORTING_PERIOD_END_DATE`.

# COMMAND ----------

# check
tmpt = tab(lsoa_region, 'region'); print()
tmpt = tab(lsoa_imd, 'IMD_2019_DECILES', 'IMD_2019_QUINTILES', var2_unstyled=1); print()

# COMMAND ----------

lsoas_with_regions = (
    # Map LSOAs to regions 
    lsoa_reformat.join(lsoa_region, on='LSOA', how='left')
    # Overwrite $region with W for Wales and S for Scotland, otherwise keep it the same
    .withColumn('region', f
        .when(f.col('LSOA_1') == 'W', 'Wales')
        .when(f.col('LSOA_1') == 'S', 'Scotland')
        .otherwise(f.col('region'))
    )
)

tab(lsoas_with_regions, 'region')

# COMMAND ----------

display(lsoas_with_regions.limit(5))

# COMMAND ----------

# Merge with IMD
lsoa_closest_top_all = (
    lsoas_with_regions
    .join(lsoa_imd, on='LSOA', how='left')
)
tab(lsoa_closest_top_all, 'IMD_2019_DECILES')
lsoa_closest_top_all = temp_save(df=lsoa_closest_top_all, out_name=f'{proj}_tmp_lsoa_closest_top_all_{cohort}')

# COMMAND ----------

# Load (if re-running)
lsoa_closest_top_all = spark.table(f'{dsa}.{proj}_tmp_lsoa_closest_top_all_{cohort}')

# COMMAND ----------

# assert that each LSOA only has 1 region and IMD
assert lsoa_closest_top_all.select("LSOA","region","IMD_2019_DECILES","IMD_2019_QUINTILES").distinct().groupBy("LSOA").count().filter(f.col("count")>1).count() == 0

# COMMAND ----------

# MAGIC %md ##4.3 Conflicts

# COMMAND ----------

# MAGIC %md ###4.3.1 LSOA

# COMMAND ----------

# DBTITLE 1,LSOA conflicts
# MAGIC %md **98.4% of the individuals in GDPPR have no LSOA conflict as at the `REPORTING_PERIOD_END_DATE` closest to the `study_start_date`. Thus there are 1.6% individuals who have at least 2 different LSOAs as at the `REPORTING_PERIOD_END_DATE` closest to the `study_start_date`.**
# MAGIC
# MAGIC These %s may change slightly when the individuals are filtered to the project cohort.
# MAGIC
# MAGIC LSOA conflicts should be reviewed if LSOA is to be used as a covariate; however, the conflict % may change (reduce) if region and IMD are of interest instead.

# COMMAND ----------

# count no. of lsoas a person has as at their closest RPED and join back on
lsoa_counts = (lsoa_closest_top_all
               .groupBy("PERSON_ID")
               .count()
               .withColumnRenamed("count","lsoa_count")
               .withColumn('lsoa_conflict', f.when(f.col('lsoa_count') >1, 1).otherwise(0)))

lsoa_closest_top_all = lsoa_counts.join(lsoa_closest_top_all,on=["PERSON_ID"],how="left")

display(lsoa_closest_top_all)

# COMMAND ----------

# DBTITLE 1,% of Individuals with LSOA conflicts
# note that this is all individuals - not the cohort yet

# tabulate lsoa counts
tab(lsoa_closest_top_all, 'lsoa_count'); print()
# tabulate those with lsoa conflict and those with no conflict
tab(lsoa_closest_top_all, 'lsoa_conflict'); print()

# COMMAND ----------

# MAGIC %md ###4.3.2 Region

# COMMAND ----------

# DBTITLE 1,Region conflicts
# MAGIC %md **99.7% of the individuals in GDPPR have no region conflict as at the `REPORTING_PERIOD_END_DATE` closest to the `study_start_date`. Thus there are 0.3% individuals who have at least 2 different regions as at the `REPORTING_PERIOD_END_DATE` closest to the `study_start_date`.**
# MAGIC
# MAGIC These %s may change slightly when the individuals are filtered to the project cohort.

# COMMAND ----------

# count no. of regions a person has as at their closest RPED and join back on
region_counts = (lsoa_closest_top_all
               .select("PERSON_ID","region")
               .distinct()
               .groupBy("PERSON_ID")
               .count()
               .withColumnRenamed("count","region_count")
               .withColumn('region_conflict', f.when(f.col('region_count') >1, 1).otherwise(0)))

lsoa_closest_top_all = region_counts.join(lsoa_closest_top_all,on=["PERSON_ID"],how="left")

display(lsoa_closest_top_all)

# COMMAND ----------

# those with an lsoa conflict may not have a region conflict
display(lsoa_closest_top_all.filter(f.col("lsoa_conflict")==1).filter(f.col("region_conflict")==0))

# COMMAND ----------

# DBTITLE 1,% of Individuals with Region conflicts
# note that this is all individuals - not the cohort yet

# tabulate lsoa counts
tab(lsoa_closest_top_all.select("PERSON_ID","region_count","region_conflict").distinct(), 'region_count'); print()
# tabulate those with lsoa conflict and those with no conflict
tab(lsoa_closest_top_all.select("PERSON_ID","region_count","region_conflict").distinct(), 'region_conflict'); print()

# COMMAND ----------

# MAGIC %md ###4.3.3 IMD

# COMMAND ----------

# DBTITLE 1,IMD conflicts
# MAGIC %md 
# MAGIC **Deciles
# MAGIC <br>99.3% of the individuals in GDPPR have no IMD decile conflict as at the `REPORTING_PERIOD_END_DATE` closest to the `study_start_date`. Thus there are 0.7% individuals who have at least 2 different IMD deciles as at the `REPORTING_PERIOD_END_DATE` closest to the `study_start_date`.**
# MAGIC
# MAGIC These %s may change slightly when the individuals are filtered to the project cohort.
# MAGIC
# MAGIC **Quintiles
# MAGIC <br>99.4% of the individuals in GDPPR have no IMD quintile conflict as at the `REPORTING_PERIOD_END_DATE` closest to the `study_start_date`. Thus there are 0.6% individuals who have at least 2 different IMD quintiles as at the `REPORTING_PERIOD_END_DATE` closest to the `study_start_date`.**
# MAGIC
# MAGIC These %s may change slightly when the individuals are filtered to the project cohort.

# COMMAND ----------

# MAGIC %md ####4.3.3.1 Deciles

# COMMAND ----------

# DECILES
# count no. of imds a person has as at their closest RPED and join back on
imd_decile_counts = (lsoa_closest_top_all
               .select("PERSON_ID","IMD_2019_DECILES")
               .distinct()
               .groupBy("PERSON_ID")
               .count()
               .withColumnRenamed("count","imd_deciles_count")
               .withColumn('imd_deciles_conflict', f.when(f.col('imd_deciles_count') >1, 1).otherwise(0)))

lsoa_closest_top_all = imd_decile_counts.join(lsoa_closest_top_all,on=["PERSON_ID"],how="left")

display(lsoa_closest_top_all)

# COMMAND ----------

# those with an lsoa conflict may not have a imd deciles conflict
display(lsoa_closest_top_all.filter(f.col("lsoa_conflict")==1).filter(f.col("imd_deciles_conflict")==0))

# COMMAND ----------

# DBTITLE 1,% of Individuals with IMD Decile conflicts
# note that this is all individuals - not the cohort yet

# tabulate lsoa counts
tab(lsoa_closest_top_all.select("PERSON_ID","imd_deciles_count","imd_deciles_conflict").distinct(), 'imd_deciles_count'); print()
# tabulate those with lsoa conflict and those with no conflict
tab(lsoa_closest_top_all.select("PERSON_ID","imd_deciles_count","imd_deciles_conflict").distinct(), 'imd_deciles_conflict'); print()

# COMMAND ----------

# MAGIC %md ####4.3.3.1 Quintiles

# COMMAND ----------

# QUINTILES
# count no. of imds a person has as at their closest RPED and join back on
imd_decile_counts = (lsoa_closest_top_all
               .select("PERSON_ID","IMD_2019_QUINTILES")
               .distinct()
               .groupBy("PERSON_ID")
               .count()
               .withColumnRenamed("count","imd_quintiles_count")
               .withColumn('imd_quintiles_conflict', f.when(f.col('imd_quintiles_count') >1, 1).otherwise(0)))

lsoa_closest_top_all = imd_decile_counts.join(lsoa_closest_top_all,on=["PERSON_ID"],how="left")

display(lsoa_closest_top_all)

# COMMAND ----------

# those with an lsoa conflict may not have a imd quintiles conflict
display(lsoa_closest_top_all.filter(f.col("lsoa_conflict")==1).filter(f.col("imd_quintiles_conflict")==0))

# COMMAND ----------

# DBTITLE 1,% of Individuals with IMD Quintile conflicts
# note that this is all individuals - not the cohort yet

# tabulate lsoa counts
tab(lsoa_closest_top_all.select("PERSON_ID","imd_quintiles_count","imd_quintiles_conflict").distinct(), 'imd_quintiles_count'); print()
# tabulate those with lsoa conflict and those with no conflict
tab(lsoa_closest_top_all.select("PERSON_ID","imd_quintiles_count","imd_quintiles_conflict").distinct(), 'imd_quintiles_conflict'); print()

# COMMAND ----------

# MAGIC %md ###4.3.4 Save

# COMMAND ----------

# DBTITLE 1,Save collapsed histories
save_table(df=lsoa_closest_top_all, out_name=f'{proj}_gdppr_lsoa_rped_collapsed_{cohort}', save_previous=True)

# COMMAND ----------

lsoa_closest_top_all = spark.table(f'{dsa}.{proj}_gdppr_lsoa_rped_collapsed_{cohort}')
display(lsoa_closest_top_all)

# COMMAND ----------

# MAGIC %md ###4.3.5 Final - one row per person

# COMMAND ----------

# MAGIC %md Final LSOA version will deal with conflicts as below:
# MAGIC
# MAGIC **A.** If there is an LSOA conflict, resulting in a Region conflict, resulting in an IMD Deciles conflict resulting in an IMD Quintiles conflict then `LSOA`, `LSOA_1`, `lsoa_name`, `region`, `IMD_2019_DECILES`, `IMD_2019_QUINTILES` will **all** be nulled.
# MAGIC
# MAGIC <br>**B.** If there is an LSOA conflict, resulting in a Region conflict, resulting in an IMD Deciles conflict but this time does not result in an IMD Quintiles conflict (e.g. IMD Decile moves from 1 to 2 but this means the person remains in IMD Quintile 1) then `LSOA`, `LSOA_1`, `lsoa_name`, `region`, `IMD_2019_DECILES` will be nulled whilst `IMD_2019_QUINTILES` will be carried forward.
# MAGIC
# MAGIC <br>**C.** If there is an LSOA conflict resulting in a Region conflict but no IMD Deciles (and thus no IMD Quintiles) conflict then `LSOA`, `LSOA_1`, `lsoa_name` `region` will be nulled whilst `IMD_2019_DECILES`, `IMD_2019_QUINTILES` are carried forward.
# MAGIC
# MAGIC <br>**D.** If there is an LSOA conflict that does not result in a Region conflict but does result in a IMD Deciles (and thus IMD Quintiles) conflict then `LSOA`, `LSOA_1`, `lsoa_name` `IMD_2019_DECILES`, `IMD_2019_QUINTILES` will be nulled whilst `region` is carried forward.
# MAGIC
# MAGIC <br>**E.** If there is an LSOA conflict that does not result in a Region conflict but does result in a IMD Deciles but this time does not result in an IMD Quintiles conflict (e.g. IMD Decile moves from 1 to 2 but this means the person remains in IMD Quintile 1) then `LSOA`, `LSOA_1`, `lsoa_name` `IMD_2019_DECILES` will be nulled whilst `region` and `IMD_2019_QUINTILES` will be carried forward.
# MAGIC
# MAGIC <br>**F.** If there is an LSOA conflict that does **not** result in a Region conflict or an IMD Deciles (and thus IMD Quintiles) conflict then only `LSOA`, `LSOA_1`, `lsoa_name` will be nulled whilst `region`, `IMD_2019_DECILES`, `IMD_2019_QUINTILES` are carried forward.
# MAGIC
# MAGIC This will ensure that each person only has one row each.

# COMMAND ----------

lsoa_final = (lsoa_closest_top_all
             .withColumn("region",
                         f.when((f.col("region_conflict") == 1),f.lit(None)).otherwise(f.col("region")))
             .withColumn("IMD_2019_DECILES",
                         f.when((f.col("imd_deciles_conflict") == 1),f.lit(None)).otherwise(f.col("IMD_2019_DECILES")))
             .withColumn("IMD_2019_QUINTILES",
                         f.when((f.col("imd_quintiles_conflict") == 1),f.lit(None)).otherwise(f.col("IMD_2019_QUINTILES")))
            
)

lsoa_cols = ["LSOA", "LSOA_1", "lsoa_name"]

for col_name in lsoa_cols:
    lsoa_final = (lsoa_final
    .withColumn(col_name,
             f.when((f.col("lsoa_conflict") == 1),
                     f.lit(None)).otherwise(f.col(col_name)))
    )


# COMMAND ----------

lsoa_final = (lsoa_final
             .select("PERSON_ID",
                     "LSOA",
                     f.col("record_date").alias("LSOA_date"),
                     f.col("lsoa_conflict").alias("LSOA_conflict"),
                     'region_conflict',
                     'imd_deciles_conflict',
                     'imd_quintiles_conflict',
                     "LSOA_1",
                     "lsoa_name",
                     "region","IMD_2019_DECILES","IMD_2019_QUINTILES")
             .dropDuplicates()
)

# COMMAND ----------

display(lsoa_final)

# COMMAND ----------

# ensure one row per persion (conflicts have been nulled and now one row each)
assert lsoa_final.count() == lsoa_closest_top_all.select("PERSON_ID").distinct().count()

# COMMAND ----------

save_table(df=lsoa_final, out_name=f'{proj}_lsoa_{cohort}', save_previous=True)

# COMMAND ----------

lsoa_final = spark.table(f'{dsa}.{proj}_lsoa_{cohort}')

# COMMAND ----------

# MAGIC %md ###4.3.6 Conflict full history

# COMMAND ----------

# MAGIC %md For those with a conflict as at the closest `REPORTING_PERIOD_END_DATE` to baseline (approx 525k individuals) we present the **full** LSOA history (at all `REPORTING_PERIOD_END_DATE`s) to browse. This will include the region and IMD for each LSOA.
# MAGIC
# MAGIC Notes:
# MAGIC It may be the case that there is no conflict as at the next closest to baseline `REPORTING_PERIOD_END_DATE` and this LSOA may be carried forward. Alternatively, there may be an LSOA that is used in the majority of a persons history and thus may be used as either the overall most likely LSOA or used as justification that a new LSOA has been introduced as at the closest `REPORTING_PERIOD_END_DATE` (initiated by a change of practice) and then this new LSOA is likely to be the most accurate.

# COMMAND ----------

# lsoa_conflicts_full_history = (
#   lsoa_closest_top_all
#   .filter(f.col("lsoa_conflict")==1)
#   .select("PERSON_ID")
#   .distinct()
#   .join(lsoa_selection_groups, on=["PERSON_ID"], how="left")
#   .join(lsoa_region, on=["LSOA"], how="left")
#   .withColumn('region',
#                f.when(f.col('LSOA_1') == 'W', 'Wales')
#               .when(f.col('LSOA_1') == 'S', 'Scotland')
#               .otherwise(f.col('region'))
#               )
#   .join(lsoa_imd, on=["LSOA"], how="left")
#   .orderBy("PERSON_ID","diff")
# )


# display(lsoa_conflicts_full_history)

# COMMAND ----------

# save_table(df=lsoa_conflicts_full_history, out_name=f'{proj}_gdppr_lsoa_rped_conflicts_full_{cohort}', save_previous=True)

# COMMAND ----------

# MAGIC %md # 5. LSOA comparison

# COMMAND ----------

# MAGIC %md Excluding those who have an LSOA conflict to be resolved (that is more than one LSOA at the same `REPORTING_PERIOD_END_DATE`), 99.2% of the population have **the same** LSOA as was reported using the first/last lsoa GDPPR derivation. That is, 0.8% have an updated LSOA using the staggered derivation approach - this will be people for whom a closer to study start date LSOA was found between the first and last batches of GDPPR.

# COMMAND ----------

# read in data that used first and last GDPPR
#spark.sql(f'REFRESH TABLE {dsa}.{proj}_lsoa_archive')

#lsoa_archive = spark.table(f'{dsa}.{proj}_lsoa_archive')

# COMMAND ----------

#lsoa_comparison = (
#  lsoa_closest_top_all
#  .filter(f.col("lsoa_conflict")==0) #remove those who have a conflict (only compare these with no conflict)
#  .select("PERSON_ID","LSOA")
#  .join((lsoa_archive.select("PERSON_ID",f.col("LSOA").alias("LSOA_archive"))),on=["PERSON_ID"],how="left")
#  .withColumn('conflict', f.when(f.col('LSOA') == f.col('LSOA_archive'), 0).otherwise(1))
#)

#display(lsoa_comparison)

# COMMAND ----------

#display(lsoa_comparison.filter(f.col("conflict")==1))

# COMMAND ----------

#tab(lsoa_comparison,"conflict");print()