###########################################################################################################################################
# This script aims provide extra data wrangling and cleaning
# Authors: Carmen Petitjean & Alexia Sampri
###########################################################################################################################################

# Clear background data
rm(list = ls())

# Set working directory
setwd("/db-mnt/databricks/rstudio_collab/CCU004_03/")

source("software/backup_Rprofile") # Installs packages within the CCU004_03 subfolder
source("general_functions.R") # load general functions

###########################################################################################################################################
# 1. Install packages, load required libraries and load external functions
###########################################################################################################################################

packages <- c("R.utils", "data.table", "tidyverse", "ggplot2", "magrittr", "purrr")

for(pack in packages){load_install_package(pack)}


###########################################################################################################################################
# 2. Read in raw data
###########################################################################################################################################

# Important variables
cohort <- "c01"
cohort_start_date <- as.Date(ifelse(cohort == "c01", "2020-01-01", "2022-01-01"))

# Important paths
raw_data_path <- "/db-mnt/databricks/rstudio_collab/CCU004_03/data/raw/"
path_to_results <- paste0("/db-mnt/databricks/rstudio_collab/CCU004_03/data/clean/", cohort, "/")

# Read in data - using custom function which takes the most recent file
covariates <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004_03_out_covariates_markers_", cohort))
additional_covariates <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_cohort_covariates_", cohort))
lsoa <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_cohort_lsoa_", cohort))
outcomes <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_cohort_outcomes_", cohort))
outcomes.noncvd <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_cohort_outcomes_noncvddeath_", cohort))
hx_cvd <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_tmp_hx_nonfatal_", cohort))
chronic.conditions <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_cohort_chronic_conditions_", cohort))
diabetes_count <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_diabetes_type_count_", cohort))
qrisk <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_qrisk_", cohort))
# Only read in history of COVID variables if cohort is c02
if(cohort == "c02"){
  hx_covid <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_cur_hx_covid2_", cohort))
  hx_vacc <- read_mostrecent(pathofinterest = raw_data_path, prefix = paste0("ccu004-03_cohort_exposures_hx_vacc_", cohort))
}else{
  hx_covid <- NULL
  hx_vacc <- NULL
}

###########################################################################################################################################
# 3. Additional data wrangling and variable renaming
###########################################################################################################################################

# Remove unnecessary variables
covariates <- covariates[, c("PERSON_ID",
                             "cov_bmi_value","cov_bmi_date",  "cov_creat_value", "cov_creat_date", "cov_egfr_value", "cov_egfr_date",
                             "cov_hba1c_value", "cov_hba1c_date", "cov_hdl_value", "cov_hdl_date", "cov_sbp_value", "cov_sbp_date", 
                             "cov_tchol_value", "cov_tchol_date", "cov_weight_value", "cov_weight_date", "cov_height_value", "cov_height_date")]
additional_covariates <- additional_covariates[, c("PERSON_ID",  "cov_n_consultations_hes_episodes",
                                                   "cov_smoking_status", "cov_smoking_max_date", 
                                                   "cov_meds_bp_lowering_flag", "cov_meds_statin_flag", "cov_meds_metformin_flag","cov_shielding_shielding_flag",
                                                   "cov_shielding_shielding_date")]
lsoa <- lsoa[,c("PERSON_ID", "DOB", "DOD","SEX", "ETHNIC_CAT", "ETHNIC_DESC", "study_start_age", "fu_days", "fu_end_date", "IMD_2019_DECILES", "region")]
outcomes <- outcomes[,c("PERSON_ID", "name", "DATE", "code")]
outcomes.noncvd <- outcomes.noncvd[,c("PERSON_ID", "NONCVD_DEATH_DATE", "NONCVD_DEATH_CODE")]
hx_cvd <- hx_cvd[,c("PERSON_ID", "cov_hx_ischaemic_heart_disease_flag", "cov_hx_ischaemic_heart_disease_date", "cov_hx_nonfatal_angina_flag", "cov_hx_nonfatal_angina_date",
                    "cov_hx_nonfatal_myocardial_infarction_flag", "cov_hx_nonfatal_myocardial_infarction_date", "cov_hx_nonfatal_stroke_flag", "cov_hx_nonfatal_stroke_date",
                    "cov_hx_peripheral_artery_disease_flag", "cov_hx_peripheral_artery_disease_date", "cov_hx_tia_flag", "cov_hx_tia_date")]
chronic.conditions <- chronic.conditions[,c("PERSON_ID", 
                                            "cov_chron_diabetes_general_flag", "cov_chron_diabetes_general_date", "diabetes_general_age",
                                            "cov_chron_diabetes_mody_flag", "cov_chron_diabetes_mody_date",
                                            "cov_chron_diabetes_other_flag", "cov_chron_diabetes_other_date",
                                            "cov_chron_diabetes_type1_flag", "cov_chron_diabetes_type1_date",
                                            "cov_chron_diabetes_type2_flag", "cov_chron_diabetes_type2_date", "diabetes_type2_age",
                                            "cov_chron_hypertension_flag", "cov_chron_hypotension_flag",
                                            "cov_chron_obesity_flag", "cov_chron_underweight_flag")]
diabetes_count <- diabetes_count[,c("PERSON_ID", "name", "DATE", "diabetes_age_type_count")]
qrisk <- qrisk[,c("PERSON_ID", "QRISK_DATE", "QRISK_score", "QRISK_version")]
if(cohort == "c02"){
  hx_covid <- hx_covid[,c("PERSON_ID", "exp_hxcovid_date", "covid_phenotype")]
  hx_vacc <- hx_vacc[,c("PERSON_ID", "num_vaccinations")]
}

# Rename variables
covariates <- dplyr::rename(covariates, "bmi" = "cov_bmi_value", "bmi_date" = "cov_bmi_date", "creat" = "cov_creat_value", "creat_date" = "cov_creat_date",
                     "egfr" = "cov_egfr_value", "egfr_date" = "cov_egfr_date", "hba1c" = "cov_hba1c_value", "hba1c_date" = "cov_hba1c_date",
                     "hdl" = "cov_hdl_value", "hdl_date" = "cov_hdl_date", "sbp" = "cov_sbp_value", "sbp_date" = "cov_sbp_date",
                     "tchol" = "cov_tchol_value", "tchol_date" = "cov_tchol_date",
                     "weight_value" = "cov_weight_value", "weight_date" = "cov_weight_date", 
                      "height_value" = "cov_height_value", "height_date"= "cov_height_date")
additional_covariates <- dplyr::rename(additional_covariates, "n_hes_episodes" = "cov_n_consultations_hes_episodes",
                     "smoking" = "cov_smoking_status", "smoking_date"  = "cov_smoking_max_date",
                     "meds_bp_lowering" = "cov_meds_bp_lowering_flag", "meds_statin" = "cov_meds_statin_flag", "meds_metformin" = "cov_meds_metformin_flag", 
                     "shielding" = "cov_shielding_shielding_flag", "shielding_date" = "cov_shielding_shielding_date")
lsoa <- dplyr::rename(lsoa, "age" = "study_start_age", "sex" = "SEX", "region_uk" = "region")
outcomes <- dplyr::rename(outcomes, "outcome" = "name", "outcome_date" = "DATE", 
                   "outcome_code" = "code")
outcomes.noncvd <- dplyr::rename(outcomes.noncvd, "noncvd_death_date" = "NONCVD_DEATH_DATE", "noncvd_death_code" = "NONCVD_DEATH_CODE")
hx_cvd <- dplyr::rename(hx_cvd, "hx_ischaemic_heart_disease_flag"= "cov_hx_ischaemic_heart_disease_flag", "hx_ischaemic_heart_disease_date" = "cov_hx_ischaemic_heart_disease_date", 
                 "hx_nonfatal_angina_flag"="cov_hx_nonfatal_angina_flag", "hx_nonfatal_angina_date"="cov_hx_nonfatal_angina_date",
                 "hx_nonfatal_myocardial_infarction_flag"="cov_hx_nonfatal_myocardial_infarction_flag", "hx_nonfatal_myocardial_infarction_date"="cov_hx_nonfatal_myocardial_infarction_date", 
                 "hx_nonfatal_stroke_flag"="cov_hx_nonfatal_stroke_flag", "hx_nonfatal_stroke_date"="cov_hx_nonfatal_stroke_date",
                 "hx_peripheral_artery_disease_flag"="cov_hx_peripheral_artery_disease_flag", "hx_peripheral_artery_disease_date"="cov_hx_peripheral_artery_disease_date", 
                 "hx_tia_flag"="cov_hx_tia_flag", "hx_tia_date"="cov_hx_tia_date")
chronic.conditions <- dplyr::rename(chronic.conditions, "diabetes_type2" = "cov_chron_diabetes_type2_flag", "diabetes_type2_date" = "cov_chron_diabetes_type2_date", 
                                    "diabetes_general" = "cov_chron_diabetes_general_flag", "diabetes_general_date" = "cov_chron_diabetes_general_date", 
                                    "diabetes_mody" = "cov_chron_diabetes_mody_flag", "diabetes_mody_date" = "cov_chron_diabetes_mody_date",
                                    "diabetes_other" = "cov_chron_diabetes_other_flag", "diabetes_other_flag" = "cov_chron_diabetes_other_date",
                                    "diabetes_type1" = "cov_chron_diabetes_type1_flag", "diabetes_type1_date" = "cov_chron_diabetes_type1_date",
                                    "chron_hypertension" = "cov_chron_hypertension_flag", "chron_hypotension" = "cov_chron_hypotension_flag",
                                    "chron_obesity" = "cov_chron_obesity_flag", "chron_underweight" = "cov_chron_underweight_flag")
diabetes_count <- dplyr::rename(diabetes_count, "diabetes_type_name"="name", "diabetes_date" = "DATE", "diabetes_age" = "diabetes_age_type_count")
qrisk <- dplyr::rename(qrisk, "qrisk_date" = "QRISK_DATE", "qrisk_score" = "QRISK_score", "qrisk_version" = "QRISK_version")
if(cohort == "c02"){
  hx_covid <- dplyr::rename(hx_covid, "hxcovid_date" = "exp_hxcovid_date", "hxcovid_phenotype" = "covid_phenotype")
  hx_vacc <- dplyr::rename(hx_vacc, "num_vacc_before_baseline" = "num_vaccinations")
}

# Reshape outcomes as multiple outcomes are reported for certain individuals
# NOTE: some outcomes are on the same day (can be a mix of fatal and non-fatal, and non fatal events)
outcomes <- outcomes[order(outcomes$outcome),]
outcomes <- outcomes[order(outcomes$outcome_date),]
outcomes <- outcomes %>% dplyr::group_by(PERSON_ID) %>% 
  dplyr::mutate(event_number = dplyr::row_number()) 
outcome_list_tmp <- vector(mode = "list", length = 0)
for(i in unique(outcomes$event_number)){
  outcome_list_tmp[[i]] <- outcomes[which(outcomes$event_number == i),] %>% dplyr::select(-event_number)
  colnames(outcome_list_tmp[[i]])[-1] <- paste0(colnames(outcome_list_tmp[[i]]), "_", i)[-1]
}
outcomes <- as.data.frame(purrr::reduce(outcome_list_tmp, dplyr::left_join, by = "PERSON_ID"))
rm(outcome_list_tmp)
# Add check
if(any(outcomes[which(outcomes$outcome_date_1 > outcomes$outcome_date_2),])){stop("Check failed: outcome date 1 after outcome date 2")}
if(any(outcomes[which(outcomes$outcome_date_2 > outcomes$outcome_date_3),])){stop("Check failed: outcome date 2 after outcome date 3")}
if(any(outcomes[which(outcomes$outcome_date_1 > outcomes$outcome_date_3),])){stop("Check failed: outcome date 1 after outcome date 3")}

# Merge data as necessary
if(cohort == "c02"){
  dt <- lsoa %>% dplyr::left_join(covariates, by = "PERSON_ID") %>%
    dplyr::left_join(additional_covariates, by = "PERSON_ID")  %>%
    dplyr::left_join(outcomes, by = "PERSON_ID")  %>%
    dplyr::left_join(outcomes.noncvd, by = "PERSON_ID")  %>%
    dplyr::left_join(hx_cvd, by = "PERSON_ID")  %>%
    dplyr::left_join(chronic.conditions, by = "PERSON_ID")  %>%
    dplyr::left_join(diabetes_count, by = "PERSON_ID") %>%
    dplyr::left_join(hx_covid, by = "PERSON_ID")  %>%
    dplyr::left_join(hx_vacc, by = "PERSON_ID")  %>%
    dplyr::left_join(qrisk, by = "PERSON_ID")
  
  rm(lsoa, covariates, additional_covariates, outcomes, outcomes.noncvd, hx_cvd,
     chronic.conditions, hx_covid, hx_vacc, diabetes_count, qrisk)
  
}else if(cohort == "c01"){
  dt <- lsoa %>% dplyr::left_join(covariates, by = "PERSON_ID") %>%
    dplyr::left_join(additional_covariates, by = "PERSON_ID")  %>%
    dplyr::left_join(outcomes, by = "PERSON_ID")  %>%
    dplyr::left_join(outcomes.noncvd, by = "PERSON_ID")  %>%
    dplyr::left_join(hx_cvd, by = "PERSON_ID")  %>%
    dplyr::left_join(chronic.conditions, by = "PERSON_ID")  %>%
    dplyr::left_join(diabetes_count, by = "PERSON_ID") %>%
    dplyr::left_join(qrisk, by = "PERSON_ID")
  
  rm(lsoa, covariates, additional_covariates, outcomes, outcomes.noncvd, hx_cvd,
     chronic.conditions, diabetes_count, qrisk)
  
}

dt <- as.data.frame(dt)

###############################################################################################################################
# 3. Clean data and add derived variables
###############################################################################################################################

# Change the format of any columns with the word date in it to a date
dt <- dt %>%
  dplyr::mutate(across(contains("date"), ~ as.Date(.x)))

# Change sex variables so that male = 0 and female = 1 (currently male = 1 and female = 2)
dt[which(dt$sex == 1), "sex"] <- 0
dt[which(dt$sex == 2), "sex"] <- 1
dt$sex <- as.factor(dt$sex)

# Add a binary CVD_event indicator for the first logged outcome
dt[,"cvd_event"] <- 0
dt[which(!is.na(dt$outcome_1)),"cvd_event"] <- 1
# Add a binary operator for non-cvd deaths
dt[,"noncvd_death"] <- 0
dt[which(!is.na(dt$noncvd_death_date)),"noncvd_death"] <- 1

# Add an event indicator
dt$event_indicator <- 0
dt[which(dt$noncvd_death == 1),"event_indicator"] <- 2
dt[which(dt$cvd_event == 1),"event_indicator"] <- 1 # Some individuals have both CVD-event and non-CVD event, in that case prioritise CVD-event
dt$comp_event <- ifelse(dt$event_indicator ==2, 1, 0)
dt <- dt[- which(dt$outcome_date_1 > dt$noncvd_death_date & !is.na(dt$outcome_date_1) & !is.na(dt$noncvd_death_date)),] # Remove anyone who had a non-CVD death logged earlier than a CVD-event

# Change follow up time so that the person is censored after a CVD-event even if non-fatal
dt$fu_days_fatal <- dt$fu_days
# Checks
all(dt[which(dt$event_indicator == 0), "fu_days_fatal"] == max(dt$fu_days)) # check that people not experiencing an event are censored only at end of follow up
all(!is.na(dt[which(dt$event_indicator == 0 & dt$fu_days_fatal<max(dt$fu_days)), "DOD"])) # checks that all these individuals have a DOD (should be TRUE)
# remove people that died without a competing event or cvd event flag 
dt <- dt[- which(dt$event_indicator == 0 & dt$fu_days_fatal<max(dt$fu_days)), ]
# Adjust follow-up time for individuals not dying
dt[which(dt$event_indicator == 1), "fu_days"] <- difftime(dt[which(dt$event_indicator == 1), "outcome_date_1"], cohort_start_date, units = "days")
# Check that for individuals with competing events, fu_days was correctly calculated
tmp <- dt[which(dt$event_indicator == 2),]
tmp$fu_days_recalculated <- difftime(tmp$noncvd_death_date, cohort_start_date, units = "days")
if(all(tmp$fu_days == tmp$fu_days_recalculated)){print("Check passed: fu_days was correctly calculated for individuals with competing events")}else{
  stop("Check failed: fu_days not correct for competing events")}
rm(tmp)

#Add ESC age groups for all datasets
dt[which(dt[,"age"]  >= 40 & dt[,"age"] < 50),"esc_agegrp"] <- 1
dt[which(dt[,"age"]  >= 50 & dt[,"age"] < 70),"esc_agegrp"] <- 2
dt[which(dt[,"age"] >= 70),"esc_agegrp"] <- 3

#Add ten-year age group
dt[,"agegrp_ten"] <- data.frame(cut(dt$age,breaks=seq(min(dt$age), 110,by=10),include.lowest=TRUE, right = FALSE))


# Type diabetes ################################################################################################################################################################

# ## Multiple codelist strategy ################################################################################
# # For all diabetes columns, replace missingness with 0
# diab_col <- c("diabetes_general", "diabetes_type1", "diabetes_type2", "diabetes_other", "diabetes_mody")
# for(column in diab_col){dt[which(is.na(dt[,column])),column] <- 0}
# # Plot a venn diagram of the overlap between "diabetes_general", "diabetes_type1", "diabetes_type2"
# dt_diab <- dt # There is no venn diagram package so we have to make a count table
# dt_diab$diabgroup <- ifelse(dt$diabetes_general == 1 & dt$diabetes_type1 == 1 & dt$diabetes_type2 == 1, "all 3", NA)
# dt_diab[which(dt_diab$diabetes_general == 0 & dt_diab$diabetes_type1 == 0 & dt_diab$diabetes_type2 == 0), "diabgroup"] <- "none"
# dt_diab[which(dt_diab$diabetes_general == 1 & dt_diab$diabetes_type1 == 1 & dt_diab$diabetes_type2 == 0), "diabgroup"] <- "type1-general"
# dt_diab[which(dt_diab$diabetes_general == 1 & dt_diab$diabetes_type1 == 0 & dt_diab$diabetes_type2 == 1), "diabgroup"] <- "type2-general"
# dt_diab[which(dt_diab$diabetes_general == 0 & dt_diab$diabetes_type1 == 1 & dt_diab$diabetes_type2 == 1), "diabgroup"] <- "type1-type2"
# dt_diab[which(dt_diab$diabetes_general == 1 & dt_diab$diabetes_type1 == 0 & dt_diab$diabetes_type2 == 0), "diabgroup"] <- "general"
# dt_diab[which(dt_diab$diabetes_general == 0 & dt_diab$diabetes_type1 == 1 & dt_diab$diabetes_type2 == 0), "diabgroup"] <- "type1"
# dt_diab[which(dt_diab$diabetes_general == 0 & dt_diab$diabetes_type1 == 0 & dt_diab$diabetes_type2 == 1), "diabgroup"] <- "type2"
# diab_venn <- as.data.frame(table(dt_diab$diabgroup))
# diab_venn$Freq <- round(diab_venn$Freq/5) * 5
# fwrite(diab_venn, file = paste0("Results/04_variables_investigations/diabetes_code_venn_", Sys.Date(), ".csv"))
# # Check: is there anyone with both a type 1 and type 2 diagnosis
# dim(dt[dt$diabetes_type1 == 1 & dt$diabetes_type2 == 1,]) # These individuals will be removed
# # Use diabetes_general and diabetes_general_age to inform more type1 and type2 diabetes
# dt[which(dt$diabetes_general == 1 & dt$diabetes_general_age <30),"diabetes_type1"] <- 1
# dt[which(dt$diabetes_general == 1 & dt$diabetes_general_age >=30),"diabetes_type2"] <- 1
# # Make sure if the individual has both type 2 and general diabetes, earliest diabetes age is taken
# dt$diabetes_type2_age_backup <- dt$diabetes_type2_age
# dt[which(dt$diabetes_type2 == 1 & dt$diabetes_general == 1), "diabetes_type2_age"] <- pmin(dt[which(dt$diabetes_type2 == 1 & dt$diabetes_general == 1), "diabetes_type2_age_backup"],
#                                                                                            dt[which(dt$diabetes_type2 == 1 & dt$diabetes_general == 1), "diabetes_general_age"])
# #check if all diabetes_type2_age < diabetes_general_age
# all(dt[which(dt$diabetes_type2 == 1 & dt$diabetes_general == 1),"diabetes_type2_age"] <= dt[which(dt$diabetes_type2 == 1 & dt$diabetes_general == 1),"diabetes_general_age"]) # Should be TRUE

# Count codes strategy ################################################################################

# # Conserve old diabetes type from previous strategy
# dt$old_diabetes_type1 <- dt$diabetes_type1
# dt$old_diabetes_type2 <- ifelse(dt$diabetes_type2 == 1 & dt$diabetes_type1 == 0, 1, 0)

# Replace missing diabetes type name with 0
dt[which(is.na(dt$diabetes_type_name)), "diabetes_type_name"] <- "0"

# Assign diabetes type
dt$diabetes_type2 <- ifelse(dt$diabetes_type_name == "diabetes_type2", 1, 0)
dt$diabetes_type1 <- ifelse(dt$diabetes_type_name == "diabetes_type1", 1, 0)
dt$diabetes_general <- ifelse(dt$diabetes_type_name == "diabetes_general", 1, 0)
dt$diabetes_other <- ifelse(dt$diabetes_type_name == "diabetes_other", 1, 0)
dt$diabetes_mody <- ifelse(dt$diabetes_type_name == "diabetes_mody", 1, 0)
# Use diabetes_general and diabetes_general_age to inform more type1 and type2 diabetes
dt[which(dt$diabetes_general == 1 & dt$diabetes_age <30),"diabetes_type1"] <- 1
dt[which(dt$diabetes_general == 1 & dt$diabetes_age >=30),"diabetes_type2"] <- 1
# Age at diabetes for type 2
dt[which(dt$diabetes_type2 ==1), "diabetes_type2_age"] <- dt[which(dt$diabetes_type2 ==1), "diabetes_age"]

# Complete BMI with information from height and weight
dt$bmi_added <- dt$weight_value / (dt$height_value / 100)^2 # Height is in cm
# Conserve old BMI
dt$bmi_direct <- dt$bmi
dt[which(is.na(dt$bmi)), "bmi"] <- dt[which(is.na(dt$bmi)), "bmi_added"]

# Assumption that missing medication means no medication
dt[is.na(dt$meds_bp_lowering),"meds_bp_lowering"] <- 0
dt[is.na(dt$meds_statin),"meds_statin"] <- 0
dt[is.na(dt$meds_metformin),"meds_metformin"] <- 0

# Assumption that no diagnosis in a chronic condition means no chronic condition
dt[is.na(dt$chron_hypertension),"chron_hypertension"] <- 0
dt[is.na(dt$chron_hypotension),"chron_hypotension"] <- 0
dt[is.na(dt$chron_obesity),"chron_obesity"] <- 0
dt[is.na(dt$chron_underweight),"chron_underweight"] <- 0

# Cap number of hospital visits 
dt[which(dt$n_hes_episodes >260), "n_hes_episodes"] <- 260 # cap number of hospital episodes in the last 5y to 260

# Adjust smoking categories
dt[dt$smoking == "","smoking"] <- NA #fix smoking missing
dt[which(dt$smoking == "Never" & !is.na(dt$smoking)),"smoking"] <- 0 #fix smoking 
dt[which(dt$smoking == "Ex" & !is.na(dt$smoking)),"smoking"] <- 0 #fix smoking 
dt[which(dt$smoking == "Current" & !is.na(dt$smoking)),"smoking"] <- 1 #fix smoking 
# # Limit the smoking lookback period
# dt[which(dt$smoking_date < "2020-07-01"), c("smoking", "smoking_date")] <- NA

# Adjust shielding category
# If shielding is missing, set to 0
dt[is.na(dt$shielding),"shielding"] <- 0

if(cohort == "c02"){
  # History of COVID variables
  # Have three tiers of COVID history (diagnosis, hospital and ICU)
  dt$hxcovid <- ifelse(is.na(dt$hxcovid_phenotype), 0, 1)
  dt$hxcovid_hosp_primary <- ifelse((dt$hxcovid_phenotype == "02_Covid_admission_primary_position" | dt$hxcovid_phenotype == "03_ICU_admission") & !is.na(dt$hxcovid_phenotype), 1, 0)
  dt$hxcovid_hosp_any <- ifelse((dt$hxcovid_phenotype == "02_Covid_admission_primary_position" | dt$hxcovid_phenotype == "02_Covid_admission_any_position" | dt$hxcovid_phenotype == "03_ICU_admission") & !is.na(dt$hxcovid_phenotype), 1, 0)
  dt$hxcovid_icu <- ifelse(dt$hxcovid_phenotype == "03_ICU_admission" & !is.na(dt$hxcovid_phenotype), 1, 0)
  # Make a categorical tier for hxcovid
  dt$hxcovid_severity <- as.factor(ifelse(dt$hxcovid_icu == 1 , 3, 
                                          ifelse(dt$hxcovid_hosp_primary == 1, 2,
                                                 ifelse(dt$hxcovid == 1, 1, 0))))
  
  # If vaccine variable is missing set to 0
  dt[which(is.na(dt$num_vacc_before_baseline)),"num_vacc_before_baseline"] <- 0
  # Add a binary vaccine variable
  dt$threevacc <- ifelse(dt$num_vacc_before_baseline >= 3, 1, 0)
  dt$twovacc <- ifelse(dt$num_vacc_before_baseline >= 2, 1, 0)
  # Define undervaccination (<2 for under 50, and <3 for 50 and over)
  dt$goodvacc <- ifelse(dt$agegrp_ten == "[40,50)" & dt$num_vacc_before_baseline >=2, 1, ifelse(dt$agegrp_ten != "[40,50)" & dt$num_vacc_before_baseline >=3, 1, 0))
  # Change number of vaccines variable so that any vaccines above 3 is marked as ">3"
  dt[which(dt$num_vacc_before_baseline >3),"num_vacc_before_baseline"] <- ">3"
}

# Fix Ethnicity category
dt[which(dt$ETHNIC_CAT == ""),"ETHNIC_CAT"] <- "Unknown"  # Missing will be set as unknown
# Separate the Asian category to "south Asian" and "other Asian"
dt[which(dt$ETHNIC_DESC == "Bangladeshi" | dt$ETHNIC_DESC == "Indian" |
           dt$ETHNIC_DESC == "Pakistani"),"ETHNIC_CAT"] <- "Bangladeshi, Indian or Pakistani"
dt[which(dt$ETHNIC_DESC == "Any other Asian background" | dt$ETHNIC_DESC == "Chinese"),"ETHNIC_CAT"] <- "Any other Asian background"
# # Move the two "Chinese" categories to "Asian or Asian British" from the "Other" category - no longer needed - fixed in databricks
# dt[which(dt$ETHNIC_DESC == "Chinese" | dt$ETHNIC_DESC == "Chinese (other ethnic group)"),"ETHNIC_CAT"] <- "Asian or Asian British"

# Fix history of CVD
hx_cvd_flags <- c("hx_ischaemic_heart_disease_flag", "hx_nonfatal_angina_flag", "hx_nonfatal_myocardial_infarction_flag", 
                  "hx_nonfatal_stroke_flag", "hx_peripheral_artery_disease_flag", "hx_tia_flag")
for(i in hx_cvd_flags){dt[which(is.na(dt[,i])), i] <- 0} # If flag is missing, set flag to 0
dt$hx_cvd <- ifelse(dt$hx_ischaemic_heart_disease_flag == 0 & dt$hx_nonfatal_angina_flag == 0 & dt$hx_nonfatal_myocardial_infarction_flag == 0 &
                      dt$hx_nonfatal_stroke_flag == 0 & dt$hx_peripheral_artery_disease_flag == 0 & dt$hx_tia_flag == 0, 0, 1)

# Convert HbA1c from percentage to other unit when <15
dt[which(dt$hba1c < 15),"hba1c"] <- (dt[which(dt$hba1c < 15),"hba1c"] - 2.15)/0.0915

# Calculated egfr
# Serum creatinine should be in mg/dL
dt$creat_mgdl <- dt$creat / 88.4 #converts umol/L to mg/dL

# Using the 2009 CKD-Epi equation
dt$egfr_ckdepi <- ifelse(dt$sex ==1,
                         #women
                          141.28 * (ifelse(dt$creat_mgdl/0.7 >1, 1, dt$creat_mgdl/0.7))^(-0.329) *
                           (ifelse(dt$creat_mgdl/0.7 <1, 1, dt$creat_mgdl/0.7))^(-1.209) * (0.992915^dt$age) * 1.018 *
                           (1.159 ^(ifelse(dt$ETHNIC_CAT == "Black or Black British", 1, 0))),
                         #men
                         141.28 * (ifelse(dt$creat_mgdl/0.9 >1, 1, dt$creat_mgdl/0.9))^(-0.411) *
                           (ifelse(dt$creat_mgdl/0.9 <1, 1, dt$creat_mgdl/0.9))^(-1.209) * (0.992915^dt$age) *
                           (1.159 ^(ifelse(dt$ETHNIC_CAT == "Black or Black British", 1, 0))))

dt[which(dt$egfr_ckdepi > 175),"egfr_ckdepi"] <- NA
#Make egfr combined variable which uses both egfr and egfr-ckd-epi prioritising egfr
dt$egfr_combined <- dt$egfr
dt[which(is.na(dt$egfr_combined)),"egfr_combined"] <- dt[which(is.na(dt$egfr_combined)),"egfr_ckdepi"]

# Define acceptable ranges
dt[which(dt$hba1c< 10 | dt$hba1c> 195) ,"hba1c"] <- NA
dt[which(dt$bmi< 0 | dt$bmi> 100) ,"bmi"] <- NA

# Add any other relevant variables which need to be derived and remove any irrelevant variable to save memory!
dt <- dt[, !(names(dt) %in% c("weight_value", "weight_date", "height_value", "height_date",
             "outcome_1", "outcome_date_1", "outcome_code_1", "outcome_2", "outcome_date_2", "outcome_code_2",
             "outcome_3", "outcome_date_3", "outcome_code_3", "chron_hypertension", "chron_hypotension",
             "chron_obesity", "chron_underweight", 
             "hx_ischaemic_heart_disease_flag", "hx_ischaemic_heart_disease_date",
             "hx_nonfatal_angina_flag", "hx_nonfatal_angina_date",
             "hx_nonfatal_myocardial_infarction_flag", "hx_nonfatal_myocardial_infarction_date",
             "hx_nonfatal_stroke_flag", "hx_nonfatal_stroke_date", 
             "hx_peripheral_artery_disease_flag", "hx_peripheral_artery_disease_date",
             "hx_tia_flag", "hx_tia_date"))]


###############################################################################################################################
# 4. Remove excluded individuals
###############################################################################################################################

# Individuals with missing regions # Should missing IMD be removed?
dt <- dt[-which(dt$region_uk == ""),] 

# Remove individuals with type 1 diabetes
dt <- dt[-which(dt$diabetes_type1 == 1),] 

# Remove individuals with MODYs
dt <- dt[-which(dt$diabetes_mody == 1),]

dt_with_hxcvd <- dt #conserve a copy of individuals with a  history of CVD

# Remove individuals with a history of CVD
dt <- dt[which(dt$hx_cvd == 0),]

###############################################################################################################################
# 5. Save 
###############################################################################################################################

# Save relevant datasets in "Data/clean" + make sure to timestamp them like below
data.table::fwrite(dt_with_hxcvd, file = paste0(path_to_results, "cleandata_hxcvd_", cohort, "_",Sys.Date(),".csv"))
data.table::fwrite(dt, file = paste0(path_to_results, "cleandata_", cohort, "_",Sys.Date(),".csv"))

# Save SCORE specific datasets
score2 <- dt[which(dt$diabetes_type2 == 0 & dt$age >= 40 & dt$age < 70),]
score2OP <- dt[which(dt$diabetes_type2 == 0 & dt$age >= 70 & dt$age < 90),]
score2diab <- dt[which(dt$diabetes_type2 == 1 & dt$age >= 40 & dt$age < 90),]
score2family <- list("score2" = score2, "score2OP" = score2OP, "score2diab" = score2diab)
save(score2family, 
     file = paste0(path_to_results,"score2_family_list_", cohort, "_", Sys.Date(),".RData"))

###############################################################################################################################
# 6. Complete Case Analysis
###############################################################################################################################

dt <- as.data.frame(dt)
selected_columns <- c('sbp', 'tchol', 'hdl', 'smoking', 'sex', 'age')
selected_columns_diab <- c('sbp', 'tchol', 'hdl', 'smoking', 'sex', 'age', 'egfr_ckdepi', 'hba1c', 'diabetes_type2_age')
dt_cca <- dt[complete.cases(dt[, selected_columns]), ]
dt_cca_diab <- dt[complete.cases(dt[, selected_columns_diab]), ]

missing_values <- colSums(is.na(dt_cca[, c('sbp', 'tchol', 'hdl', 'smoking', 'sex', 'age')]))
missing_values_diab <- colSums(is.na(dt_cca_diab[, selected_columns_diab]))

# Check if there are still missing values
if (any(missing_values > 0) | any(missing_values_diab > 0)) {
  stop("There are still missing values in the specified columns.")
  print(missing_values)
} else {
  print("No missing values in the specified columns.")
}

data.table::fwrite(dt_cca, file = paste0(path_to_results, "cleandata_cca_", cohort, "_", Sys.Date(),".csv"))
data.table::fwrite(dt_cca_diab, file = paste0(path_to_results,"cleandata_cca_diab_", cohort, "_", Sys.Date(),".csv"))

# List format
score2_cca <- dt_cca[which(dt_cca$diabetes_type2 == 0 & dt_cca$age >= 40 & dt_cca$age < 70),]
score2OP_cca <- dt_cca[which(dt_cca$diabetes_type2 == 0 & dt_cca$age >= 70 & dt_cca$age < 90),]
score2diab_cca <- dt_cca_diab[which(dt_cca_diab$diabetes_type2 == 1 & dt_cca_diab$age >= 40 & dt_cca_diab$age < 90),]
score2family_cca <- list("score2" = score2_cca, "score2OP" = score2OP_cca, "score2diab" = score2diab_cca)
save(score2family_cca, 
     file = paste0(path_to_results, "score2_family_list_cca_", cohort, "_", Sys.Date(),".RData"))
