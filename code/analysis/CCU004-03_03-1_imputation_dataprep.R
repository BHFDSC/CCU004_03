###########################################################################################################################################################################
# 
# This script prepares the data for imputation, as part of the SCORE2 validation
# Imputation will be carried out in a case-cohort design for SCORE2, and in whole cohorts for SCORE2-OP and SCORE2-Diabetes
# Author: Carmen Petitjean
#
###########################################################################################################################################################################

# setwd("/db-mnt/databricks/rstudio_collab/CCU004_03/")
source("software/backup_Rprofile") # Installs packages within the CCU004_03 subfolder
source("general_functions.R") # Load general functions

###########################################################################################################################################
# 1. Install packages, load required libraries and load external functions
###########################################################################################################################################

required_packages <- c("mice", "purrr", "rlang", "data.table", 'mitml')
for(pack in required_packages){load_install_package(pack)}

source("Analysis/03.1_imputation/imputation_functions.R")

###########################################################################################################################################
# 2. Read in clean data
###########################################################################################################################################

# Specify cohort
cohort <- "c02"

# Read in datasets 
load_mostrecent(pathofinterest = paste0("data/clean/", cohort), prefix = paste0("score2_family_list_", cohort))

# Variables of interest
models_of_interest <- c("score2", "score2OP", "score2diab")
sex_of_interest <- c("men", "women")
path_to_results <- paste0("Analysis/03.1_imputation/data/", cohort, "/")

# Divide the list in a sex-specific manner
data <- list()
for(models.i in models_of_interest){
  data[[models.i]] <- split(score2family[[models.i]], score2family[[models.i]]$sex)
  names(data[[models.i]])[names(data[[models.i]]) == "0"] <- "men"
  names(data[[models.i]])[names(data[[models.i]]) == "1"] <- "women"
}

means_used_for_centering <- list() # Empty list to store means used for centering. This will be useful for "uncentering"
sd_used_for_scaling <- list() # Empty list to store means used for for un-scaling
score2family_prepared <- list() # Empty list to store the data prepared for imputation
score2family_casecohort <- list() # Empty list to store the data prepared for imputation in case-cohorts

rm(score2family)

###########################################################################################################################################
# 3. Quality assurance and data cleaning - in the main pipeline this should be in a script of it's own
###########################################################################################################################################


###########################################################################################################################################
# 4. Preparing subsets
###########################################################################################################################################

for(model.i in models_of_interest){
  for(sex.i in sex_of_interest){
    
    data_tmp <- data[[model.i]][[sex.i]]
    
    if(cohort == "c02"){
      # Only include variables on interest
      data_tmp <- data_tmp[,c("PERSON_ID", "age", "sex" , "n_hes_episodes",
                              "smoking", "bmi", "hba1c", "hdl", "sbp", "egfr_ckdepi", "diabetes_type2_age",
                              "tchol", "diabetes_type2", "fu_days", "cvd_event", "comp_event", "event_indicator",
                              "meds_statin", "meds_bp_lowering", "ETHNIC_CAT", "IMD_2019_DECILES", "goodvacc", 
                              "hxcovid", "hxcovid_hosp_primary", "region_uk")]
    }else if(cohort == "c01"){
      # Only include variables on interest
      data_tmp <- data_tmp[,c("PERSON_ID", "age", "sex" , "n_hes_episodes",
                              "smoking", "bmi", "hba1c", "hdl", "sbp", "egfr_ckdepi", "diabetes_type2_age",
                              "tchol", "diabetes_type2", "fu_days", "cvd_event", "comp_event", "event_indicator",
                              "meds_statin", "meds_bp_lowering", "ETHNIC_CAT", "IMD_2019_DECILES", "region_uk")]
    }

    
    # Make an any event indicator
    data_tmp$any_event <- 0
    data_tmp[which(data_tmp$event_indicator != 0),"any_event"] <- 1
    
    # Estimate Nelson-Aalen estimates
    data_tmp[, "na_anyevent_wholecohort"] <- nelsonaalen(data_tmp, "fu_days", "any_event")
    data_tmp[, "na_anyevent_wholecohort_center"] <- data_tmp[, "na_anyevent_wholecohort"] - mean(data_tmp[, "na_anyevent_wholecohort"])
    data_tmp[, "na_anyevent_center_divbysd"] <- data_tmp[, "na_anyevent_wholecohort_center"] / sd(data_tmp[, "na_anyevent_wholecohort_center"])
    
    # Prepping variables as they are prepared for SCORE2 calculations
    # This include variable centering, and interactions calculated from centered variables
    
    data_tmp$smoking <- as.numeric(data_tmp$smoking)
    
    if(model.i == "score2" | model.i == "score2diab"){
      #Center variables and scale as appropriate
      #Centered variable will be named c + variable (e.g. age becomes cage)
      data_tmp$cage           <- (data_tmp$age - 60)/5
      data_tmp$csbp            <- (data_tmp$sbp - 120)/20
      data_tmp$ctchol          <- (data_tmp$tchol - 6)/1
      data_tmp$chdl            <- (data_tmp$hdl - 1.3)/0.5
    }
    
    if(model.i == "score2diab"){
      data_tmp$cagediab        <- (data_tmp$diabetes_type2_age - 50)/5
      data_tmp$chba1c         <- (data_tmp$hba1c - 31)/9.34
      data_tmp$clnegfr         <- (log(data_tmp$egfr_ckdepi) - 4.5)/0.15   # Class log egfr
      # quadratic term for egfr
      data_tmp$clnegfr.clnegfr    <- data_tmp$clnegfr * data_tmp$clnegfr
    }
    
    if(model.i == "score2OP"){
      data_tmp$cage <- (data_tmp$age - 73)
      data_tmp$csbp <- (data_tmp$sbp - 150)
      data_tmp$ctchol <- data_tmp$tchol - 6
      data_tmp$chdl <- (data_tmp$hdl - 1.4)
    }
    
    #Add interaction variables
    #interaction will be named variable1.variable2
    data_tmp$smoking.cage     <- data_tmp$smoking * data_tmp$cage
    data_tmp$csbp.cage         <- data_tmp$csbp * data_tmp$cage
    data_tmp$ctchol.cage     <- data_tmp$ctchol * data_tmp$cage
    data_tmp$chdl.cage         <- data_tmp$chdl * data_tmp$cage
    
    if(model.i == "score2diab"){
      data_tmp$chba1c.cage  <- data_tmp$chba1c * data_tmp$cage
      data_tmp$clnegfr.cage      <- data_tmp$clnegfr * data_tmp$cage
      data_tmp$cagediab.cage      <- data_tmp$cagediab * data_tmp$cage
    }
    
    # Interactions between NA and age
    data_tmp$na_anyevent_center_divbysd.cage <- data_tmp$cage * data_tmp$na_anyevent_center_divbysd
    # Interactions between age and event indicators
    data_tmp$cage.cvd_event <- data_tmp$cage * data_tmp$cvd_event
    data_tmp$cage.comp_event <- data_tmp$cage * data_tmp$comp_event
    
    # Additional variables which need to be centered
    var_to_center <- c("n_hes_episodes", "bmi")
    # Center and scale variables which need to be centered and scaled
    for(var.i in var_to_center){
      means_used_for_centering[[model.i]][[sex.i]][[var.i]] <- mean(data_tmp[,var.i], na.rm = TRUE)
      data_tmp[, paste0(var.i, "_center")] <- (data_tmp[, var.i] - means_used_for_centering[[model.i]][[sex.i]][[var.i]])
      sd_used_for_scaling[[model.i]][[sex.i]][[var.i]] <- sd(data_tmp[, paste0(var.i, "_center")], na.rm = TRUE)
      data_tmp[, paste0(var.i, "_center_divbysd")] <- data_tmp[, paste0(var.i, "_center")]  / sd_used_for_scaling[[model.i]][[sex.i]][[var.i]]
    }
    
    # Drop columns
    data_tmp <- subset(data_tmp, select = - c(age, sbp, tchol, hdl, n_hes_episodes, bmi, hba1c, egfr_ckdepi, diabetes_type2_age,
                                              diabetes_type2, na_anyevent_wholecohort_center, any_event, na_anyevent_wholecohort, bmi_center, 
                                              n_hes_episodes_center))
    
  
    
    # Re-assign the dataset to the prepared list
    score2family_prepared[[model.i]][[sex.i]] <- data_tmp
  }
}

# Save means used for centering
save(means_used_for_centering, sd_used_for_scaling,
     file = paste0(path_to_results, "centering_info/means_used_for_centering_", cohort, "_", Sys.Date(), ".RData"))


# ###########################################################################################################################################
# # 4. Export imputation files - traditional case cohorts where we take a random x% subcohort + all events not taken
# ###########################################################################################################################################
# 
# set.seed(1234)
# 
# split_numbers <- c(0.01, 0.05, 0.15, 0.25)
# 
# for(splits in split_numbers){
#   
#   split_character <- gsub("\\.", "_", as.character(splits))
#   
#   for(model.i in models_of_interest){
#     for(sex.i in sex_of_interest){
#       
#       score2family_casecohort[[model.i]][[sex.i]] <- make_case_cohort(dataset = score2family_prepared[[model.i]][[sex.i]],
#                                                                       relative_sample_size_total = splits,
#                                                                       event_column = "event_indicator")
#       
#     }
#   }
#   
#   save(score2family_casecohort, 
#        file = paste0(path_to_results, "imputation_subsets_", split_character, "_", Sys.Date(), "_score2prep.RData"))
#   
# }
# 
# rm(score2family_casecohort); score2family_casecohort <- list() # Reset the score2family_casecohort variable

###########################################################################################################################################
# 5. Alternative approach to define a case cohort
###########################################################################################################################################

# 1. Estimate if there are more CVD event or competing event for each score
# 2. Take a random sub-cohort the size of 20 times more than the biggest number of event
# 3. Add all events not taken by the random subcohort

set.seed(1234)

relative_number <- c(20)

for(relative_split in relative_number){
  
  split_character <- gsub("\\.", "_", as.character(relative_split))
  
  for(model.i in c("score2", "score2diab")){ # for score2OP, there are too many events compared to the cohort to do a subcohort the size of 20*number of events
    for(sex.i in sex_of_interest){
      
      score2family_casecohort[[model.i]][[sex.i]] <- make_case_cohort(dataset = score2family_prepared[[model.i]][[sex.i]],
                                                                      relative_sample_size_event = relative_split,
                                                                      event_column = "event_indicator")
      
    }
  }
  
  save(score2family_casecohort,
       file = paste0(path_to_results, "imputation_subsets_relativebyevent_", split_character, "_", cohort, "_", Sys.Date(), "_score2prep.RData"))
  
}


###########################################################################################################################################
# 6. For the analytical pipeline, we might want to use the whole cohort for SCORE2-OP and SCORE2-Diabetes, so prepare a list version with 
#         case-cohort for SCORE2, and whole cohort for SCORE2-OP and SCORE2-Diabetes
###########################################################################################################################################

score2family_validation <- list("score2" = score2family_casecohort[["score2"]], # BE CAREFUL AS IT WILL TAKE THE LATEST RELATIVE SPLIT DEFINED ABOVE
                                "score2OP" = score2family_prepared[["score2OP"]],
                                "score2diab" = score2family_prepared[["score2diab"]])

save(score2family_validation, 
     file = paste0(path_to_results, "imputation_subsets_forvalidation_", cohort, "_", Sys.Date(), ".RData"))

