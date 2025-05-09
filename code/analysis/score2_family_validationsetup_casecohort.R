##################################################################################################################################
#
# This script includes the setup to run the validation for the score2 family of models validation in a case cohort including:
#                    setting up working directory
#                    loading general functions
#                    packages to be loaded in the environment
#                    reading in data
#                    defining variables of interest
#
# Authors: Carmen Petitjean <>, Alexia Sampri <>
#                    
##################################################################################################################################

# # Set working directory
# setwd("/db-mnt/databricks/rstudio_collab/CCU004_03/")

# Load general functions
source("software/backup_Rprofile") # Installs packages within the CCU004_03 subfolder
source("general_functions.R") 

# Load dependencies
dependencies <- c("rlang", "dplyr", "Hmisc", "cmprsk", "tidyverse", "magrittr", "survival", "ggplot2", "cowplot", 
                  "grid", "gridExtra", "data.table", "tidyr")

for(pack in dependencies){
  load_install_package(pack)
  print(paste0(pack, " done"))
}

# Define variables of interest
models_of_interest <- c("score2", "score2OP", "score2diab")
sex_of_interest <- c("men", "women")
region <- 1 # we are validating in a low risk region
if(cohort == "c02"){
  subgroups_of_interest <- c("region_uk", "hxcovid", "hxcovid_hosp_primary", "hxcovid_hosp_any", "hxcovid_icu", "threevacc", "goodvacc", "ETHNIC_CAT")
}else{
  subgroups_of_interest <- c("region_uk", "ETHNIC_CAT")
}
                                

# Specify validation format
valform <- data.frame("score2" = "casecohort", "score2OP" = "wholecohort", "score2diab" = "wholecohort")

# Define path of interests
path_to_results <- paste0("Analysis/03.2_case_cohort_validation/output/two_year_fu/", cohort, "/")

# Load in input data
load_mostrecent(pathofinterest = paste0("Analysis/03.1_imputation/output/validation_mix/", cohort), prefix = paste0("imputation_complete_data_", cohort, "_"))

score2family_imputed <- pmm_comp; rm(pmm_comp) # Reassign the variables

# Define number of imputations
imp.n <- length(score2family_imputed$score2$men)

# Set sampling fraction
sampling_fraction <- list()
for(model.i in models_of_interest){
  for(sex.i in sex_of_interest){
    if("sampling_fraction" %in% colnames(score2family_imputed[[model.i]][[sex.i]][[1]])){
      sampling_fraction[[model.i]][[sex.i]] <- unique(score2family_imputed[[model.i]][[sex.i]][[1]]$sampling_fraction)
    }else{
      sampling_fraction[[model.i]][[sex.i]] <- 1
    }
  }
}


# Load data with missingness with variables of interest - this might benefit from being in the imputation script
load_mostrecent(pathofinterest = paste0("data/clean/", cohort, "/"), prefix = paste0("score2_family_list_", cohort, "_"))

# variables to add
if(cohort == "c02"){
  var_to_add <- c("fu_days", "n_hes_episodes", "diabetes_type2_age", "cvd_event", "comp_event", "diabetes_type2", "hxcovid_hosp_any", 
                  "hxcovid_icu", "threevacc", "num_vacc_before_baseline", "esc_agegrp", "age", "hxcovid_date", "shielding")
}else if(cohort == "c01"){
  var_to_add <- c("fu_days", "n_hes_episodes", "diabetes_type2_age", "cvd_event", "comp_event", "diabetes_type2", "esc_agegrp", "age")
}



# Merge variables with the imputed dataset and add weights for the case cohort analysis
for(model.i in models_of_interest){ 
  for(sex.i in sex_of_interest){
   
     # For each model, merge the additional variables with the first imputation by PERSON ID
    score2family_imputed[[model.i]][[sex.i]][[1]] <- dplyr::left_join(score2family_imputed[[model.i]][[sex.i]][[1]], 
                                                                      score2family[[model.i]][,c("PERSON_ID", var_to_add)],
                                                                      by = "PERSON_ID")
    # Add an indicator for any vaccine for c02
    if(cohort == "c02"){
      score2family_imputed[[model.i]][[sex.i]][[1]][,"vacc"] <- ifelse(score2family_imputed[[model.i]][[sex.i]][[1]][,"num_vacc_before_baseline"] == "0", 0, 1)
    }

    print("hello2")
    
    # While we are running this loop we will also estimate weights to include for the case-cohort analysis
    if(!("subcohort" %in% colnames(score2family_imputed[[model.i]][[sex.i]][[1]]))){
      score2family_imputed[[model.i]][[sex.i]][[1]][,"subcohort"] <- 1
      score2family_imputed[[model.i]][[sex.i]][[1]][,"sampling_fraction"] <- 1
      score2family_imputed[[model.i]][[sex.i]][[1]][,"weights"] <- 1
      print("hello3")
    }else{
      score2family_imputed[[model.i]][[sex.i]][[1]][,"weights"] <- ifelse(score2family_imputed[[model.i]][[sex.i]][[1]][,"subcohort"] == 1 & score2family_imputed[[model.i]][[sex.i]][[1]][,"event_indicator"] == 0, 
                                                                  1 / score2family_imputed[[model.i]][[sex.i]][[1]][,"sampling_fraction"], 
                                                                  1)
      print("hello4")
    }
    
    print("hello5")
    
    if(imp.n > 1){
      for(imp.i in 2:imp.n){ # For each imputation...
        
        # Check if the order of PERSONID is the same
        if(all(score2family_imputed[[model.i]][[sex.i]][[1]]$PERSON_ID == score2family_imputed[[model.i]][[sex.i]][[imp.i]]$PERSON_ID)){
          
          print("hello6")
          
          # if this is true then we can perform a simple cbind
          score2family_imputed[[model.i]][[sex.i]][[imp.i]][,c(var_to_add, "weights")] <- score2family_imputed[[model.i]][[sex.i]][[1]][,c(var_to_add, "weights", "vacc")]
          
          if(!("subcohort" %in% colnames(score2family_imputed[[model.i]][[sex.i]][[imp.i]]))){
            
            print("hello7")
            score2family_imputed[[model.i]][[sex.i]][[imp.i]][,"subcohort"] <- score2family_imputed[[model.i]][[sex.i]][[1]][,"subcohort"]
            print("hello8")
            score2family_imputed[[model.i]][[sex.i]][[imp.i]][,"sampling_fraction"] <- score2family_imputed[[model.i]][[sex.i]][[1]][,"sampling_fraction"]
          }
          
        }else{ # otherwise stop the code with an error
          stop("The order of PERSONIDs is not the same across the imputations")
        }
      }
    }
  }
}

# As imputation was performed on centered variable, we want to "uncenter" variable information

# Load the means used for centering
load_mostrecent(pathofinterest = paste0("Analysis/03.1_imputation/data/", cohort, "/centering_info/"), prefix = paste0("means_used_for_centering_", cohort, "_"))

# Define variables which need uncentering
uncenter_var <- c("bmi")
uncenter_var_diab <- NULL

for(model.i in models_of_interest){ 
  for(sex.i in sex_of_interest){
    for(imp.i in 1:imp.n){
      
      # Un-scale and Un-center each variable
      for(var.i in uncenter_var){
        score2family_imputed[[model.i]][[sex.i]][[imp.i]][,paste0(var.i, "_center")] <- as.numeric(score2family_imputed[[model.i]][[sex.i]][[imp.i]][,paste0(var.i, "_center_divbysd")]) *
          sd_used_for_scaling[[model.i]][[sex.i]][[var.i]]
        score2family_imputed[[model.i]][[sex.i]][[imp.i]][,var.i] <- as.numeric(score2family_imputed[[model.i]][[sex.i]][[imp.i]][,paste0(var.i, "_center")]) +
          means_used_for_centering[[model.i]][[sex.i]][[var.i]]
      }
      
      # Uncenter diabetes specific variables
      if(model.i == "score2diab"){
        for(var.i in uncenter_var_diab){
          score2family_imputed[[model.i]][[sex.i]][[imp.i]][,paste0(var.i, "_center")] <- as.numeric(score2family_imputed[[model.i]][[sex.i]][[imp.i]][,paste0(var.i, "_center_divbysd")]) *
            sd_used_for_scaling[[model.i]][[sex.i]][[var.i]]
          score2family_imputed[[model.i]][[sex.i]][[imp.i]][,var.i] <- as.numeric(score2family_imputed[[model.i]][[sex.i]][[imp.i]][,paste0(var.i, "_center")]) +
            means_used_for_centering[[model.i]][[sex.i]][[var.i]]
        }
      }
      
    }
  }
}


# We also require a table which indicates how many individuals of each one-year age groups there are in the whole cohorts
ind_counts <- list()
ind_counts_subgrp <- list()
for(model.i in models_of_interest){ 
  for(sex.i in sex_of_interest){
    
    if(sex.i == "women"){tmp <- score2family[[model.i]][which(score2family[[model.i]][,"sex"] == 1),]}
    if(sex.i == "men"){tmp <- score2family[[model.i]][which(score2family[[model.i]][,"sex"] == 0),]}
    # Cut the dataset in one-year age groups
    tmp$strata <- cut(tmp$age,breaks=seq(min(tmp$age),ceiling(max(tmp$age)),by=1),include.lowest=TRUE, right = FALSE)
    tmp$strata <-  gsub("\\]", ")", tmp[,"strata"])
    # Store how many individuals in each one year age groups in ind_counts
    ind_counts[[model.i]][[sex.i]] <- as.data.frame(table(tmp$strata))
    colnames(ind_counts[[model.i]][[sex.i]]) <- c("strata", "n_ind")
    
    tmp$vacc <- ifelse(tmp$num_vacc_before_baseline == "0", 0 , 1)
    
    # We also assess how many individuals are in each one-year age groups in each subgroups of interest and store these in ind_counts_subgrp
    for(subgroup.i in c(subgroups_of_interest, "vacc")){
      # Define unique categories for each subgroup of interest (eg. different ethnicity categories)
      unique_categories <- as.character(unique(tmp[,subgroup.i]))
      for(cat.i in unique_categories){
        # Temporary data only including individuals in the subgroup of interest
        tmp_subgrp <- tmp[which(tmp[,subgroup.i] == cat.i),]
        # Storing results
        ind_counts_subgrp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]] <- as.data.frame(table(tmp_subgrp$strata))
        colnames(ind_counts_subgrp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]]) <- c("strata", "n_ind")
        rm(tmp_subgrp) # Remove intermediate variables
      }
    }
    
  }
}


score2family <- score2family_imputed
rm(score2family_imputed)

