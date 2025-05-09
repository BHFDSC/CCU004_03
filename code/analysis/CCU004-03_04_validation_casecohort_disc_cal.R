##################################################################################################################################
#
# This script runs the validation for the score2 family of models validation including assessing:
#                    discrimination
#                    calibration
#
# Authors: Carmen Petitjean <>, Alexia Sampri <>
#                    
##################################################################################################################################

# Clean the environment
rm(list=ls())

# Specify cohort
cohort <- "c02"

# Run setup
source("Analysis/03.2_case_cohort_validation/score2_family_validationsetup_casecohort.R", local = TRUE)

# Run predicted risks
source("Analysis/03.2_case_cohort_validation/score2_family_predicted_risk.R", local = TRUE)

# Load validation functions
source("Analysis/02_cca_validation/score2_family_functions.R", local = TRUE)

# Save .RData object with predicted risks and LP
save(score2family,
     file = paste0(path_to_results, "score2familyobject_", cohort, "_", Sys.Date(), ".RData"))

#############################################################################################
# Discrimination 
# C-statistic
#############################################################################################

print('Prepare dataset for discrimination calculation')

# Prepping the file to save to calculate the C-index in STATA - this needs to be in a flat csv format
flat_colnames <- c("PERSON_ID", "score", "sex", "imp", "event_indicator", "cvd_event", "comp_event", "fu_days", "fu_compete", "age",
                   subgroups_of_interest, "risk", "subcohort", "sampling_fraction", "weights", "opp_risk", "cen_ind", "cindexage")
score2family_flat <- data.frame(matrix(nrow = 0, ncol = length(flat_colnames)))
colnames(score2family_flat) <- flat_colnames

for(model.i in models_of_interest){
  for(sex.i in sex_of_interest){
    for(imp.i in 1:imp.n){

      # Take subset of data relating to loop
      tmp <- score2family[[model.i]][[sex.i]][[imp.i]]

      # Add information on score and imputation number
      tmp$score <- model.i
      tmp$imp <- imp.i

      tmp$opp_risk <- 1 - tmp$risk
      tmp$cen_ind <- ifelse(tmp$cvd_event == 1, 0, 1)

      # Add age groups
      tmp$cindexage <- NA
      if(model.i == "score2"){
        tmp[which(tmp$age >= 40 & tmp$age <50),"cindexage"] <- "40-49"
        tmp[which(tmp$age >= 50 & tmp$age <60),"cindexage"] <- "50-59"
        tmp[which(tmp$age >= 60 & tmp$age <70),"cindexage"] <- "60-69"
      }
      if(model.i == "score2OP"){
        tmp[which(tmp$age >= 70 & tmp$age <80),"cindexage"] <- "70-79"
        tmp[which(tmp$age >= 80 & tmp$age <90),"cindexage"] <- "80-89"
      }
      if(model.i == "score2diab"){
        tmp[which(tmp$age >= 40 & tmp$age <50),"cindexage"] <- "40-49"
        tmp[which(tmp$age >= 50 & tmp$age <60),"cindexage"] <- "50-59"
        tmp[which(tmp$age >= 60 & tmp$age <70),"cindexage"] <- "60-69"
        tmp[which(tmp$age >= 70 & tmp$age <80),"cindexage"] <- "70-79"
        tmp[which(tmp$age >= 80 & tmp$age <90),"cindexage"] <- "80-89"
      }


      tmp <- tmp[,c(flat_colnames)] # Limit the columns you select to those pre-defined above

      score2family_flat <- rbind(score2family_flat, tmp) # Aggregate information you require in a dataframe

    }
  }
}

write.csv(score2family_flat, file = paste0(path_to_results, "predicted_risk_imputed_", cohort, "_", Sys.Date(), ".csv"), row.names = FALSE)
write.csv(score2family_flat[which(score2family_flat$score != "score2diab"),], file = paste0(path_to_results, "predicted_risk_imputed_score2score2OP_", cohort, "_", Sys.Date(), ".csv"), row.names = FALSE)
write.csv(score2family_flat[which(score2family_flat$score == "score2"),], file = paste0(path_to_results, "predicted_risk_imputed_score2_", cohort, "_", Sys.Date(), ".csv"), row.names = FALSE)
write.csv(score2family_flat[which(score2family_flat$score == "score2OP"),], file = paste0(path_to_results, "predicted_risk_imputed_score2OP_", cohort, "_", Sys.Date(), ".csv"), row.names = FALSE)
write.csv(score2family_flat[which(score2family_flat$score == "score2diab"),], file = paste0(path_to_results, "predicted_risk_imputed_score2diab_", cohort, "_", Sys.Date(), ".csv"), row.names = FALSE)
rm(score2family_flat, tmp)


#############################################################################################
#
# Calibration
#
#############################################################################################

# Load extrapolated 10-year observed risk
load_mostrecent(pathofinterest = paste0("Analysis/01_extrapolating_cvd_risks/two_year_fu/output/", cohort, "/"), prefix = paste0("extrapolated_10yobsrisk_wholecohort_", cohort, "_"))

agegroup_of_interest <- c("agegrp_one", "agegrp_five")

##############################################################################################################################
# Calibration Assessment with the case-cohort
##############################################################################################################################

print("Estimating calibration in the case-cohort")

# Calibration by age groups and by sex ##############################################################################################################################

# Calibration plots

cal <- list()

for(agegroup_of_interest.i in agegroup_of_interest){
  for(model.i in models_of_interest){
    for(sex.i in sex_of_interest){
      for(imp.i in 1:imp.n){

        data_tmp <- score2family[[model.i]][[sex.i]][[imp.i]]

        if(agegroup_of_interest.i == "agegrp_one") {# Add one-year age groups
          data_tmp[,"agegrp_one"] <- data.frame(cut(data_tmp$age,breaks=seq(min(data_tmp$age),ceiling(max(data_tmp$age)),by=1),include.lowest=TRUE, right = FALSE))
          data_tmp[,"agegrp_one"] <- gsub("\\]", ")", data_tmp[,"agegrp_one"])
        }
        if(agegroup_of_interest.i == "agegrp_five") {# Add one-year age groups
          data_tmp[,"agegrp_five"] <- data.frame(cut(data_tmp$age,breaks=seq(min(data_tmp$age),ceiling(max(data_tmp$age)),by=5),include.lowest=TRUE, right = FALSE))
          data_tmp[,"agegrp_five"] <- gsub("\\]", ")", data_tmp[,"agegrp_five"])
        }


        if(model.i == "score2" | model.i == "score2OP"){
          cal[[agegroup_of_interest.i]][[model.i]][[sex.i]][[imp.i]] <- cal_subgrp_case_cohort(dataset= data_tmp, risk="risk", n.groups=length(levels(data_tmp[,agegroup_of_interest.i])), score = model.i,
                                                                     groupvar=agegroup_of_interest.i, population= sex.i, extrapolated_observed_risk = lt[[agegroup_of_interest.i]][["no_diabetes"]][[sex.i]],
                                                                     which.sampling.fraction = sampling_fraction[[model.i]][[sex.i]])

        }

        if(model.i == "score2diab"){
          cal[[agegroup_of_interest.i]][[model.i]][[sex.i]][[imp.i]] <- cal_subgrp_case_cohort(dataset= data_tmp, risk="risk", n.groups=length(levels(data_tmp[,agegroup_of_interest.i])), score = model.i,
                                                                     groupvar=agegroup_of_interest.i, population= sex.i, extrapolated_observed_risk = lt[[agegroup_of_interest.i]][["diabetes"]][[sex.i]],
                                                                     which.sampling.fraction = sampling_fraction[[model.i]][[sex.i]])

        }
      }
    }
  }
}



# Save cal object
save(cal, file = paste0(path_to_results, "calibration_agegroup1_bysex_", cohort, "_", Sys.Date(),".RData"))

#Plot calibration
pdf(paste0(path_to_results, "calibration_plots_imputed_", cohort, "_", Sys.Date(), ".pdf"), width=13, height=7)
for(agegroup_of_interest.i in agegroup_of_interest){
  print(plot_calibration_by_imputation(cal.object = cal[[agegroup_of_interest.i]], predict.col = "weighted_predicted_risk", observed.col = "extra_10y_obs_cvd_risk", title_detail = NULL))
  print(plot_calibration_by_imputation_agex(cal.object = cal[[agegroup_of_interest.i]], title_detail = paste0("(", cohort, ")"),
                                            predict.col = "weighted_predicted_risk", observed.col = "extra_10y_obs_cvd_risk"))
}
dev.off()


# E/O ratios and calibration slopes

    # Expected - weighted mean of weighted predicted risks to account for case-cohort design and to account for proportions of different one year age groups
    # Observed - weighted mean of estimated 10-year observed risks to account for proportions of different one year age groups
    # To estimate the calibration slope conduct a linear regression on the predicted and observed

# Empty objects
cal_table <- list()
tmp <- list()
cal_quant <- NULL

for(model.i in models_of_interest){
  for(sex.i in sex_of_interest){

    # Initialise calibration quantification results table
    tmp[[model.i]][[sex.i]] <- as.data.frame(matrix(data = NA, nrow = imp.n, ncol = 0))
    tmp[[model.i]][[sex.i]][ ,"sex"] <- sex.i
    tmp[[model.i]][[sex.i]][ ,"score"] <- model.i

    for(imp.i in 1:imp.n){

      # Reduce to important calibration information to cal_table
      cal_table[[model.i]][[sex.i]][[imp.i]] <- cal[["agegrp_one"]][[model.i]][[sex.i]][[imp.i]][,c("strata", "weighted_predicted_risk", "extra_10y_obs_cvd_risk")]
      cal_table[[model.i]][[sex.i]][[imp.i]] <- left_join(cal_table[[model.i]][[sex.i]][[imp.i]], ind_counts[[model.i]][[sex.i]], by = "strata") # Add how many individuals are in each age group (these values should come from the whole cohort)

      # Transformation
      cal_table[[model.i]][[sex.i]][[imp.i]][,"transf_weighted_predicted_risk"] <- log(-log(1-cal_table[[model.i]][[sex.i]][[imp.i]][,"weighted_predicted_risk"]))
      cal_table[[model.i]][[sex.i]][[imp.i]][,"transf_extra_10y_obs_cvd_risk"] <- log(-log(1-cal_table[[model.i]][[sex.i]][[imp.i]][,"extra_10y_obs_cvd_risk"]))

      # Regression model to estimate the calibration slope # Transformed by log(-log(1-x)) for both obs and predicted - force intercept to 0 to get overall calibration
      regression <- lm(formula = transf_extra_10y_obs_cvd_risk  ~  0 + transf_weighted_predicted_risk, weights = n_ind, data = cal_table[[model.i]][[sex.i]][[imp.i]])

      # Store results
      tmp[[model.i]][[sex.i]][imp.i ,"imp"] <- imp.i #indicate which imputation the results are for
          # Calibration slope
      tmp[[model.i]][[sex.i]][imp.i ,"calslope"] <- regression$coefficients["transf_weighted_predicted_risk"]
      tmp[[model.i]][[sex.i]][imp.i ,"calslope_se"] <- summary(regression)$coefficients["transf_weighted_predicted_risk", "Std. Error"]
      tmp[[model.i]][[sex.i]][imp.i ,"calslope_low"] <- tmp[[model.i]][[sex.i]][imp.i ,"calslope"] - 1.96*tmp[[model.i]][[sex.i]][imp.i ,"calslope_se"]
      tmp[[model.i]][[sex.i]][imp.i ,"calslope_high"] <- tmp[[model.i]][[sex.i]][imp.i ,"calslope"] + 1.96*tmp[[model.i]][[sex.i]][imp.i ,"calslope_se"]
          # Estimate a total weighted predicted risk
      tmp[[model.i]][[sex.i]][imp.i ,"E"]  <- weighted.mean(cal_table[[model.i]][[sex.i]][[imp.i]][,"weighted_predicted_risk"], w = cal_table[[model.i]][[sex.i]][[imp.i]][,"n_ind"])
          # Estimate a total observed risk
      tmp[[model.i]][[sex.i]][imp.i ,"O"] <- weighted.mean(cal_table[[model.i]][[sex.i]][[imp.i]][,"extra_10y_obs_cvd_risk"], w = cal_table[[model.i]][[sex.i]][[imp.i]][,"n_ind"])
    }
    cal_quant <- rbind(cal_quant, tmp[[model.i]][[sex.i]])
  }
}
rm(tmp) # Remove temporary variable
cal_quant[,"EO"] <- cal_quant[,"E"] / cal_quant[,"O"] # Estimate expected-to-observed ratio
write.csv(cal_quant, file = paste0(path_to_results, "calibration_quantified_", cohort, "_", Sys.Date(), ".csv"))


# Calibration by subgroups ##############################################################################################################################

print("Estimating calibration in case-cohorts by subgroups")

# Load extrapolated 10-year observed risk for subgroups
load_mostrecent(pathofinterest = paste0("Analysis/01_extrapolating_cvd_risks/two_year_fu/output/", cohort, "/"), prefix = paste0("extrapolated_10yobsrisk_wholecohort_bysubgroup_", cohort, "_"))

# Define maximum risk for each subgroup, so that the risk-axis for each plots of each categories of each subgroups match
max_category <- list(
  'score2' = list('region_uk' = 12,
                  'hxcovid' = 10,
                  'hxcovid_hosp_primary' = 15,
                  'hxcovid_hosp_any' = 15,
                  'hxcovid_icu' = 20,
                  'threevacc' = 20,
                  'goodvacc' = 30,
                  'ETHNIC_CAT' = 15),
  'score2OP' = list('region_uk' = 35,
                    'hxcovid' = 35,
                    'hxcovid_hosp_primary' = 35,
                    'hxcovid_hosp_any' = 35,
                    'hxcovid_icu' = 40,
                    'threevacc' = 40,
                    'goodvacc' = 30,
                    'ETHNIC_CAT' = 35),
  'score2diab' = list('region_uk' = 35,
                      'hxcovid' = 35,
                      'hxcovid_hosp_primary' = 35,
                      'hxcovid_hosp_any' = 35,
                      'hxcovid_icu' = 40,
                      'threevacc' = 40,
                      'goodvacc' = 50,
                      'ETHNIC_CAT' = 35))

cal_subgrp <- list()

for(agegroup_of_interest.i in c("agegrp_one")){
  for(model.i in models_of_interest){
    for(sex.i in sex_of_interest){
      for(imp.i in 1:imp.n){
        for(subgroup.i in c(subgroups_of_interest, "vacc")){

          data_tmp <- score2family[[model.i]][[sex.i]][[imp.i]]

          if(subgroup.i == "vacc"){data_tmp$vacc <- ifelse(data_tmp$num_vacc_before_baseline == "0", 0, 1)}
          
          # Define unique categories for each subgroup of interest (eg. different ethnicity categories)
          unique_categories <- as.character(unique(data_tmp[,subgroup.i]))

          if(agegroup_of_interest.i == "agegrp_one") {# Add one-year age groups
            data_tmp[,"agegrp_one"] <- data.frame(cut(data_tmp$age,breaks=seq(min(data_tmp$age),ceiling(max(data_tmp$age)),by=1),include.lowest=TRUE, right = FALSE))
            data_tmp[,"agegrp_one"] <- gsub("\\]", ")", data_tmp[,"agegrp_one"])
          }

          # For each categories of the subgroup, assess the calibration
          for(cat.i in unique_categories){

            # Restrict data from individuals from that category
            data_tmp_2 <- data_tmp[which(data_tmp[,subgroup.i] == cat.i),]

            if(model.i == "score2" | model.i == "score2OP"){
              cal_subgrp[[agegroup_of_interest.i]][[model.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][[imp.i]] <- cal_subgrp_case_cohort(dataset= data_tmp_2, risk="risk", n.groups=length(levels(data_tmp_2[,agegroup_of_interest.i])), score = model.i,
                                                                                                          groupvar=agegroup_of_interest.i, population= sex.i, extrapolated_observed_risk = lt[[agegroup_of_interest.i]][["no_diabetes"]][[subgroup.i]][[as.character(cat.i)]][[sex.i]],
                                                                                                          which.sampling.fraction = sampling_fraction[[model.i]][[sex.i]])

            }

            if(model.i == "score2diab"){
              cal_subgrp[[agegroup_of_interest.i]][[model.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][[imp.i]] <- cal_subgrp_case_cohort(dataset= data_tmp_2, risk="risk", n.groups=length(levels(data_tmp_2[,agegroup_of_interest.i])), score = model.i,
                                                                                                          groupvar=agegroup_of_interest.i, population= sex.i, extrapolated_observed_risk = lt[[agegroup_of_interest.i]][["diabetes"]][[subgroup.i]][[as.character(cat.i)]][[sex.i]],
                                                                                                          which.sampling.fraction = sampling_fraction[[model.i]][[sex.i]])

            }

          }
        }
      }
    }
  }
}


# Save cal object
save(cal_subgrp, file = paste0(path_to_results, "calibration_agegroup1_bysubgroups_", cohort, "_", Sys.Date(),".RData"))


#Plot calibration by subgroup
pdf(paste0(path_to_results, "calibration_plots_by_subgroup_imputed_", cohort, "_", Sys.Date(), ".pdf"), width=10, height=8)

for(agegroup_of_interest.i in "agegrp_one"){
  for(subgroup.i in subgroups_of_interest){
    for(model.i in models_of_interest){
      for(imp.i in 1:imp.n){

        a <- plot_calibration_by_subgroup_agex_by_imputation(cal.object = cal_subgrp, title_detail =  paste0("(", cohort, ")"),
                                               predict.col = "weighted_predicted_risk", observed.col = "extra_10y_obs_cvd_risk",
                                               max_y_axis_object = max_category)

        print(a)
      }
    }
  }
}

dev.off()

# E/O ratios and calibration slopes

# Expected - weighted mean of weighted predicted risks to account for case-cohort design and to account for proportions of different one year age groups
# Observed - weighted mean of estimated 10-year observed risks to account for proportions of different one year age groups
# To estimate the calibration slope conduct a linear regression on the predicted and observed

# Empty objects
cal_table_subgrp <- list()
tmp <- list()
cal_quant_subgrp <- NULL

for(model.i in models_of_interest){
  for(sex.i in sex_of_interest){

    for(subgroup.i in c(subgroups_of_interest, "vacc")){

      # Define unique categories for each subgroup of interest (eg. different ethnicity categories)
      unique_categories <- as.character(names(cal_subgrp[["agegrp_one"]][[model.i]][[subgroup.i]]))

      # For each categories of the subgroup, assess the calibration quantitatively
      for(cat.i in unique_categories){

        # Initialise calibration quantification results table
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]] <- as.data.frame(matrix(data = NA, nrow = imp.n, ncol = 0))
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][ ,"sex"] <- sex.i
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][ ,"score"] <- model.i
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][ ,"subgroup"] <- subgroup.i
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][ ,"subgroup_cat"] <- cat.i

        for(imp.i in 1:imp.n){

          # Reduce to important calibration information to cal_table
          cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]] <- cal_subgrp[["agegrp_one"]][[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,c("strata", "weighted_predicted_risk", "extra_10y_obs_cvd_risk")]
          
          # Add how many individuals are in each age group (these values should come from the whole cohort)
          cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]] <- left_join(cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]], ind_counts_subgrp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]], by = "strata")

          # Transformation
          cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"transf_weighted_predicted_risk"] <- log(-log(1-cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"weighted_predicted_risk"]))
          cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"transf_extra_10y_obs_cvd_risk"] <- log(-log(1-cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"extra_10y_obs_cvd_risk"]))

          # Regression model to estimate the calibration slope # Transformed by log(-log(1-x)) for both obs and predicted - force intercept to 0 to get overall calibration
          regression <- lm(formula = transf_extra_10y_obs_cvd_risk  ~  0 + transf_weighted_predicted_risk, weights = n_ind, data = cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]])

          # Store results
          tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"imp"] <- imp.i #indicate which imputation the results are for
              # Calibration slope
          tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope"] <- regression$coefficients["transf_weighted_predicted_risk"]
          tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_se"] <- summary(regression)$coefficients["transf_weighted_predicted_risk", "Std. Error"]
          tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_low"] <- tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope"] - 1.96*tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_se"]
          tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_high"] <- tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope"] + 1.96*tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_se"]
              # Estimate a total weighted predicted risk
          tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"E"]  <- weighted.mean(cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"weighted_predicted_risk"], w = cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"n_ind"])
              # Estimate a total observed risk
          tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"O"] <- weighted.mean(cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"extra_10y_obs_cvd_risk"], w = cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"n_ind"])
        }
        cal_quant_subgrp <- rbind(cal_quant_subgrp, tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]])
      }
    }
  }
}
rm(tmp) # Remove temporary variable

# Estimate the E/O and the calibration slope by 10-year age groups
  # To achieve this, we add 10-year indicators in the calibration object
score2_agegrp <- list("40-49" = c("[40,41)", "[41,42)", "[42,43)", "[43,44)", "[44,45)", "[45,46)", "[46,47)", "[47,48)", "[48,49)", "[49,50)"),
                      "50-59" = c("[50,51)", "[51,52)", "[52,53)", "[53,54)", "[54,55)", "[55,56)", "[56,57)", "[57,58)", "[58,59)", "[59,60)"),
                      "60-69" = c("[60,61)", "[61,62)", "[62,63)", "[63,64)", "[64,65)", "[65,66)", "[66,67)", "[67,68)", "[68,69)", "[69,70)"))
score2OP_agegrp <- list("70-79" = c("[70,71)", "[71,72)", "[72,73)", "[73,74)", "[74,75)", "[75,76)", "[76,77)", "[77,78)", "[78,79)", "[79,80)"),
                        "80-89" = c("[80,81)", "[81,82)", "[82,83)", "[83,84)", "[84,85)", "[85,86)", "[86,87)", "[87,88)", "[88,89)", "[89,90)"))
score2diab_agegrp <- list("40-49" = c("[40,41)", "[41,42)", "[42,43)", "[43,44)", "[44,45)", "[45,46)", "[46,47)", "[47,48)", "[48,49)", "[49,50)"),
                          "50-59" = c("[50,51)", "[51,52)", "[52,53)", "[53,54)", "[54,55)", "[55,56)", "[56,57)", "[57,58)", "[58,59)", "[59,60)"),
                          "60-69" = c("[60,61)", "[61,62)", "[62,63)", "[63,64)", "[64,65)", "[65,66)", "[66,67)", "[67,68)", "[68,69)", "[69,70)"),
                          "70-79" = c("[70,71)", "[71,72)", "[72,73)", "[73,74)", "[74,75)", "[75,76)", "[76,77)", "[77,78)", "[78,79)", "[79,80)"),
                          "80-89" = c("[80,81)", "[81,82)", "[82,83)", "[83,84)", "[84,85)", "[85,86)", "[86,87)", "[87,88)", "[88,89)", "[89,90)"))
ten_year_agegroups <- list("score2" = score2_agegrp, "score2OP" = score2OP_agegrp, "score2diab" = score2diab_agegrp)

# Empty objects
cal_table_subgrp <- list()
tmp <- list()
for(model.i in models_of_interest){
  for(sex.i in sex_of_interest){

    for(cat.i in names(ten_year_agegroups[[model.i]])){

      # Initialise calibration quantification results table
      tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]] <- as.data.frame(matrix(data = NA, nrow = imp.n, ncol = 0))
      tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][ ,"sex"] <- sex.i
      tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][ ,"score"] <- model.i
      tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][ ,"subgroup"] <- "Age group"
      tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][ ,"subgroup_cat"] <- cat.i

      for(imp.i in 1:imp.n){

        # Reduce to important calibration information to cal_table
        cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]] <- cal[["agegrp_one"]][[model.i]][[sex.i]][[imp.i]][which(cal[["agegrp_one"]][[model.i]][[sex.i]][[imp.i]]$category %in% ten_year_agegroups[[model.i]][[cat.i]]),
                                                                                                                                                               c("strata", "weighted_predicted_risk", "extra_10y_obs_cvd_risk")]
        
        # Add how many individuals are in each age group (these values should come from the whole cohort)
        cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]] <- left_join(cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]], ind_counts[[model.i]][[sex.i]], by = "strata")

        # Transformation
        cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"transf_weighted_predicted_risk"] <- log(-log(1-cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"weighted_predicted_risk"]))
        cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"transf_extra_10y_obs_cvd_risk"] <- log(-log(1-cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"extra_10y_obs_cvd_risk"]))

        # Regression model to estimate the calibration slope # Transformed by log(-log(1-x)) for both obs and predicted - force intercept to 0 to get overall calibration
        regression <- lm(formula = transf_extra_10y_obs_cvd_risk  ~  0 + transf_weighted_predicted_risk, weights = n_ind, data = cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]])

        # Store results
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"imp"] <- imp.i #indicate which imputation the results are for
        # Calibration slope
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope"] <- regression$coefficients["transf_weighted_predicted_risk"]
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_se"] <- summary(regression)$coefficients["transf_weighted_predicted_risk", "Std. Error"]
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_low"] <- tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope"] - 1.96*tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_se"]
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_high"] <- tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope"] + 1.96*tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"calslope_se"]
        # Estimate a total weighted predicted risk
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"E"]  <- weighted.mean(cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"weighted_predicted_risk"], w = cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"n_ind"])
        # Estimate a total observed risk
        tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]][imp.i ,"O"] <- weighted.mean(cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"extra_10y_obs_cvd_risk"], w = cal_table_subgrp[[model.i]][[subgroup.i]][[cat.i]][[sex.i]][[imp.i]][,"n_ind"])
      }
      cal_quant_subgrp <- rbind(cal_quant_subgrp, tmp[[model.i]][[sex.i]][[subgroup.i]][[cat.i]])
    }
  }
}
cal_quant_subgrp[,"EO"] <- cal_quant_subgrp[,"E"] / cal_quant_subgrp[,"O"] # Estimate expected-to-observed ratio
write.csv(cal_quant_subgrp, file = paste0(path_to_results, "calibration_quantified_bysubgroup_", cohort, "_", Sys.Date(), ".csv"))

# ##############################################################################################################################
# # Calibration Assessment with only the sub-cohort ############################################################################
# ##############################################################################################################################
#
# # Calibration by 1-year age groups and by sex
#
# cal_subcohort <- list()
#
# for(agegroup_of_interest.i in agegroup_of_interest){
#   for(model.i in models_of_interest){
#     for(sex.i in sex_of_interest){
#       for(imp.i in 1:imp.n){
#
#         data_tmp <- score2family[[model.i]][[sex.i]][[imp.i]]
#
#         # Only select subcohort
#         data_tmp <- data_tmp[which(data_tmp$subcohort == 1),]
#
#         if(agegroup_of_interest.i == "agegrp_one") {# Add one-year age groups
#           data_tmp[,"agegrp_one"] <- data.frame(cut(data_tmp$age,breaks=seq(min(data_tmp$age),ceiling(max(data_tmp$age)),by=1),include.lowest=TRUE, right = FALSE))
#         }
#         if(agegroup_of_interest.i == "agegrp_five") {# Add one-year age groups
#           data_tmp[,"agegrp_five"] <- data.frame(cut(data_tmp$age,breaks=seq(min(data_tmp$age),ceiling(max(data_tmp$age)),by=5),include.lowest=TRUE, right = FALSE))
#         }
#
#         if(model.i == "score2" | model.i == "score2OP"){
#           cal_subcohort[[agegroup_of_interest.i]][[model.i]][[sex.i]][[imp.i]] <- cal_subgrp(dataset= data_tmp, risk="risk", n.groups=length(levels(data_tmp$agegrp_one)), score = model.i,
#                                                                    groupvar="agegrp_one", population= sex.i, extrapolated_observed_risk = lt[[agegroup_of_interest.i]][["no_diabetes"]][[sex.i]])
#
#         }
#
#         if(model.i == "score2diab"){
#           cal_subcohort[[agegroup_of_interest.i]][[model.i]][[sex.i]][[imp.i]] <- cal_subgrp(dataset= data_tmp, risk="risk", n.groups=length(levels(data_tmp$agegrp_one)), score = model.i,
#                                                                    groupvar="agegrp_one", population= sex.i, extrapolated_observed_risk = lt[[agegroup_of_interest.i]][["diabetes"]][[sex.i]])
#
#         }
#       }
#     }
#   }
# }
#
#
# # Save cal object
# save(cal_subcohort, file = paste0(path_to_results, "calibration_subcohort_agegroup1_bysex_", Sys.Date(),".RData"))


##############################################################################################################################
# Calibration Assessment with two-year predicted risks 
##############################################################################################################################

print("Estimating calibration with two-year predicted risks")

# In the whole cohort
print("Estimating calibration with two-year predicted risks in the whole cohort")

cal_two <- list()

for(model.i in models_of_interest){
  for(sex.i in sex_of_interest){
    for(imp.i in 1:imp.n){

      # Convert 10-year predicted risks to 1-year predicted risks
      score2family[[model.i]][[sex.i]][[imp.i]]$two_year_risk <- 1 - (1 - score2family[[model.i]][[sex.i]][[imp.i]]$risk)^(2/10)

      data_tmp <- score2family[[model.i]][[sex.i]][[imp.i]]

      # Add one-year age groups
      data_tmp[,"agegrp_one"] <- data.frame(cut(data_tmp$age,breaks=seq(min(data_tmp$age),ceiling(max(data_tmp$age)),by=1),include.lowest=TRUE, right = FALSE))

      cal_two[[model.i]][[sex.i]][[imp.i]] <- cal_subgrp_case_cohort_one(dataset= data_tmp, risk ="two_year_risk", n.groups=length(levels(data_tmp$agegrp_one)),
                                                    score = model.i, year.risk = 728,
                                                    groupvar="agegrp_one", population= sex.i, which.sampling.fraction = sampling_fraction[[model.i]][[sex.i]])
    }
  }
}

# Save cal object
save(cal_two, file = paste0(path_to_results, "calibration_casecohort_twoyear_agegroup1_bysex_", Sys.Date(),".RData"))

# E/O ratios (not estimating these here) and calibration slopes
  # Expected - weighted mean of weighted predicted risks to account for case-cohort design and to account for proportions of different one year age groups
  # Observed - weighted mean of estimated 10-year observed risks to account for proportions of different one year age groups
  # To estimate the calibration slope conduct a linear regression on the predicted and observed

# Empty objects
cal_two_table <- list(); tmp <- list(); cal_two_quant <- NULL

for(model.i in models_of_interest){
  for(sex.i in sex_of_interest){
    
    # Initialise calibration quantification results table
    tmp[[model.i]][[sex.i]] <- as.data.frame(matrix(data = NA, nrow = imp.n, ncol = 0))
    tmp[[model.i]][[sex.i]][ ,"sex"] <- sex.i
    tmp[[model.i]][[sex.i]][ ,"score"] <- model.i
    
    for(imp.i in 1:imp.n){
      
      # Reduce to important calibration information to cal_two_table
      cal_two_table[[model.i]][[sex.i]][[imp.i]] <- cal_two[[model.i]][[sex.i]][[imp.i]][,c("category", "weighted_predicted_risk", "observed_2y")]
      cal_two_table[[model.i]][[sex.i]][[imp.i]] <- dplyr::left_join(cal_two_table[[model.i]][[sex.i]][[imp.i]], ind_counts[[model.i]][[sex.i]], by = c("category" = "strata")) # Add how many individuals are in each age group (these values should come from the whole cohort)
      
      # Transformation
      cal_two_table[[model.i]][[sex.i]][[imp.i]][,"transf_weighted_predicted_risk"] <- log(-log(1-cal_two_table[[model.i]][[sex.i]][[imp.i]][,"weighted_predicted_risk"]))
      cal_two_table[[model.i]][[sex.i]][[imp.i]][,"transf_observed_2y"] <- log(-log(1-cal_two_table[[model.i]][[sex.i]][[imp.i]][,"observed_2y"]))
      
      # Regression model to estimate the calibration slope # Transformed by log(-log(1-x)) for both obs and predicted - force intercept to 0 to get overall calibration
      regression <- lm(formula = transf_observed_2y  ~  0 + transf_weighted_predicted_risk, weights = n_ind, data = cal_two_table[[model.i]][[sex.i]][[imp.i]])
      
      # Store results
      tmp[[model.i]][[sex.i]][imp.i ,"imp"] <- imp.i #indicate which imputation the results are for
      # Calibration slope
      tmp[[model.i]][[sex.i]][imp.i ,"calslope"] <- regression$coefficients["transf_weighted_predicted_risk"]
      tmp[[model.i]][[sex.i]][imp.i ,"calslope_se"] <- summary(regression)$coefficients["transf_weighted_predicted_risk", "Std. Error"]
      tmp[[model.i]][[sex.i]][imp.i ,"calslope_low"] <- tmp[[model.i]][[sex.i]][imp.i ,"calslope"] - 1.96*tmp[[model.i]][[sex.i]][imp.i ,"calslope_se"]
      tmp[[model.i]][[sex.i]][imp.i ,"calslope_high"] <- tmp[[model.i]][[sex.i]][imp.i ,"calslope"] + 1.96*tmp[[model.i]][[sex.i]][imp.i ,"calslope_se"]
      # Estimate a total weighted predicted risk
      tmp[[model.i]][[sex.i]][imp.i ,"E"]  <- NA
      # Estimate a total observed risk
      tmp[[model.i]][[sex.i]][imp.i ,"O"] <- NA
    }
    cal_two_quant <- rbind(cal_two_quant, tmp[[model.i]][[sex.i]])
  }
}
rm(tmp) # Remove temporary variable
cal_two_quant[,"EO"] <- cal_two_quant[,"E"] / cal_two_quant[,"O"] # Estimate expected-to-observed ratio

# Save calibration slopes
write.csv(cal_two_quant, file = paste0(path_to_results, "calibration_quantified_twoyear_", cohort, "_", Sys.Date(), ".csv"), 
          row.names = FALSE, col.names = TRUE)


