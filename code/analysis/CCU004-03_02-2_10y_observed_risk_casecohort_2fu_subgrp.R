###########################################################################################################################################
# This script aims to provide code to calculate observed 1-year risk and estimate 10-year risk for the case cohort validation analysis.
# This provides the 10-year observed risks by the subgroups of interest
# This is conducted by one-year age-groups, by sex and diabetes status.
# Author: Carmen Petitjean 
###########################################################################################################################################

source("software/backup_Rprofile") # Installs packages within the CCU004_03 subfolder
source("general_functions.R") # Load general functions

###########################################################################################################################################
# 1. Install packages, load required libraries and load external functions
###########################################################################################################################################

required_packages <- c("rlang", "survival", "magrittr", "data.table")

for(pack in required_packages){load_install_package(pack)}

###########################################################################################################################################
# 2. Read in data
###########################################################################################################################################

# Specify cohort
cohort <- "c02"

# Data to read in? For the case-cohort analysis, we read in data from the whole cohort. 
dt <- as.data.frame(read_mostrecent(pathofinterest = paste0("data/clean/", cohort, "/"), prefix = paste0("cleandata_", cohort, "_")))

cohorts_of_interest <- c("no_diabetes", "diabetes")
sex_of_interest <- c("men", "women")
agegrp_of_interest <- c("agegrp_one", "agegrp_five")
cohort_start_date <- as.Date(ifelse(cohort == "c02", "2022-01-01", "2020-01-01"))

if(cohort == "c02"){
  dt$vacc <- ifelse(dt$num_vacc_before_baseline == "0", 0, 1)
  subgroups_of_interest <- c("region_uk", "hxcovid", "hxcovid_hosp_primary", "hxcovid_hosp_any", "hxcovid_icu", "threevacc", "goodvacc", "ETHNIC_CAT", "vacc")
}else{
  subgroups_of_interest <- c("region_uk", "ETHNIC_CAT")
}
 

# Only select useful variables to save memory
dt <- dt[,c("PERSON_ID", "sex", "ETHNIC_CAT", "age", "fu_days", 
            "cvd_event", "event_indicator", "comp_event", "diabetes_type2", subgroups_of_interest)]

# Individuals with type 2 diabetes
dt_diab <- dt[which(dt$diabetes_type2 == 1),] #diabetes dataset

###########################################################################################################################################
# 2.1. Additional data wrangling
###########################################################################################################################################

dt <- dt[-which(dt$diabetes_type2 ==1),] # Remove individuals with type 2 diabetes

dt <- list("no_diabetes" = dt, "diabetes" = dt_diab)
rm(dt_diab)

###########################################################################################################################################
# 2.2. As we have two years of follow up, we need to ensure each person contributes to two age groups.
###########################################################################################################################################

print("Additional data wrangling")

dt_unprocessed <- dt # Conserve data
dt_1 <- list()
dt_2 <- list()

days_in_years <- list("2020" = 366, "2021" = 365, "2022" = 365, "2023" = 365)

for(cohort.i in cohorts_of_interest){
  
  # Make a two datasets with the two years of follow up
  dt_1[[cohort.i]] <- dt_unprocessed[[cohort.i]]
  dt_2[[cohort.i]] <- dt_unprocessed[[cohort.i]]
  
  #############################################################################################
  # For the first year of follow-up, we have to cap the number of follow-up days to a year, and 
  # if the individuals did not experience the event within that year, we have to set their event to 0
  #############################################################################################
  
  # Changing event status to 0 if follow up is longer than a year (means they did not experience any event in the first year)
  dt_1[[cohort.i]][which(dt_1[[cohort.i]][,"fu_days"] > days_in_years[[as.character(year(cohort_start_date))]]),"cvd_event"] <- 0
  dt_1[[cohort.i]][which(dt_1[[cohort.i]][,"fu_days"] > days_in_years[[as.character(year(cohort_start_date))]]),"comp_event"] <- 0
  # Capping the number of follow-up days to number of days in a year
  dt_1[[cohort.i]][which(dt_1[[cohort.i]][,"fu_days"] > days_in_years[[as.character(year(cohort_start_date))]]),"fu_days"] <- days_in_years[[as.character(year(cohort_start_date))]]
  
  
  #############################################################################################
  # For the second year of follow-up, we have to remove individuals with fu_days < 1 year,
  # Change fu_days, to fu_days - days in the previous year and
  # Age them up by + 1, we have to set their event to 0
  #############################################################################################
  
  # Remove individuals with fu_days < 1 year
  dt_2[[cohort.i]] <- dt_2[[cohort.i]][- which(dt_2[[cohort.i]][,"fu_days"] <= days_in_years[[as.character(year(cohort_start_date))]]),]
  # Readjust number of fu_days, to remove one year
  dt_2[[cohort.i]][,"fu_days"] <- dt_2[[cohort.i]][,"fu_days"] - days_in_years[[as.character(year(cohort_start_date))]]
  # Changing the age at baseline to age+1 in the second year of follow up
  dt_2[[cohort.i]][,"age"] <- dt_2[[cohort.i]][,"age"]+1
  
}

# Assert if we have the expected number of individuals in each cohort
for(cohort.i in cohorts_of_interest){
  
  n.fu.2 <- sum(dt_unprocessed[[cohort.i]][,"fu_days"] > days_in_years[[as.character(year(cohort_start_date))]]) # Number of individuals with more than one year of follow-up
  
  if(dim(dt_2[[cohort.i]])[1] != n.fu.2){
    stop("The second dataframe does not have the expected number of individuals")}
  else{print("Check passed")}
  
}

# Combine dataframe
for(cohort.i in cohorts_of_interest){
  dt[[cohort.i]] <- rbind(dt_1[[cohort.i]], dt_2[[cohort.i]])
}

rm(dt_1, dt_2) # Remove temporary variables
rm(dt_unprocessed)

###########################################################################################################################################
# 2.3. Split the data by subgroups
###########################################################################################################################################

# Wrangling to incorporate a subgroup loop
dt_conserve <- dt
dt <- list()

for(cohort.i in cohorts_of_interest){
  for(subgroup.i in subgroups_of_interest){
    
    dt_conserve[[cohort.i]] <- as.data.frame(dt_conserve[[cohort.i]])
    
    # Define the unique categories of each subgroup
    unique_categories <- unique(dt_conserve[[cohort.i]][,subgroup.i])
    
    # Make a list that's split in the different subgroup categories
    for(cat.i in unique_categories){
      dt[[cohort.i]][[subgroup.i]][[as.character(cat.i)]] <- dt_conserve[[cohort.i]][which(dt_conserve[[cohort.i]][,subgroup.i] == cat.i),]
      
    }
  }
}


rm(dt_conserve) # Remove variable to save memory

###########################################################################################################################################
# 3. Estimate 1-year observed risk of CVD and non-CVD death
###########################################################################################################################################

print("3. Estimate 1-year observed risk of CVD and non-CVD death")

# To estimate 1-year observed risk by sex and one-year age group, we use the KM estimates

km_cvd <- list() # empty list to store the results for cvd models
km_comp <- list() # empty list to store the results for competing events models

tmp <- list()

for(j in cohorts_of_interest){
  for(subgroup.i in subgroups_of_interest){
    
    # Define the unique categories of each subgroup
    unique_categories <- names(dt[[j]][[subgroup.i]])
    
    for(cat.i in unique_categories){
      
      tmp <- dt[[j]][[subgroup.i]][[as.character(cat.i)]] %>%
        dplyr::group_split(sex) %>%
        as.list()
      names(tmp) <- c("men", "women")
      
      for(i.sex in sex_of_interest){
        tmp[[i.sex]] <- as.data.frame(tmp[[i.sex]]) 
        
        # Add one-year age groups
        tmp[[i.sex]][,"agegrp_one"] <- data.frame(cut(tmp[[i.sex]]$age,breaks=seq(min(tmp[[i.sex]]$age),ceiling(max(tmp[[i.sex]]$age)),by=1),include.lowest=TRUE, right = FALSE))
        
        # Add five-year age groups
        tmp[[i.sex]][,"agegrp_five"] <- data.frame(cut(tmp[[i.sex]]$age,breaks=seq(min(tmp[[i.sex]]$age),ceiling(max(tmp[[i.sex]]$age)),by=5),include.lowest=TRUE, right = FALSE))
        
        for(big.agegrp.i in agegrp_of_interest){
          # KM estimates
          km_cvd[[big.agegrp.i]][[j]][[subgroup.i]][[as.character(cat.i)]][[i.sex]] <- survfit(Surv(fu_days, cvd_event) ~ tmp[[i.sex]][,big.agegrp.i], data = tmp[[i.sex]])
          km_comp[[big.agegrp.i]][[j]][[subgroup.i]][[as.character(cat.i)]][[i.sex]] <- survfit(Surv(fu_days, comp_event) ~ tmp[[i.sex]][,big.agegrp.i], data = tmp[[i.sex]])
      
        }
      }
    }
  }
}

rm(tmp)

###########################################################################################################################################
# 4. Use these estimates in a life-table approach
###########################################################################################################################################

# 4.1. Estimate cvd and comp risk for each age. ##########################################################################################

print("4.1. Estimate cvd and comp risk for each age")

lt <- list()
num_strata <- list()

for(big.agegrp.i in agegrp_of_interest){
  for(cohort.i in cohorts_of_interest){
    for(subgroup.i in subgroups_of_interest){
      
      # Define the unique categories of each subgroup
      unique_categories <- names(dt[[cohort.i]][[subgroup.i]])
      
      for(cat.i in unique_categories){
        for(sex.i in sex_of_interest){
          
          # Extract the number of strata (number of age groups)
          num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- length(km_cvd[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$strata)
          # Extract the name of that age group e.g. "80-85"
          strata_names <- names(km_cvd[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$strata)
          strata_names_shortened<- NULL
          for(i in strata_names){strata_names_shortened[i] <- stringr::str_sub(i,-7,-1)}
          
          # Cumulative sum to find the index ranges for each stratum
          index_ranges_cvd <- c(0, cumsum(km_cvd[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$strata))
          index_ranges_comp <- c(0, cumsum(km_comp[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$strata))
          
          # Set-up how we store the results
          lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- setNames(data.frame(matrix(ncol = 6, nrow = num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]])), c("strata", "r_cvd", "r_comp", 
                                                                                                                                                      "extra_10y_obs_cvd_risk", "extra_10y_obs_cvd_risk_unadjusted", "extra_10y_obs_comp_risk"))
          
          # Extract data for each stratum
          for (i in 1:num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]) {
            
            # Define the indices for the stratum
            start_index_cvd <- index_ranges_cvd[i] + 1
            end_index_cvd <- index_ranges_cvd[i + 1]
            start_index_comp <- index_ranges_comp[i] + 1
            end_index_comp <- index_ranges_comp[i + 1]
            
            # Subset the time and surv for the stratum
            stratum_data <- data.frame(
              time_cvd = km_cvd[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$time[start_index_cvd:end_index_cvd],
              surv_cvd = km_cvd[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$surv[start_index_cvd:end_index_cvd],
              cumhaz_cvd = km_cvd[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$cumhaz[start_index_cvd:end_index_cvd],
              time_comp = km_comp[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$time[start_index_comp:end_index_comp],
              surv_comp = km_comp[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$surv[start_index_comp:end_index_comp],
              cumhaz_comp = km_comp[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]$cumhaz[start_index_comp:end_index_comp]
            )
            
            # For each strata (i.e. age) save the cumhaz from the max time
            lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][i,"strata"] <- strata_names_shortened[i]
            lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][i,"cumhaz_cvd"] <- stratum_data[which(stratum_data$time_cvd == max(stratum_data$time_cvd)), "cumhaz_cvd"]
            lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][i,"cumhaz_comp"] <- stratum_data[which(stratum_data$time_comp == max(stratum_data$time_comp)), "cumhaz_comp"]
            lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][i,"r_cvd"] <-  1 - exp(- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][i,"cumhaz_cvd"])
            lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][i,"r_comp"] <- 1 - exp(- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][i,"cumhaz_comp"])
            
          }  
        }
      }
    }
  }
}

rm(km_cvd, km_comp, stratum_data, start_index_cvd, end_index_cvd, start_index_comp, end_index_comp)

## 4.2. Estimate CVD-free survival for each age ##########################################################################################

print("4.2. Estimate CVD-free survival for each age")

cvd_free_surv <- list()
cvd_free_surv_unadjusted <- list()

for(big.agegrp.i in agegrp_of_interest){
  for(cohort.i in cohorts_of_interest){
    
    for(subgroup.i in subgroups_of_interest){
      
      # Define the unique categories of each subgroup
      unique_categories <- names(dt[[cohort.i]][[subgroup.i]])
      
      for(cat.i in unique_categories){
        for(sex.i in sex_of_interest){
          
          cvd_free_surv[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- as.data.frame(matrix(data = NA, nrow = num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]], 
                                                                                     ncol = num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]))
          rownames(cvd_free_surv[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]) <- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"strata"]
          colnames(cvd_free_surv[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]) <- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"strata"]
          cvd_free_surv_unadjusted[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- cvd_free_surv[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
          
          for(agegrp.i in 1:num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]){
            
            # Isolate the age-groups, cohort and sex-specific data to improve code readability
            lt_tmp <- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            
            # If this is the first age group stratum, then survival is equal to 1
            cvd_free_surv[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i, agegrp.i] <- 1
            cvd_free_surv_unadjusted[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i, agegrp.i] <- 1
            for(next_ten.i in 0:10){
              cvd_free_surv[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i + 1 + next_ten.i, agegrp.i] <- cvd_free_surv[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i + next_ten.i, agegrp.i] * (1 - lt_tmp[agegrp.i + next_ten.i,"r_cvd"] - lt_tmp[agegrp.i + next_ten.i,"r_comp"])
              cvd_free_surv_unadjusted[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i + 1 + next_ten.i, agegrp.i] <- cvd_free_surv_unadjusted[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i + next_ten.i, agegrp.i] * (1 - lt_tmp[agegrp.i + next_ten.i,"r_cvd"])
            }
            
            # Assigning the new results back into the big results list
            rm(lt_tmp) # Delete temporary variables
            
          }
        }
      }
    }
  }
}


## 4.3. Estimate the cause-specific CVD risk for each age ##########################################################################################

print("4.3. Estimate the cause-specific CVD risk for each age")

cause_specific_cvd_r <- list()
cause_specific_cvd_r_unadjusted <- list()
cause_specific_comp_r <- list()

for(big.agegrp.i in agegrp_of_interest){
  for(cohort.i in cohorts_of_interest){
    for(subgroup.i in subgroups_of_interest){
      
      # Define the unique categories of each subgroup
      unique_categories <- names(dt[[cohort.i]][[subgroup.i]])
      
      for(cat.i in unique_categories){
        for(sex.i in sex_of_interest){
          
          cause_specific_cvd_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- as.data.frame(matrix(data = NA, nrow = num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]], 
                                                                                            ncol = num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]))
          rownames(cause_specific_cvd_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]) <- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"strata"]
          colnames(cause_specific_cvd_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]) <- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"strata"]
          
          cause_specific_cvd_r_unadjusted[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- cause_specific_cvd_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
          cause_specific_comp_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- cause_specific_cvd_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
          
          for(agegrp.i in 1:num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]){
            
            # Isolate the age-groups, cohort and sex-specific data to improve code readability
            lt_tmp <- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            cvd_free_surv_tmp <- cvd_free_surv[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            cvd_free_surv_unadjusted_tmp <- cvd_free_surv_unadjusted[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            
            
            for(next_ten.i in 0:9){
              # CVD
              cause_specific_cvd_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i + next_ten.i,agegrp.i] <- (lt_tmp[agegrp.i+ next_ten.i,"r_cvd"] / (lt_tmp[agegrp.i+ next_ten.i,"r_cvd"] + lt_tmp[agegrp.i+ next_ten.i,"r_comp"]))* 
                (cvd_free_surv_tmp[agegrp.i+ next_ten.i,agegrp.i] - cvd_free_surv_tmp[agegrp.i+ 1+ next_ten.i,agegrp.i])
              
              # CVD un-adjusted for competing risks
              cause_specific_cvd_r_unadjusted[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i+ next_ten.i,agegrp.i] <- 1 * 
                (cvd_free_surv_unadjusted_tmp[agegrp.i+ next_ten.i,agegrp.i] - cvd_free_surv_unadjusted_tmp[agegrp.i+ 1+ next_ten.i,agegrp.i])
              
              # Competing event
              cause_specific_comp_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][agegrp.i+ next_ten.i,agegrp.i] <- (lt_tmp[agegrp.i+ next_ten.i,"r_comp"] / (lt_tmp[agegrp.i+ next_ten.i,"r_cvd"] + lt_tmp[agegrp.i+ next_ten.i,"r_comp"]))* 
                (cvd_free_surv_tmp[agegrp.i+ next_ten.i,agegrp.i] - cvd_free_surv_tmp[agegrp.i+ 1+ next_ten.i,agegrp.i])
            }
            
            # Assigning the new results back into the big results list
            rm(lt_tmp, cvd_free_surv_tmp, cvd_free_surv_unadjusted_tmp) # Delete temporary variables
            
          }
        }
      }
    }
  }
}


## 4.4. Estimate the extrapolated observed 10-year risk ##########################################################################################

print("4.4. Estimate the extrapolated observed 10-year risk")

for(big.agegrp.i in agegrp_of_interest){
  for(cohort.i in cohorts_of_interest){
    for(subgroup.i in subgroups_of_interest){
      
      # Define the unique categories of each subgroup
      unique_categories <- names(dt[[cohort.i]][[subgroup.i]])
      
      for(cat.i in unique_categories){
        for(sex.i in sex_of_interest){
          for(agegrp.i in 1:num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]){
            
            # Isolate the age-groups, cohort and sex-specific data to improve code readability
            lt_tmp <- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            cause_specific_cvd_r_tmp <- cause_specific_cvd_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            cause_specific_cvd_r_unadjusted_tmp <- cause_specific_cvd_r_unadjusted[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            cause_specific_comp_r_tmp <- cause_specific_comp_r[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            
            if(big.agegrp.i == "agegrp_one"){ #if age groups are one-year age groups, you use the cause_specific_cvd_risk of your age group and next nine
              # CVD
              lt_tmp[agegrp.i,"extra_10y_obs_cvd_risk"] <- sum(cause_specific_cvd_r_tmp[agegrp.i:(agegrp.i+9), agegrp.i])
              # CVD un-adjusted for competing risks
              lt_tmp[agegrp.i,"extra_10y_obs_cvd_risk_unadjusted"] <- sum(cause_specific_cvd_r_unadjusted_tmp[agegrp.i:(agegrp.i+9),agegrp.i])
              # Competing
              lt_tmp[agegrp.i,"extra_10y_obs_comp_risk"] <- sum(cause_specific_comp_r_tmp[agegrp.i:(agegrp.i+9),agegrp.i])
            }
            
            if(big.agegrp.i == "agegrp_five"){ #if age groups are five-year age groups, you use the cause_specific_cvd_risk the next age group and times it by 10
              # CVD
              lt_tmp[agegrp.i,"extra_10y_obs_cvd_risk"] <- cause_specific_cvd_r_tmp[agegrp.i+1,agegrp.i] * 10
              # CVD un-adjusted for competing risks
              lt_tmp[agegrp.i,"extra_10y_obs_cvd_risk_unadjusted"] <- cause_specific_cvd_r_unadjusted_tmp[agegrp.i+1,agegrp.i] * 10 
              # Competing
              lt_tmp[agegrp.i,"extra_10y_obs_comp_risk"] <- cause_specific_comp_r_tmp[agegrp.i+1,agegrp.i] * 10
            }
            
            # Assigning the new results back into the big results list
            lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- lt_tmp
            rm(lt_tmp, cause_specific_cvd_r_tmp, cause_specific_cvd_r_unadjusted_tmp, cause_specific_comp_r_tmp) # Delete temporary variables
            
          }
        }
      }
    }
  }
}


###########################################################################################################################################
# 5. Using KM estimates, use an alternative way to estimate 10-year CVD risk not adjusting for competing events
###########################################################################################################################################

print("5. Using KM estimates, use an alternative way to estimate 10-year CVD risk not adjusting for competing events")

for(big.agegrp.i in agegrp_of_interest){
  for(cohort.i in cohorts_of_interest){
    for(subgroup.i in subgroups_of_interest){
      
      # Define the unique categories of each subgroup
      unique_categories <- names(dt[[cohort.i]][[subgroup.i]])
      
      for(cat.i in unique_categories){
        for(sex.i in sex_of_interest){
          for(agegrp.i in 1:num_strata[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]){
            
            # Isolate the age-groups, cohort and sex-specific data to improve code readability
            lt_tmp <- lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]]
            
            if(big.agegrp.i == "agegrp_one"){ 
              # CVD unadjusted using KM
              lt_tmp[agegrp.i,"r_cvd_10"] <- 1 - exp(- lt_tmp[agegrp.i +4 ,"r_cvd"] * 10)
              lt_tmp[agegrp.i,"r_comp_10"] <- 1 - exp(- lt_tmp[agegrp.i +4 ,"r_comp"] * 10)
            }
            
            if(big.agegrp.i == "agegrp_five"){ 
              # CVD unadjusted using KM
              lt_tmp[agegrp.i,"r_cvd_10"] <- 1 - exp(- lt_tmp[agegrp.i +1 ,"r_cvd"] * 10)
              lt_tmp[agegrp.i,"r_comp_10"] <- 1 - exp(- lt_tmp[agegrp.i +1 ,"r_comp"] * 10)
            }
            
            # Assigning the new results back into the big results list
            lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]] <- lt_tmp
            rm(lt_tmp) # Delete temporary variables
            
          }
        }
      }
    }
  }
}


###########################################################################################################################################
# 6. Save ouput
###########################################################################################################################################

save(lt, file = paste0("Analysis/01_extrapolating_cvd_risks/two_year_fu/output/", cohort, "/extrapolated_10yobsrisk_wholecohort_bysubgroup_", cohort, "_",Sys.Date(),".RData"))

# save output as csv to export
lt_as_csv <- NULL
for(big.agegrp.i in agegrp_of_interest){
  for(cohort.i in cohorts_of_interest){
    for(subgroup.i in subgroups_of_interest){
      
      # Define the unique categories of each subgroup
      unique_categories <- names(dt[[cohort.i]][[subgroup.i]])
      
      for(cat.i in unique_categories){
        for(sex.i in sex_of_interest){
          lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"age_strata"] <- stringr::str_sub(lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"strata"], -7, -1)
          lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"sex"] <- sex.i
          lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"diabetes_status"] <- cohort.i
          lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"years_per_agegroup"] <- big.agegrp.i
          lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"subgroup"] <- subgroup.i
          lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]][,"subgroup_category"] <- cat.i
          
          lt_as_csv <- rbind(lt_as_csv, lt[[big.agegrp.i]][[cohort.i]][[subgroup.i]][[as.character(cat.i)]][[sex.i]])
        }
        
      }
    }
  }
}

fwrite(lt_as_csv, file = paste0("Analysis/01_extrapolating_cvd_risks/two_year_fu/output/", cohort, "/lt_wholecohort_bysubgroup_", cohort, "_", Sys.Date(), ".csv"))


