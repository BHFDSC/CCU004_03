###########################################################################################################################################################################
# 
# This script conducts multiple imputation for the validation of SCORE2 family-of-models
# Author: Carmen Petitjean
#
###########################################################################################################################################################################

source("software/backup_Rprofile") # Installs packages within the CCU004_03 subfolder
source("general_functions.R") # Load general functions

###########################################################################################################################################
# 1. Install packages, load required libraries and load external functions
###########################################################################################################################################

required_packages <- c("rlang", "mice", "ggplot2", "cowplot", "survival", "ggsurvfit",
                       "naniar", "grid", "future")

for(pack in required_packages){
  load_install_package(pack)
  print(paste0(pack, " done"))
}

#Loading external functions
source("Analysis/03.1_imputation/imputation_functions.R")

###########################################################################################################################################
# 2. Configuration
###########################################################################################################################################

# Define cohort
cohort <- "c02"

# Load data subset 
load_mostrecent(pathofinterest = paste0("Analysis/03.1_imputation/data/", cohort, "/"), prefix = paste0("imputation_subsets_forvalidation_", cohort))

# Paths to store results
path_to_data_results <- paste0( "Analysis/03.1_imputation/output/validation_mix/", cohort, "/")
path_to_plots <- paste0( "Analysis/03.1_imputation/output/validation_mix/", cohort, "/")

# Set seed
set.seed(1234)

# Configure parallelisation
n.cores <- availableCores() - 30

###########################################################################################################################################
# 3. Data wrangling
###########################################################################################################################################

scorefamily <- score2family_validation
# Remove unnecessary variables
rm(score2family_validation)

###########################################################################################################################################
# 4. Imputation
###########################################################################################################################################

# Ensure variable type are correct, categorical variables should be set as factors

if(cohort == "c02"){
  colnames_to_be_factors <- c("sex", "smoking", "cvd_event", "comp_event", "event_indicator", "IMD_2019_DECILES", "region_uk", 
                              "ETHNIC_CAT", "diabetes_type2", "meds_statin", "meds_bp_lowering",
                              "goodvacc", "hxcovid", "hxcovid_hosp_primary")
}else if(cohort == "c01"){
  colnames_to_be_factors <- c("sex", "smoking", "cvd_event", "comp_event", "event_indicator", "IMD_2019_DECILES", "region_uk", 
                              "ETHNIC_CAT", "diabetes_type2", "meds_statin", "meds_bp_lowering")
}



for(model.list in names(scorefamily)){
  print(model.list)
  for(sex.specific in names(scorefamily[[model.list]])){
    print(sex.specific)
    data <- scorefamily[[model.list]][[sex.specific]]
    for(i in colnames_to_be_factors){
      if(i %in% colnames(scorefamily[[model.list]][[sex.specific]])){
        scorefamily[[model.list]][[sex.specific]][,i] <- as.factor(scorefamily[[model.list]][[sex.specific]][,i])
      }
    }
    for(i in colnames(data)){print(i); print(class(data[,i]))}
  }
}


############################################################
# 4.1 Plot missingness patterns
############################################################

# Drop variables to not include it in the imputation model
for(model.list in names(scorefamily)){
  print(model.list)
  for(sex.specific in names(scorefamily[[model.list]])){
    print(sex.specific)
    scorefamily[[model.list]][[sex.specific]] <- subset(scorefamily[[model.list]][[sex.specific]], select = -c(fu_days, cvd_event, comp_event))
  }
}

print("Investigating missingness patterns")

pdf(paste0(path_to_plots, "missingness_pattern_", cohort, "_", Sys.Date(), ".pdf"), width = 12, height = 15.5) # start saving all plots
par(mfrow = c(1,1))
# Plot missingness
for(model.list in names(scorefamily)){
  print(model.list)
  print(md.pattern.altered(scorefamily[[model.list]][["men"]], rotate.names = T, title.plot = paste0("Missingness pattern in men ", model.list)))
  print(md.pattern.altered(scorefamily[[model.list]][["women"]], rotate.names = T,  title.plot = paste0("Missingness pattern in women ", model.list)))
}
dev.off()

pdf(paste0(path_to_plots, "missingness_pattern_top10_", cohort, "_", Sys.Date(), ".pdf"), width = 10, height = 5.5) # start saving all plots
par(mfrow = c(1,1))
# Plot missingness, top 10 patterns
for(model.list in names(scorefamily)){
  print(model.list)
 # Top 10 patterns
  print(gg_miss_upset(scorefamily[[model.list]][["men"]], nsets = 8, nintersects = 10))
  grid.text(paste0("Missingness pattern in men ", model.list),x = 0.65, y=0.95, gp=gpar(fontsize=20))
  print(gg_miss_upset(scorefamily[[model.list]][["women"]], nsets = 8, nintersects = 10))
  grid.text(paste0("Missingness pattern in women ", model.list),x = 0.65, y=0.95, gp=gpar(fontsize=20))
}
dev.off()


# Save dataset ready for imputation
save(scorefamily, file = paste0(path_to_data_results, "scorefamily_imputationready_",cohort, "_", Sys.Date(),".RData"))


############################################################
# 4.2 Imputation configuration
############################################################

print("Setting up imputation configuration using a dry run")

# # If imputation configuration is similar to last run, we can just read the options from the previous run
# load_mostrecent(pathofinterest = path_to_data_results, prefix = paste0("dry_run_imp_configuration_", cohort, "_"))

init <- list()
pred.matrix <- vector(mode = "list", length = 0)
method_pmm <- vector(mode = "list", length = 0)

for(model.family in names(scorefamily)){
  for(sex in c("men", "women")){

    init[[model.family]][[sex]] <- mice(scorefamily[[model.family]][[sex]], maxit = 0, visitSequence = "monotone") #doing an empty imputation helps us to get the configuration elements

    # Prepare predictor matrix - this indicates which predictors are used to impute which variable
    # It is good to include both the nelson aalen estimator  AND the event indicator as predictors
    pred.matrix[[model.family]][[sex]] <- init[[model.family]][[sex]]$predictorMatrix
    pred.matrix[[model.family]][[sex]][,"PERSON_ID"]<- 0 #Make sure IDs are not used for prediction
    if("subcohort" %in% colnames(pred.matrix[[model.family]][[sex]])){pred.matrix[[model.family]][[sex]][,"subcohort"]<- 0} #Make sure sub-cohort indicator is not used for prediction
    if("sampling_fraction" %in% colnames(pred.matrix[[model.family]][[sex]])){pred.matrix[[model.family]][[sex]][,"sampling_fraction"]<- 0} #Make sure sub-cohort indicator is not used for prediction

    # Prevent the interactions to help impute the variables in them
    pred.matrix[[model.family]][[sex]][c("cage", "csbp"), "csbp.cage"] <- 0
    pred.matrix[[model.family]][[sex]][c("cage", "ctchol"), "ctchol.cage"] <- 0
    pred.matrix[[model.family]][[sex]][c("cage", "chdl"), "chdl.cage"] <- 0
    pred.matrix[[model.family]][[sex]][c("cage", "smoking"), "smoking.cage"] <- 0

    # Adjust ordered categorical method
    init[[model.family]][[sex]]$method["IMD_2019_DECILES"] <- "polr"

    if(model.family == "score2diab"){
      # Prevent the interactions to help impute the variables in them
      pred.matrix[[model.family]][[sex]][c("cage", "chba1c"), "chba1c.cage"] <- 0
      pred.matrix[[model.family]][[sex]][c("cage", "clnegfr"), "clnegfr.cage"] <- 0

      # Prevent clnegfr^2 from helping to imputing clnegfr
      pred.matrix[[model.family]][[sex]]["clnegfr", "clnegfr.clnegfr"] <- 0
    }


    # Prepare methods
    # Method using PMM for numerical variables
    method_pmm[[model.family]][[sex]] <- init[[model.family]][[sex]]$method

  }
}

# Save methods and predictor matrix inputted to run the imputation
save(pred.matrix, init, method_pmm,
     file = paste0(path_to_data_results, "dry_run_imp_configuration_", cohort, "_", Sys.Date(), ".RData"))

############################################################
# 4.3 Imputation 
############################################################

mice_pmm <- list()
pmm_comp <- list()
timer <- list()

options(future.globals.maxSize= 1572864000)

for(model.family in names(scorefamily)){
  print(model.family)
  for(sex in c("men", "women")){
    print(sex)
    # PMM for numerical variables
    enter_time <- Sys.time()
    mice_pmm[[model.family]][[sex]] <- mice::futuremice(scorefamily[[model.family]][[sex]], m=5, maxit=30, seed = NA,
                            predictorMatrix = pred.matrix[[model.family]][[sex]],
                            visitSequence = "monotone", #Determines the order of variable imputation - least missing first
                            method = method_pmm[[model.family]][[sex]],
                            donors = 10,
                            n.core = n.cores, parallelseed = 1234, future.plan = "multisession")
    end_time <- Sys.time()
    timer[[model.family]][[sex]][["pmm"]] <- end_time - enter_time; print(paste0(model.family, " ", sex, " pmm ", end_time - enter_time))
    
    # Completed imputed datasets
    pmm_comp[[model.family]][[sex]] <- complete(mice_pmm[[model.family]][[sex]], "all")

  }
}

save(pmm_comp,
     file = paste0(path_to_data_results, "imputation_complete_data_", cohort, "_", Sys.Date(),".RData"))

save(mice_pmm, #mice_norm, mice_mean,
     file = paste0(path_to_data_results, "mice_models_objects_", cohort, "_", Sys.Date(),".RData"))

save(timer, file = paste0(path_to_data_results, "timer_", cohort, "_", Sys.Date(), ".RData"))

###########################################################################################################################################
# 5. Post-imputation checks
###########################################################################################################################################

pdf(paste0(path_to_plots, "post_imputation_plots_", cohort, "_", Sys.Date(), ".pdf"), width = 12, height = 12) # start saving all plots

for(model.family in c("score2", "score2OP", "score2diab")){
  
  print(model.family)
  
  for(sex in c("men", "women")){
    
    print(sex)
    
    # Check for logged events
    print(head(mice_pmm[[model.family]][[sex]]$loggedEvents))
    
    # Plot the mean of each variable through the different iterations
    # In general, we would like the streams to intermingle and be free of any trends at the later iterations.
    # We also want to see convergence at the end
    print(plot(mice_pmm[[model.family]][[sex]], main = paste0("Mean and SD through PMM imputation in ", sex, " from ", model.family)))

    # Distribution of original vs imputed data (a red line per imputation, blue for observed)
    print(densityplot(mice_pmm[[model.family]][[sex]], main = paste0("Variable distrubution after PMM imputation in ", sex, " from ", model.family)))
    
  }
}

dev.off()

###########################################################################################################################################
# 6. Reset parallel
###########################################################################################################################################

# Reset future plan
future::plan("sequential")

