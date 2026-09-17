##################################################################################################################################
#
# This script includes functions to calculate 10-year predicted risk for SCORE2-family-of-models including
#                    SCORE2
#                    SCORE2OP
#                    SCORE2DIABETES
#
# Authors: Carmen Petitjean <>, Alexia Sampri <>
#                    
##################################################################################################################################

# Load parameters
load("Analysis/02_cca_validation/score2_family_of_models_parameters.RData")

###########################################################################################################################################
# 1. Calculation of predicted SCORE2-family-of-models risk
###########################################################################################################################################

# 1.1 # Need to center and prepare variables #######################################################################

for(model.i in models_of_interest){ # For each model of interest...
  for(sex.i in sex_of_interest){ # For each sex...
    for(imp.i in 1:imp.n){ # For every imputations
      
      # Loop progress indicator
      print(paste0("Prepping variable for ", model.i, " in ", sex.i, " in imputation number ", imp.i))
      
      data_tmp <- score2family[[model.i]][[sex.i]][[imp.i]]
      
      data_tmp$smoking <- as.numeric(as.character(data_tmp$smoking))
      
      if(model.i == "score2" | model.i == "score2diab"){
        data_tmp$hxdiabbin      <- as.numeric(data_tmp$diabetes_type2)
      }
      
      
      if(model.i == "score2diab"){
      }
      
      if(model.i == "score2OP"){
        data_tmp$hxdiabbin  <- as.numeric(data_tmp$diabetes_type2)
      }
      
      
      #Add interaction variables
      #interaction will be named variable1.variable2
      data_tmp$hxdiabbin.cage    <- data_tmp$hxdiabbin * data_tmp$cage  

      if(model.i == "score2diab"){
        data_tmp$hxdiabbin.cagediab <- data_tmp$hxdiabbin *data_tmp$cagediab
        # quadratic term for egfr
        data_tmp$clnegfr.clnegfr    <- data_tmp$clnegfr * data_tmp$clnegfr
      }
      
      # Store the new variables back into the list
      score2family[[model.i]][[sex.i]][[imp.i]] <- data_tmp
      
    }
  }
}

rm(data_tmp, sex.i, model.i)

# 3.2 # Estimate predicted risk for each model #######################################################################

for(model.i in models_of_interest){ # For each model of interest...
  for(sex.i in sex_of_interest){ # For each sex...
    
    coeff_tmp <- s2_coeff_list[[model.i]][[sex.i]]
    scales_tmp <- scales_list[[model.i]][[sex.i]]
    S10_tmp <- S10_list[[model.i]][[sex.i]]
    
    for(imp.i in 1:imp.n){ # For each imputation
      
      # Loop progress indicator
      print(paste0("Predicting risk for ", model.i, " in ", sex.i, " in imputation number ", imp.i))
      
      data_tmp <- score2family[[model.i]][[sex.i]][[imp.i]]

      # Linear predictors
      data_tmp$LP <- rowSums(data_tmp[coeff_tmp$variable] * t(replicate(dim(data_tmp)[1], coeff_tmp$coefficient)))
      if(model.i == "score2diab"){ # For SCORE2-Diabetes, the SCORE2 LP need to be added as it was defined based on S2
        data_tmp$LP <- data_tmp$LP + rowSums(data_tmp[s2_coeff_list[["score2"]][[sex.i]]$variable]  *t(replicate(dim(data_tmp)[1],s2_coeff_list[["score2"]][[sex.i]]$coefficient)))
      }
      
      # Crude risk scores
      data_tmp$cruderisk <- 1-(S10_tmp)^exp(data_tmp$LP)
      
      # Recalibrated risk scores (same recalibration factors used for SCORE2 and SCORE2-DM2)
      data_tmp$scale1 <- scales_tmp$scale1
      data_tmp$scale2 <- scales_tmp$scale2
      data_tmp$risk <- 1 - exp(-exp(data_tmp$scale1 + data_tmp$scale2 * log(-log(1- data_tmp$cruderisk))))
      
      data_tmp$fu_compete <- ifelse(data_tmp$event_indicator==2, max(data_tmp$fu_days)+1, data_tmp$fu_days)
      
      score2family[[model.i]][[sex.i]][[imp.i]] <- data_tmp 
    }
  }
}

