###########################################################################################################################################################################
# 
# This script stores different functions used in the general analysis in the SDE + helps with package management
# Author: Carmen Petitjean, Sam Ip
#
###########################################################################################################################################################################

## Set default repo to SDE package manager mirror
local({r <- getOption("repos")
r["CRAN"] <- "https://packages.sde.digital.nhs.uk/repository/cran-mirror/" 
options(repos=r)
})

####################################################################################################################
# Functions
####################################################################################################################

#' Function to load packages and install them only when required
#' 
#' @param p The package you desire to load

load_install_package <- function(p) {
  if (!is.element(p, installed.packages()[,1]))
    install.packages(p, dep = TRUE)
  require(p, character.only = TRUE)
}


#' Function to load packages and install them only when required
#' 
#' @param package The package you desire to load

sde_load_install_package <- function (package){
  tryCatch(
    {
      library(package, character.only = TRUE)
    },
    error = function(e) {
      install.packages(package, repos = "https://packages.sde.digital.nhs.uk/repository/cran-mirror/", dependencies = TRUE, character.only = TRUE)
      library(package, character.only = TRUE)
    }
  )
}


#' Function which reads the most recent file which starts with a certain prefix
#' 
#' @param pathofinterest Path where the file you want to read is
#' @param prefix How the name of your file start with

read_mostrecent <- function(pathofinterest, prefix){
  
  possible_files <- file.info(list.files(path = pathofinterest, pattern= paste0("^", prefix), full.names = TRUE))
  mostrecent_file <- rownames(possible_files)[which.max(possible_files$ctime)]
  object <- fread(mostrecent_file, tmpdir = "/db-mnt/databricks/rstudio_collab/CCU004_03/data/raw/")
  
  return(object)
}


#' Function which loads the most recent .RData file which starts with a certain prefix
#' @param varname Name of variable that should have the object assigned to
#' @param pathofinterest Path where the file you want to read is
#' @param prefix How the name of your file start with
#' @param suffix (Optional) how the file ends

load_mostrecent <- function(pathofinterest, prefix, suffix = NULL){
  
  if(is.null(suffix)){
    possible_files <- file.info(list.files(path = pathofinterest, pattern= paste0("^", prefix), full.names = TRUE))
  }else{
    possible_files <- file.info(intersect(list.files(path = pathofinterest, pattern= paste0("^", prefix), full.names = TRUE), 
                                          list.files(path = pathofinterest, pattern= paste0(suffix, "$"), full.names = TRUE)))
  }
  mostrecent_file <- rownames(possible_files)[which.max(possible_files$ctime)]
  
  load(mostrecent_file, envir= .GlobalEnv)
  
}

#' Function which applies Rubin's rule across imputations
#' @param dataset Dataset of interest
#' @param var Column name of the variable name which needs to be pooled
#' @param var_se Column name of the standard error of the variable name which needs to be pooled
#' @param imp.col Column name which indicates which imputation the row relates to

rubin_pool <- function(dataset, var, var_se, imp.col){
  
  # var*
  var_star <- mean(dataset[,var])
  
  # within-imputation variance
  w <- mean((dataset[,var_se])^2)
  # between-imputation variance
  b1 <- 0
  for(imp.i in 1:max(dataset[,imp.col])){ 
    b1 <- b1 + (dataset[which(dataset[,imp.col] == imp.i), var] - var_star)^2
  }
  b <- b1 / (max(dataset[,imp.col]) - 1)
  # Total variance of C*
  var_star_var <- w + (1 + 1/max(dataset[,imp.col]))*b
  
  rubin <- NULL
  rubin <- data.frame(imp = c("combined"),
                       var = c(var_star),
                       var_star__se = c(sqrt(var_star_var)), 
                       var_lower = c(var_star - 1.96*sqrt(var_star_var)),
                       var_upper = c(var_star + 1.96*sqrt(var_star_var)))
  
  return(rubin)
}
