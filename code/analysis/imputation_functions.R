###########################################################################################################################################################################
# 
# This script stores different functions used during the imputation analysis
# Author: Carmen Petitjean
#
###########################################################################################################################################################################

####################################################################################################################
# Libraries
####################################################################################################################

library(roxygen2) # package allows for function documentations
library(mice)

####################################################################################################################
# Functions
####################################################################################################################

#' Function to make a case-cohort
#' 
#' NOTE: random subsample is extracted, then all the cases that were not selected as part of the random subsample are added.
#' 
#' NOTE: if !isnull(relative_sample_size_event): size of case-cohort subcohort is relative to either the number of cvd event or competing event (taking the largest)
#' 
#' @param dataset Dataset to make sub-samples from
#' @param sex_value Sub-sample are sex-specific. Insert value of SEX column that you wish to make a sub-sample from.
#' @param relative_sample_size_total Total size of your random sub-sample. (Use this if you want to define your case-cohort size as an absolute number)
#' @param relative_sample_size_event How many times larger should the random sub-sample be relative to case numbers? (Use this if you want to define your case-cohort size relative to the number of cases)
#' @param event_column Name of the column where the event indicator is stores

make_case_cohort <- function(dataset, relative_sample_size_total = NULL, relative_sample_size_event = NULL, event_column= NULL){
  
  if(!is.null(relative_sample_size_event)){
    
    dataset[,"subcohort"] <- 0
    
    # Determining the maximum number of event between CVD and competing event
    max_event_n <- max(c(sum(dataset[,"cvd_event"]), sum(dataset[,"comp_event"])))
    
    # Defining the total size of the subcohort
    totalsizeofsubcohort <- relative_sample_size_event * max_event_n
    
    # Make subcohort
    random_subsample <- dataset[sample(nrow(dataset), size = (totalsizeofsubcohort)),]
    random_subsample[,"subcohort"] <- 1
    
    # Add in all the events (CVD cases and non-CVD deaths)
    all_events <- dataset[which(dataset[,event_column] == 1 | dataset[,event_column] == 2),]
    all_events <- all_events[-which(all_events[,"PERSON_ID"] %in% random_subsample[,"PERSON_ID"]),] # remove events already in the random sub-samples
    output <- rbind(random_subsample, all_events)
    output$sampling_fraction <-  totalsizeofsubcohort / dim(dataset)[1] # Save sampling fraction in dataset 
    
    return(output)
  }
  
  if(!is.null(relative_sample_size_total)){
    
    dataset[,"subcohort"] <- 0
    
    #Establishing sample size of subcohort
    sample_size <- round(dim(dataset)[1] * relative_sample_size_total)
    # Make subcohort
    random_subsample <- dataset[sample(nrow(dataset), size = (sample_size)),]
    random_subsample[,"subcohort"] <- 1
   
     # Add in all the events (CVD cases and non-CVD deaths)
    all_events <- dataset[which(dataset[,event_column] == 1 | dataset[,event_column] == 2),]
    all_events <- all_events[-which(all_events[,"PERSON_ID"] %in% random_subsample[,"PERSON_ID"]),] # remove events already in the random sub-samples
    output <- rbind(random_subsample, all_events)
    output$sampling_fraction <- relative_sample_size_total # Save sampling fraction in dataset 
    
    return(output)
  }
}


#' Function to make a sub-cohort with all cases + a random sub-sample 
#' 
#' NOTE: cases are extracted first here
#' 
#' @param dataset Dataset to make sub-samples from
#' @param sex_value Sub-sample are sex-specific. Insert value of SEX column that you wish to make a sub-sample from.
#' @param sample_size Total size of your subsample

extract_cases_add_subsample <- function(dataset, sex_value, sample_size){
  events <- dataset[which(dataset$cvd_event == 1 | dataset$noncvd_death ==1),] # Separate events
  output <- dataset[which(dataset$cvd_event == 0 & dataset$noncvd_death ==0 & dataset$SEX == sex_value),] # Sex-specific non-events
  output <- output[sample(nrow(output), size = (sample_size - dim(events[which(events$SEX == sex_value),])[1])),]
  output <- rbind(events[which(events$SEX == sex_value),], output)
}
 




#' Function to make KM plots to describe survival by missingness
#' 
#' @param dataset Dataset to make KM plot
#' @param variable Stratification variable
#' @param info Information to add to the title

km.plot <- function(dataset, variable, info){
  
  km <- survfit(Surv(fu_days, cvd_event) ~ is.na(dataset[,variable]), data = dataset) 
  output <- ggsurvfit(km, size = 1.5) + 
    labs(
      title = info,
      x = "Follow-up days",
      y = "Overall survival probability"
    ) + 
    scale_color_manual(values = c('dodgerblue', 'indianred'),
                       labels = c(paste0("Observed ", variable), paste0("Missing ", variable)))
  add_confidence_interval() +
    theme_bw()
  
  return(output)
}



#' Mice function (md.pattern) with altered source code so that the text and numbers do not overlap
#' 
#' @param x A data frame or a matrix containing the incomplete data.  Missing
#' values are coded as NA's.
#' @param plot Should the missing data pattern be made into a plot. Default is
#' `plot = TRUE`.
#' @param rotate.names Whether the variable names in the plot should be placed
#' horizontally or vertically. Default is `rotate.names = FALSE`.
#' @return A matrix with \code{ncol(x)+1} columns, in which each row corresponds
#' to a missing data pattern (1=observed, 0=missing).  Rows and columns are
#' sorted in increasing amounts of missing information. The last column and row
#' contain row and column counts, respectively.

md.pattern.altered <- function(x, plot = TRUE, rotate.names = FALSE, title.plot = NULL) {
  if (!(is.matrix(x) || is.data.frame(x))) {
    stop("Data should be a matrix or dataframe")
  }
  if (ncol(x) < 2) {
    stop("Data should have at least two columns")
  }
  R <- is.na(x)
  nmis <- colSums(R)
  # sort columnwise
  R <- matrix(R[, order(nmis)], dim(x))
  pat <- apply(R, 1, function(x) paste(as.numeric(x), collapse = ""))
  # sort rowwise
  sortR <- matrix(R[order(pat), ], dim(x))
  if (nrow(x) == 1) {
    mpat <- is.na(x)
  } else {
    mpat <- sortR[!duplicated(sortR), ]
  }
  # update row and column margins
  if (all(!is.na(x))) {
    cat(" /\\     /\\\n{  `---'  }\n{  O   O  }\n==>  V <==")
    cat("  No need for mice. This data set is completely observed.\n")
    cat(" \\  \\|/  /\n  `-----'\n\n")
    mpat <- t(as.matrix(mpat, byrow = TRUE))
    rownames(mpat) <- table(pat)
  } else {
    if (is.null(dim(mpat))) {
      mpat <- t(as.matrix(mpat))
    }
    rownames(mpat) <- table(pat)
  }
  r <- cbind(abs(mpat - 1), rowSums(mpat))
  r <- rbind(r, c(nmis[order(nmis)], sum(nmis)))
  if (plot) {
    op <- par(mar = rep(0, 4))
    on.exit(par(op))
    plot.new()
    title(title.plot, line = -1.5)
    if (is.null(dim(sortR[!duplicated(sortR), ]))) {
      R <- t(as.matrix(r[1:nrow(r) - 1, 1:ncol(r) - 1]))
    } else {
      if (is.null(dim(R))) {
        R <- t(as.matrix(R))
      }
      R <- r[1:nrow(r) - 1, 1:ncol(r) - 1]
    }
    if (rotate.names) {
      adj <- c(0, 0.5)
      srt <- 90
      length_of_longest_colname <- max(nchar(colnames(r))) / 2.6
      plot.window(
        xlim = c(-1, ncol(R) + 1),
        ylim = c(-1, nrow(R) + length_of_longest_colname),
        asp = 1
      )
    } else {
      adj <- c(0.5, 0)
      srt <- 0
      plot.window(
        xlim = c(-1, ncol(R) + 1),
        ylim = c(-1, nrow(R) + 1),
        asp = 1
      )
    }
    M <- cbind(c(row(R)), c(col(R))) - 1
    shade <- ifelse(R[nrow(R):1, ], mice::mdc(1), mice::mdc(2))
    rect(M[, 2], M[, 1], M[, 2] + 1, M[, 1] + 1, col = shade)
    for (i in 1:ncol(R)) {
      propmiss <-  round(nmis[order(nmis)][i] / nrow(x), digits = 2)
      text(i - .5, nrow(R) + .3, colnames(r)[i], adj = adj, srt = srt)
      text(i - .5, -.3, propmiss, adj = 1, srt = srt)
      #text(i - .5, -3.3, nmis[order(nmis)][i], adj = 1, srt = srt) #number of missing per variable
    }
    for (i in 1:nrow(R)) {
      text(ncol(R) + .3, i - .5, r[(nrow(r) - 1):1, ncol(r)][i], adj = 0)
      text(-.3, i - .5, rownames(r)[(nrow(r) - 1):1][i], adj = 1)
    }
    #text(ncol(R) + .3, -.3, r[nrow(r), ncol(r)])
    return(plot)
    return(r)
  } else {
    return(r)
  }
}

#' Mice function (md.pattern) with altered source code so that the text and numbers do not overlap and the numbers respect the SDE exporting rules
#' 
#' @param x A data frame or a matrix containing the incomplete data.  Missing
#' values are coded as NA's.
#' @param plot Should the missing data pattern be made into a plot. Default is
#' `plot = TRUE`.
#' @param rotate.names Whether the variable names in the plot should be placed
#' horizontally or vertically. Default is `rotate.names = FALSE`.
#' @return A matrix with \code{ncol(x)+1} columns, in which each row corresponds
#' to a missing data pattern (1=observed, 0=missing).  Rows and columns are
#' sorted in increasing amounts of missing information. The last column and row
#' contain row and column counts, respectively.

md.pattern.altered.sdesafe <- function(x, plot = TRUE, rotate.names = FALSE, title.plot = NULL) {
  if (!(is.matrix(x) || is.data.frame(x))) {
    stop("Data should be a matrix or dataframe")
  }
  if (ncol(x) < 2) {
    stop("Data should have at least two columns")
  }
  R <- is.na(x)
  nmis <- colSums(R)
  # sort columnwise
  R <- matrix(R[, order(nmis)], dim(x))
  pat <- apply(R, 1, function(x) paste(as.numeric(x), collapse = ""))
  # sort rowwise
  sortR <- matrix(R[order(pat), ], dim(x))
  if (nrow(x) == 1) {
    mpat <- is.na(x)
  } else {
    mpat <- sortR[!duplicated(sortR), ]
  }
  # update row and column margins
  if (all(!is.na(x))) {
    cat(" /\\     /\\\n{  `---'  }\n{  O   O  }\n==>  V <==")
    cat("  No need for mice. This data set is completely observed.\n")
    cat(" \\  \\|/  /\n  `-----'\n\n")
    mpat <- t(as.matrix(mpat, byrow = TRUE))
    rownames(mpat) <- table(pat)
  } else {
    if (is.null(dim(mpat))) {
      mpat <- t(as.matrix(mpat))
    }
    rownames(mpat) <- table(pat)
  }
  r <- cbind(abs(mpat - 1), rowSums(mpat))
  r <- rbind(r, c(nmis[order(nmis)], sum(nmis)))
  if (plot) {
    op <- par(mar = rep(0, 4))
    on.exit(par(op))
    plot.new()
    title(title.plot, line = -1.5)
    if (is.null(dim(sortR[!duplicated(sortR), ]))) {
      R <- t(as.matrix(r[1:nrow(r) - 1, 1:ncol(r) - 1]))
    } else {
      if (is.null(dim(R))) {
        R <- t(as.matrix(R))
      }
      R <- r[1:nrow(r) - 1, 1:ncol(r) - 1]
    }
    if (rotate.names) {
      adj <- c(0, 0.5)
      srt <- 90
      length_of_longest_colname <- max(nchar(colnames(r))) / 2.6
      plot.window(
        xlim = c(-1, ncol(R) + 1),
        ylim = c(-1, nrow(R) + length_of_longest_colname),
        asp = 1
      )
    } else {
      adj <- c(0.5, 0)
      srt <- 0
      plot.window(
        xlim = c(-1, ncol(R) + 1),
        ylim = c(-1, nrow(R) + 1),
        asp = 1
      )
    }
    M <- cbind(c(row(R)), c(col(R))) - 1
    shade <- ifelse(R[nrow(R):1, ], mice::mdc(1), mice::mdc(2))
    rect(M[, 2], M[, 1], M[, 2] + 1, M[, 1] + 1, col = shade)
    for (i in 1:ncol(R)) {
      propmiss <-  round(nmis[order(nmis)][i] / nrow(x), digits = 2)
      text(i - .5, nrow(R) + .3, colnames(r)[i], adj = adj, srt = srt)
      text(i - .5, -.3, propmiss, adj = 1, srt = srt)
      #text(i - .5, -3.3, nmis[order(nmis)][i], adj = 1, srt = srt) #number of missing per variable
    }
    for (i in 1:nrow(R)) {
      text(ncol(R) + .3, i - .5, r[(nrow(r) - 1):1, ncol(r)][i], adj = 0)
      n.ind <- round(as.numeric(rownames(r)[(nrow(r) - 1):1][i]) / 5) * 5
      if(n.ind < 10){n.ind <- "<10"}
      text(-.3, i - .5, n.ind, adj = 1) # Number of individuals with the missingness pattern
      # text(-.3, i - .5, rownames(r)[(nrow(r) - 1):1][i], adj = 1) # Not rounded
    }
    #text(ncol(R) + .3, -.3, r[nrow(r), ncol(r)])
    return(plot)
    return(r)
  } else {
    return(r)
  }
}



#' Cumulative hazard rate or Nelson-Aalen estimator (altered function)
#'
#' Calculates the cumulative hazard rate (Nelson-Aalen estimator)
#'
#' This function is useful for imputing variables that depend on survival time.
#' White and Royston (2009) suggested using the cumulative hazard to the
#' survival time H0(T) rather than T or log(T) as a predictor in imputation
#' models.  See section 7.1 of Van Buuren (2012) for an example.
#'
#' @aliases nelsonaalen hazard
#' @param data A data frame containing the data.
#' @param timevar The name of the time variable in \code{data}.
#' @param statusvar The name of the event variable, e.g. death in \code{data}.
#' @return A vector with \code{nrow(data)} elements containing the Nelson-Aalen
#' estimates of the cumulative hazard function.
#' @author Stef van Buuren, 2012
#' @references White, I. R., Royston, P. (2009). Imputing missing covariate
#' values for the Cox model.  \emph{Statistics in Medicine}, \emph{28}(15),
#' 1982-1998.
#'
#' Van Buuren, S. (2018).
#' \href{https://stefvanbuuren.name/fimd/sec-toomany.html#a-further-improvement-survival-as-predictor-variable}{\emph{Flexible Imputation of Missing Data. Second Edition.}}
#' Chapman & Hall/CRC. Boca Raton, FL.
#' @keywords misc

nelsonaalen_altered <- function(data, timevar, time2var, statusvar) {
  if (!is.data.frame(data)) {
    stop("Data must be a data frame")
  }
  timevar <- as.character(substitute(timevar))
  time2var <- as.character(substitute(time2var))
  statusvar <- as.character(substitute(statusvar))
  time <- data[, timevar, drop = TRUE]
  time2 <- data[, time2var, drop = TRUE]
  status <- data[, statusvar, drop = TRUE]

  hazard <- survival::basehaz(survival::coxph(survival::Surv(time, time2, status) ~ 1))
  idx <- match(time2, hazard[, "time"])
  hazard[idx, "hazard"]
}
