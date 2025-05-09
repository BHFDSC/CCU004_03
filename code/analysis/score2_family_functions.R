##################################################################################################################################
#
# This script stores functions to run the validation for the score2 family of models validation including assessing:
#                    discrimination
#                    calibration
#
# Authors: Carmen Petitjean <>
#                    
##################################################################################################################################

#' Function to calculate the C-index
#' 
#' @param dataset The input dataset
#' @param population The population used when assessing the C-index

disc <- function(dataset,
                 population,
                 score) {
  
  #Overall
  surv.obj <- with(dataset,Surv(fu_compete,cvd_event))
  
  # C-statistic for SCORE2
  # HMisc rcorr.cens: Computes the c index and the corresponding generalization of Somers' Dxy rank correlation for a censored response variable.
  rcorr_s2  <-rcorr.cens(x=1-dataset$risk,S=surv.obj)
  c_stat      <-rcorr_s2[[1]]
  se     <-rcorr_s2[[3]]/2

  np <- dim(dataset)[1]
  ne <- sum(dataset$cvd_event ==1)
  
  # Save statistics
  c.stat <-as.data.frame(cbind(score, population,c_stat,se,np,ne))
  
  return(c.stat)
  
} 


#' Function to calculate the C-index in a case-cohort
#' 
#' @param dataset The input dataset
#' @param population The population used when assessing the C-index

disc_casecohort <- function(dataset,
                 population,
                 score,
                 which.imp,
                 which.samplingfraction) {
  
  # Set weights
  dataset[,"weights"] <- 1
  dataset[which(dataset[,"subcohort"] == 1 & dataset[,"event_indicator"] == 0),"weights"] <- 1 / which.samplingfraction

  # C-statistic for SCORE2
  # HMisc rcorr.cens: Computes the c index and the corresponding generalization of Somers' Dxy rank correlation for a censored response variable.
  c <- intsurv::cIndex(time = dataset[,"fu_compete"], event = dataset[,"cvd_event"], 
                       risk_score = dataset[,"risk"], weight = dataset[,"weights"])
  c_stat <- c["index"]
  se <- NA
  
  np <- dim(dataset)[1]
  ne <- sum(dataset[,"cvd_event"] == 1)

  # Save statistics
  c.stat <-as.data.frame(cbind(score, population,c_stat,se,np,ne, which.imp))
  
  return(c.stat)
  
} 




#' Function to assess calibration by deciles
#' 
#' @param dataset The input dataset
#' @param risk The column name with the predicted risk to be assessed
#' @param n.groups How many groups of risk do we divide the data in
#' @param year.risk 
#' @param population The population used when assessing calibration

cal_deciles<- function(dataset,
                       risk,		     #define the risk score to be assessed
                       n.groups=10,  #how many groups of risk do we divide the data in
                       year.risk=364, # 1 YEAR #years of risk calculated e.g. 10-year risk   #CHECK
                       population,
                       output="caldeciles",
                       append=0) {
  
  grp<-c(1:n.groups)
  
  #Calibration assessment by splitting the data in risk deciles
  dataset$groups1  <- as.numeric(cut2(dataset[,risk],g=n.groups,levels.mean=TRUE))
  cumulative_inc.1 <- cmprsk::cuminc(ftime=dataset$fu_days,fstatus=dataset$event_indicator,cencode=0,group=dataset$groups1)
  error.1          <- as.numeric(timepoints(cumulative_inc.1[c(1:n.groups)],year.risk)$var)
  predicted.1      <- tapply(dataset[,risk],dataset$groups1,mean)
  
  observed.1       <- as.numeric(timepoints(cumulative_inc.1[c(1:n.groups)],year.risk)$est)
  
  cal.decile       <-cbind(rep(risk,n.groups),rep(population,n.groups),grp,predicted.1,observed.1)
  
  if (append==1) {
    caldeciles <- rbind(eval(parse(text=output)),cal.decile)
  } else {
    caldeciles<-cal.decile
  }
  
  assign(output,caldeciles,envir=.GlobalEnv)
  
}

#' Function to assess calibration by subgroup
#' 
#' @param dataset The input dataset
#' @param risk The column name with the predicted risk to be assessed
#' @param score Name of prediction score assessed
#' @param n.groups How many groups of risk do we divide the data in
#' @param population The population used when assessing calibration
#' @param extrapolated_observed_risk Object containing the extrapolated observed risk

cal_subgrp <- function(dataset,
                   risk,
                   score,
                   n.groups,
                   groupvar="",
                   population="Overall", 
                   extrapolated_observed_risk) {
  
  observed.2 <- as.data.frame(extrapolated_observed_risk)
  observed.2$category <- observed.2$strata
  
  # Predicted risk
  if (groupvar!="") {
    dataset.size <- dim(dataset)[1]
    events.size <- dim(dataset[which(dataset[,"event_indicator"] == 1),])[1]
    comp.size <- dim(dataset[which(dataset[,"event_indicator"] == 2),])[1]
    predicted.2      <-tapply(dataset[,risk],dataset[,groupvar],mean)
    dataset$groupvar<-groupvar
    rows1<-rep(population,length(levels(as.factor(dataset[,groupvar]))))
    rows2<-levels(as.factor(dataset[,groupvar]))
    rows2 <- gsub("\\]", ")", rows2)
    risk_col<-rep(risk,length(levels(as.factor(dataset[,groupvar]))))
  } else {
    predicted.2      <-mean(dataset[,risk])
    risk_col<-risk
    rows1<-population
    groupvar<-groupvar
    rows2<-"Overall"
  }
  
  #Combine
  risk.summary <- cbind(score,rows1,groupvar,rows2, dataset.size, events.size, comp.size, mean(dataset[,risk],na.rm=TRUE), predicted.2)
  risk.summary<-as.data.frame(risk.summary)
  names(risk.summary)<-c("score", "population","groupvar","category", "n", "ne", "nc", "mean_predicted_risk", "predicted.2")
  rownames(risk.summary)<-NULL
  
  risk.summary.with.obs <- dplyr::left_join(risk.summary, observed.2, by= "category")

  return(risk.summary.with.obs)
  
}

#' Function to assess calibration by subgroup using the one year predicted risks and actual observed risks
#' 
#' @param dataset The input dataset
#' @param risk The column name with the predicted risk to be assessed
#' @param score Name of prediction score assessed
#' @param n.groups How many groups of risk do we divide the data in
#' @param year.risk 
#' @param population The population used when assessing calibration

cal_subgrp_one <- function(dataset, 
                   risk, #define the risk score to be assessed
                   score,
                   n.groups,
                   year.risk=364,     #years of risk calculated e.g. 10-year risk
                   groupvar="",
                   population="Overall") {
  
  #Cumulative incidence, observed and predicted risks
  if (groupvar!="") {
    cumulative_inc.2 <-cmprsk::cuminc(ftime=dataset[,"fu_days"],fstatus=dataset[,"event_indicator"],cencode="0",group=dataset[,groupvar])
  } else {
    cumulative_inc.2 <-cmprsk::cuminc(ftime=dataset[,"fu_days"],fstatus=dataset[,"event_indicator"],cencode="0")
  }
  error.2          <-as.numeric(timepoints(cumulative_inc.2[c(1:n.groups)],year.risk)$var)
  observed.2       <-as.numeric(timepoints(cumulative_inc.2[c(1:n.groups)],year.risk)$est)  
  
  if (groupvar!="") {
    dataset.size <- dim(dataset)[1]
    events.size <- dim(dataset[which(dataset[,"event_indicator"] == 1),])[1]
    comp.size <- dim(dataset[which(dataset[,"event_indicator"] == 2),])[1]
    predicted.2      <-tapply(dataset[,risk],dataset[,groupvar],mean)
    dataset$groupvar<-groupvar
    rows1<-rep(population,length(levels(as.factor(dataset[,groupvar]))))
    rows2<-levels(as.factor(dataset[,groupvar]))
    risk_col<-rep(risk,length(levels(as.factor(dataset[,groupvar]))))
  } else {
    predicted.2      <-mean(dataset[,risk])
    risk_col<-risk
    rows1<-population
    groupvar<-groupvar
    rows2<-"Overall"
  }
  
  risk.summary     <-cbind(score, rows1,groupvar,rows2, dataset.size, events.size, comp.size, predicted.2,observed.2,mean(dataset[,risk],na.rm=TRUE))
  
  risk.summary<-as.data.frame(risk.summary)
  names(risk.summary)<-c("score", "population","groupvar","category", "n", "ne", "nc", "predicted.2","observed.2", "mean_predicted_risk")
  rownames(risk.summary)<-NULL
  
  return(risk.summary)
  
}



#' Function to assess calibration by subgroup in a case cohort
#' 
#' @param dataset The input dataset
#' @param risk The column name with the predicted risk to be assessed
#' @param score Name of prediction score assessed
#' @param n.groups How many groups of risk do we divide the data in
#' @param year.risk 
#' @param population The population used when assessing calibration
#' @param extrapolated_observed_risk Object containing the extrapolated observed risk

cal_subgrp_case_cohort <- function(dataset,
                   risk,
                   score,
                   n.groups,
                   year.risk=364,     #years of risk calculated e.g. 10-year risk
                   groupvar="",
                   population="Overall", 
                   extrapolated_observed_risk,
                   which.sampling.fraction) {
  
  # Read in extrapolated observed risk
  observed.2       <- as.data.frame(extrapolated_observed_risk[,c("strata", "extra_10y_obs_cvd_risk", "extra_10y_obs_cvd_risk_unadjusted", "extra_10y_obs_comp_risk")])
  # observed.2$category <- sapply(strsplit(observed.2$strata, "="), "[", 2)
  observed.2$category <- observed.2$strata
  
  # Add weights for the predicted risk
  dataset[,"weights"] <- 1
  dataset[which(dataset[,"subcohort"] == 1 & dataset[,"event_indicator"] == 0),"weights"] <- 1 / which.sampling.fraction
  
  # Predicted risk
  if (groupvar!="") {
    
    predicted.2 <- dataset %>%
      group_by(dataset[,groupvar]) %>% 
      mutate(weighted_risk = weighted.mean(risk, weights))
    
    predicted.2 <- as.data.frame(unique(predicted.2[,c(groupvar ,"weighted_risk")]))
    colnames(predicted.2) <- c("category", "weighted_predicted_risk")

    dataset$groupvar <- groupvar
    dataset.size <- dim(dataset)[1]
    events.size <- dim(dataset[which(dataset[,"event_indicator"] == 1),])[1]
    comp.size <- dim(dataset[which(dataset[,"event_indicator"] == 2),])[1]
    rows1 <- rep(population,length(levels(as.factor(dataset[,groupvar]))))
    rows2 <- levels(as.factor(dataset[,groupvar]))
    risk_col <- rep(risk,length(levels(as.factor(dataset[,groupvar]))))
  } 

  #Combine
  risk.summary <- cbind(score,rows1,groupvar,rows2, dataset.size, events.size, comp.size)
  risk.summary<-as.data.frame(risk.summary)
  names(risk.summary)<-c("score", "population","groupvar","category",  "n", "ne", "nc")
  risk.summary <- dplyr::left_join(risk.summary, predicted.2, by = "category")
  rownames(risk.summary)<-NULL

  risk.summary.with.obs <- dplyr::left_join(risk.summary, observed.2, by= "category")
  
  return(risk.summary.with.obs)
  
}


#' Function to assess calibration by subgroup in a case cohort with one year of follow-up
#' 
#' @param dataset The input dataset
#' @param risk The column name with the predicted risk to be assessed
#' @param score Name of prediction score assessed
#' @param n.groups How many groups of risk do we divide the data in
#' @param year.risk 
#' @param population The population used when assessing calibration
#' @param extrapolated_observed_risk Object containing the extrapolated observed risk

cal_subgrp_case_cohort_one <- function(dataset,
                                       risk,
                                   score,
                                   n.groups,
                                   year.risk=364,     #years of risk calculated e.g. 10-year risk
                                   groupvar="",
                                   population="Overall", 
                                   extrapolated_observed_risk,
                                   which.sampling.fraction) {
  
  #Cumulative incidence, observed risks of the subcohort
  data_subcohort <- dataset[which(dataset[,"subcohort"] == 1),]
  
  if (groupvar!="") {
    cumulative_inc.2 <-cmprsk::cuminc(ftime=data_subcohort[,"fu_days"],fstatus=data_subcohort[,"event_indicator"],cencode="0",group=data_subcohort[,groupvar])
  } else {
    cumulative_inc.2 <-cmprsk::cuminc(ftime=data_subcohort[,"fu_days"],fstatus=data_subcohort[,"event_indicator"],cencode="0")
  }
  error.2          <-as.numeric(timepoints(cumulative_inc.2[c(1:n.groups)],year.risk)$var)
  # observed.2.subcohort       <-as.numeric(timepoints(cumulative_inc.2[c(1:n.groups)],year.risk)$est)
  observed.2.subcohort       <- as.data.frame(timepoints(cumulative_inc.2[c(1:n.groups)],year.risk)$est)
  observed.2.subcohort$category <- substr(rownames(observed.2.subcohort), 1, 7)
  colnames(observed.2.subcohort) <- c("observed_2y", "category")
  
  # Add weights for the predicted risk
  dataset[,"weights"] <- 1
  dataset[which(dataset[,"subcohort"] == 1 & dataset[,"event_indicator"] == 0),"weights"] <- 1 / which.sampling.fraction
  
  # Predicted risk
  if (groupvar!="") {
    
    predicted.2 <- dataset %>%
      dplyr::group_by(agegrp_one) %>% 
      dplyr::mutate(weighted_risk = weighted.mean(two_year_risk, weights))
    
    predicted.2 <- as.data.frame(unique(predicted.2[,c(groupvar ,"weighted_risk")]))
    colnames(predicted.2) <- c("category", "weighted_predicted_risk")
    
    dataset$groupvar <- groupvar
    dataset.size <- dim(dataset)[1]
    events.size <- dim(dataset[which(dataset[,"event_indicator"] == 1),])[1]
    comp.size <- dim(dataset[which(dataset[,"event_indicator"] == 2),])[1]
    rows1 <- rep(population,length(levels(as.factor(dataset[,groupvar]))))
    rows2 <- levels(as.factor(dataset[,groupvar]))
    risk_col <- rep(risk,length(levels(as.factor(dataset[,groupvar]))))
  } 
  
  #Combine
  risk.summary <- cbind(score,rows1,groupvar,rows2, dataset.size, events.size, comp.size)
  risk.summary<-as.data.frame(risk.summary)
  names(risk.summary)<-c("score", "population","groupvar","category",  "n", "ne", "nc")
  risk.summary <- dplyr::left_join(risk.summary, predicted.2, by = "category")
  rownames(risk.summary)<-NULL
  
  risk.summary.with.obs <- dplyr::left_join(risk.summary, observed.2.subcohort, by= "category")

  return(risk.summary.with.obs)
  
}


#' Function to plot calibration with x-axis = predicted and y-axis = observed
#' @param overall_cal_object Calibration object, for men and women
#' @param predictedcol Name of column with predicted risks to plots in overall_cal
#' @param observedcol Name of column with observed risks to plots in overall_cal
#' @param xlabel x-axis label 
#' @param ylabel y-axis label
 
trad_cal_plot <- function(overall_cal_object, predictedcol, observedcol, 
                          xlabel, ylabel, 
                          model.i_fct = model.i, agegroup_of_interest.i_fct = agegroup_of_interest.i){
  
  plot_output <- ggplot(overall_cal_object, aes(x = as.numeric(overall_cal_object[,predictedcol]) * 100, y = as.numeric(overall_cal_object[,observedcol]) * 100, color = overall_cal_object[,"category"])) +
          geom_abline(slope=1, intercept = 0, linetype = "dashed") + # add the x=y line representing perfect calibration
          geom_point(size = 1) +
          scale_y_continuous(limits = c(0, NA), expand = c(0,0)) +
          scale_x_continuous(limits = c(0, NA), expand = c(0,0)) +
          ggtitle(paste0(model.i_fct, " calibration"),
                  subtitle = agegroup_of_interest.i_fct) + # title label
          xlab(xlabel) + ylab(ylabel) + # axis labels
          facet_wrap(~population) +
          theme_bw()
  
  return(plot_output)
}


#' Function to plot calibration with x-axis = age groups and y-axis = observed and predicted risks
#' @param overall_cal_object Calibration object, for men and women
#' @param agegroup_category Name of column with x-axis information 
#' @param risks_to_be_plotted Names of columns with the different risks you want to plot
#' @param risks_labels Names of labels you would like the above-mentionned columns to be plotted as 
#' @param legend_n_col How many columns the legend presents
#' @param plot_legend_ratio Ratio of plot to legend in the final pdf e.g. c(2, 0.5)

age_cal_plot <- function(overall_cal_object, risks_to_be_plotted, risks_labels,
                         legend_n_col = 2, plot_legend_ratio = c(2, 0.5)){
  
  # Make dataset in long format
  overall_cal_object <- reshape2::melt(overall_cal_object,
                      id.vars = c("score", "population", "category", "category_plot"),
                      measure.vars = risks_to_be_plotted,
                      variable.name = "risk")
  
  # Calibration plot with age as x-axis
  plot_output_tmp <- ggplot(overall_cal_object, aes(x = as.numeric(overall_cal_object[,"category_plot"]), y = as.numeric(overall_cal_object[,"value"]) * 100, color = overall_cal_object[,"risk"])) +
          geom_point(size = 1) +
          ggtitle(paste0(model.i, " calibration"),
                  subtitle = agegroup_of_interest.i) + # title label
          xlab("Age group") + ylab("Risks (%)") + # axis labels
          scale_y_continuous(limits = c(0, NA), expand = c(0,0)) + # Y-axis start from 0
          scale_color_discrete(name="Types of risks",
                               labels= risks_labels) +
          guides(color=guide_legend(ncol= legend_n_col)) +
          facet_wrap(~population) +
          theme_bw()
  
  # Get legend
  legend_plot <- get_legend(plot_output_tmp)
  
  # Combine plots
  plot_output <- plot_grid(
    plotlist =  list(plot_output_tmp + theme(legend.position="none"), legend_plot),
    ncol = 1, rel_heights = plot_legend_ratio)
  
  return(plot_output)
  
}

 

#' Function to plot calibration by subgroup categories with risks on the y axis, and age group in the x-axis
#' 
#' @param cal.object The input dataset containing calibration information
#' @param predict.col The name of the column with the predicted risks to plot
#' @param observed.col The name of the column with the observed risks to plot
#' @param title_detail Detail to the title (optional) e.g. " only in the subcohort"
#' @param max_y_axis_object The maximum set on each plot on the y-axis. This is specified so that all plots for each subgroup match.

plot_calibration_by_subgroup_agex <-function(
    cal.object, predict.col, observed.col, title_detail, max_y_axis_object
){
  
  plot_list <- list()
  combined_plot <- list()
  
  # Define unique categories
  unique_categories <- as.character(names(cal.object[[agegroup_of_interest.i]][[model.i]][[subgroup.i]]))
  
  max_y_axis <- as.numeric(max_y_axis_object[[model.i]][[subgroup.i]])
      
    for(cat.i in unique_categories){
      
      overall_cal <- NULL
      
      # Overall calibration results dataset
      overall_cal <- rbind(cal.object[[agegroup_of_interest.i]][[model.i]][[subgroup.i]][[cat.i]][["men"]], 
                           cal.object[[agegroup_of_interest.i]][[model.i]][[subgroup.i]][[cat.i]][["women"]])
        
      overall_cal$category_plot <- as.numeric(substr(overall_cal$category, 2, 3))
      
      overall_cal$facet_info <- paste0(overall_cal$population, ", n=", prettyNum(overall_cal$n,big.mark=","), "\n ne=", prettyNum(overall_cal$ne,big.mark=","), ", nc=", prettyNum(overall_cal$nc,big.mark=","))
      
      # Make dataset in long format
      overall_cal <- reshape2::melt(overall_cal, 
                                    id.vars = c("score", "population", "category", "category_plot", "facet_info"),
                                    measure.vars = c(predict.col, observed.col),
                                    variable.name = "risk")
      
      overall_cal$value_100 <- as.numeric(overall_cal$value) * 100 

      plot_list[[as.character(cat.i)]] <- ggplot(overall_cal, aes(x = category_plot, y = value_100, color = risk)) +
        geom_point(size = 1) +
        xlab("") + ylab("") + # axis labels
        scale_y_continuous(limits = c(0, max_y_axis), expand = c(0,0)) + # Y-axis start from 0
        scale_color_discrete(name="Types of risk",
                             #breaks=c(predict.col, observed.col),
                             labels=c("Predicted 10-year risk (%)", "Extrapolated observed 10-year risk (%)")) +
        labs(subtitle = cat.i) +
        # geom_text(x= min(overall_cal$category_plot)+1, y= (max_y_axis - 2), label= overall_cal$n_info, size = 3, color = "black") + 
        facet_wrap(vars(facet_info)) +
        theme_bw()
    }

  # common title
  title <- cowplot::ggdraw() +
    draw_label(paste0(model.i, " calibration by subgroup: ", subgroup.i),
                fontface = 'bold',
                x = 0.05, hjust = 0)

  # common legend: extract the legend from one of the plots
  legend <- get_legend(
    # create some space to the left of the legend
    plot_list[[as.character(unique_categories[1])]] +
    theme(legend.box.margin = margin(0, 0, 0, 12))
  )

  # Combining the calibration plots without individual legends
  for(i in unique_categories){
    plot_list[[i]] <- plot_list[[i]] + theme(legend.position="none")
  }
  
  # Set number of columns when combining plots
  if(length(unique_categories) >2){number_of_col <- 3}else{number_of_col <- length(unique_categories)}
  calibration_plots_combined <- plot_grid(
      plotlist =  plot_list,
      #labels = unique_categories, label_size = 9, # Labels not required if we are using subtitles for each plot
      ncol = number_of_col)

  # Adding title
  combined_plot <- plot_grid(title,
                             calibration_plots_combined,
                             ncol = 1,  rel_heights = c(0.2, 2))

  # Add shared x and y axis
  y.grob <- textGrob("Risks (%)",
                       gp=gpar(fontface="bold", col="black", fontsize=10), rot=90)

  x.grob <- textGrob("Age group",
                       gp=gpar(fontface="bold", col="black", fontsize=10))

  combined_plot <- arrangeGrob(combined_plot, left = y.grob, bottom = x.grob)

  # Add legend
  combined_plot <-  plot_grid(combined_plot,
                              legend, ncol = 1, rel_heights = c(8, 1.2))
  
  # Return finalised plot
  return(combined_plot)
  
}



#' Function to plot calibration by imputation with x-axis = predicted and y-axis = observed
#' 
#' @param cal.object The input dataset containing calibration information
#' @param predict.col The name of the column with the predicted risks to plot
#' @param observed.col The name of the column with the observed risks to plot
#' @param title_detail Detail to the title (optional) e.g. " only in the subcohort"

plot_calibration_by_imputation <- function(
    cal.object, predict.col, observed.col, title_detail
    ){
  
  plot_list <- list()
  combined_plot <- list()
  
  for(model.i in models_of_interest){
    for(imp.i in 1:imp.n){
      # Overall calibration results dataset
      overall_cal <- rbind(cal.object[[model.i]][["men"]][[imp.i]], cal.object[[model.i]][["women"]][[imp.i]])
      
      # This creates an easier to plot age group (works for one-year age group)
      overall_cal$category_plot <- substr(overall_cal$category, 2, 3)
      
      # Convert values for plotting
      overall_cal$predict.col.100 <- as.numeric(overall_cal[,predict.col]) * 100
      overall_cal$observed.col.100 <-as.numeric(overall_cal[,observed.col]) * 100
      
      plot_list[[imp.i]] <- ggplot(overall_cal, aes(x = predict.col.100, 
                                                    y = observed.col.100, 
                                                    color = category_plot)) +
        geom_abline(slope=1, intercept = 0, linetype = "dashed") + # add the x=y line representing perfect calibration
        geom_point(size = 1) +
        scale_y_continuous(limits = c(0, NA), expand = c(0,0)) +
        scale_x_continuous(limits = c(0, NA), expand = c(0,0)) + 
        xlab("") + ylab("") + # axis labels
        facet_wrap(vars(population)) +
        theme_bw()
    }
    
    # common title
    title <- cowplot::ggdraw() +
      draw_label(paste0(model.i, " calibration ", title_detail), 
                  fontface = 'bold',
                  x = 0.05, hjust = 0)
    
    # common legend: extract the legend from one of the plots
    legend <- get_legend(
      # create some space to the left of the legend
      plot_list[[1]] + theme(legend.box.margin = margin(0, 0, 0, 12))
    )
    
    # Combining the calibration plots
    for(i in 1:imp.n){plot_list[[i]] <- plot_list[[i]] + theme(legend.position="none")}
    
    # Set number of columns when combining plots
    if(imp.n >2){number_of_col <- 3}else{number_of_col <- imp.n}
    
    calibration_plots_combined <- plot_grid(
      plotlist =  plot_list,
      labels = c(1:imp.n), label_size = 12,
      ncol = number_of_col)
    
    # Adding title and legend
    combined_plot[[model.i]] <- plot_grid(title,
                               calibration_plots_combined,
                               ncol = 1,  rel_heights = c(0.2, 2))
    combined_plot[[model.i]]<-  plot_grid(combined_plot[[model.i]],
                               legend, ncol = 2, rel_widths = c(2, 0.5))
    
    # Add shared x and y axis
    y.grob <- textGrob("Extrapolated observed 10-year risk (%)",
                       gp=gpar(fontface="bold", col="black", fontsize=10), rot=90)
    x.grob <- textGrob("Predicted 10-year risk (%)",
                       gp=gpar(fontface="bold", col="black", fontsize=10))
    
    combined_plot[[model.i]] <- grid.arrange(arrangeGrob(combined_plot[[model.i]], left = y.grob, bottom = x.grob))
    
  }
  
  # Return finalised plot
  return(combined_plot)
}


#' Function to plot calibration by imputation with risks on the y axis, and age group in the x-axis
#' 
#' @param cal.object The input dataset containing calibration information
#' @param predict.col The name of the column with the predicted risks to plot
#' @param observed.col The name of the column with the observed risks to plot
#' @param title_detail Detail to the title (optional) e.g. " only in the subcohort"

plot_calibration_by_imputation_agex <- function(
    cal.object, predict.col, observed.col, title_detail
){
  
  plot_list <- list()
  combined_plot <- list()
  
  for(model.i in models_of_interest){
    for(imp.i in 1:imp.n){
      
      # Overall calibration results dataset
      overall_cal <- rbind(cal.object[[model.i]][["men"]][[imp.i]], cal.object[[model.i]][["women"]][[imp.i]])

      overall_cal$category_plot <- as.numeric(substr(overall_cal$category, 2, 3))
      
      # Make dataset in long format
      overall_cal <- reshape2::melt(overall_cal, 
                          id.vars = c("score", "population", "category", "category_plot"),
                          measure.vars = c(predict.col, observed.col),
                          variable.name = "risk")
      
      # Convert values for plotting
      overall_cal$value_100 <- as.numeric(overall_cal[,"value"]) * 100
      
      plot_list[[imp.i]] <- ggplot(overall_cal, aes(x = category_plot, 
                                                    y = value_100, 
                                                    color = risk)) +
        geom_point(size = 1) +
        xlab("") + ylab("") + # axis labels
        scale_y_continuous(limits = c(0, NA), expand = c(0,0)) + # Y-axis start from 0
        scale_color_discrete(name="Types of risk",
                             #breaks=c(predict.col, observed.col),
                             labels=c("Predicted 10-year risk (%)", "Extrapolated observed 10-year risk (%)")) +
        facet_wrap(vars(population)) +
        theme_bw() 

    }

    # common title
    title <- cowplot::ggdraw() +
      draw_label(paste0(model.i, " calibration ", title_detail),
                 fontface = 'bold',
                 x = 0.05, hjust = 0)
    
    # common legend: extract the legend from one of the plots
    legend <- get_legend(
      # create some space to the left of the legend
      plot_list[[1]] +
        theme(legend.box.margin = margin(0, 0, 0, 12))
    )

    # Combining the calibration plots
    for(i in 1:imp.n){plot_list[[i]] <- plot_list[[i]] + theme(legend.position="none")}

    # Set number of columns when combining plots
    if(imp.n >2){number_of_col <- 3}else{number_of_col <- imp.n}

    calibration_plots_combined <- plot_grid(
      plotlist =  plot_list,
      labels = c(1:imp.n), label_size = 12,
      ncol = number_of_col)

    # Adding title and legend
    combined_plot[[model.i]] <- plot_grid(title,
                                          calibration_plots_combined,
                                          ncol = 1,  rel_heights = c(0.2, 2))
    combined_plot[[model.i]]<-  plot_grid(combined_plot[[model.i]],
                                          legend, ncol = 2, rel_widths = c(2, 0.6))

    # Add shared x and y axis
    y.grob <- textGrob("Risks (%)",
                       gp=gpar(fontface="bold", col="black", fontsize=10), rot=90)
    x.grob <- textGrob("Age group",
                       gp=gpar(fontface="bold", col="black", fontsize=10))

    combined_plot[[model.i]] <- grid.arrange(arrangeGrob(combined_plot[[model.i]], left = y.grob, bottom = x.grob))
    
  }
  
  # Return finalised plot
  return(combined_plot)
}



#' Function to plot calibration by subgroup categories with risks on the y axis, and age group in the x-axis
#' 
#' @param cal.object The input dataset containing calibration information
#' @param predict.col The name of the column with the predicted risks to plot
#' @param observed.col The name of the column with the observed risks to plot
#' @param title_detail Detail to the title (optional) e.g. " only in the subcohort"
#' @param max_y_axis_object The maximum set on each plot on the y-axis. This is specified so that all plots for each subgroup match.

plot_calibration_by_subgroup_agex_by_imputation <-function(
    cal.object, predict.col, observed.col, title_detail, max_y_axis_object
){
  
  plot_list <- list()
  combined_plot <- list()
  
  # Define unique categories
  unique_categories <- as.character(names(cal.object[[agegroup_of_interest.i]][[model.i]][[subgroup.i]]))
  
  max_y_axis <- as.numeric(max_y_axis_object[[model.i]][[subgroup.i]])
  
  for(cat.i in unique_categories){
    
    overall_cal <- NULL
    
    # Overall calibration results dataset
    overall_cal <- rbind(cal.object[[agegroup_of_interest.i]][[model.i]][[subgroup.i]][[cat.i]][["men"]][[imp.i]], 
                         cal.object[[agegroup_of_interest.i]][[model.i]][[subgroup.i]][[cat.i]][["women"]][[imp.i]])
    
    overall_cal$category_plot <- as.numeric(substr(overall_cal$category, 2, 3))
    
    overall_cal$facet_info <- paste0(overall_cal$population, ", n=", prettyNum(overall_cal$n,big.mark=","), "\n ne=", prettyNum(overall_cal$ne,big.mark=","), ", nc=", prettyNum(overall_cal$nc,big.mark=","))
    
    # Make dataset in long format
    overall_cal <- reshape2::melt(overall_cal, 
                                  id.vars = c("score", "population", "category", "category_plot", "facet_info"),
                                  measure.vars = c(predict.col, observed.col),
                                  variable.name = "risk")
    
    overall_cal$value_100 <- as.numeric(overall_cal$value) * 100 
    
    plot_list[[as.character(cat.i)]] <- ggplot(overall_cal, aes(x = category_plot, y = value_100, color = risk)) +
      geom_point(size = 1) +
      xlab("") + ylab("") + # axis labels
      scale_y_continuous(limits = c(0, max_y_axis), expand = c(0,0)) + # Y-axis start from 0
      scale_color_discrete(name="Types of risk",
                           #breaks=c(predict.col, observed.col),
                           labels=c("Predicted 10-year risk (%)", "Extrapolated observed 10-year risk (%)")) +
      labs(subtitle = cat.i) +
      # geom_text(x= min(overall_cal$category_plot)+1, y= (max_y_axis - 2), label= overall_cal$n_info, size = 3, color = "black") + 
      facet_wrap(vars(facet_info)) +
      theme_bw()
  }
  
  # common title
  title <- cowplot::ggdraw() +
    draw_label(paste0(model.i, " calibration by subgroup: ", subgroup.i, " imputation ", imp.i),
               fontface = 'bold',
               x = 0.05, hjust = 0)
  
  # common legend: extract the legend from one of the plots
  legend <- get_legend(
    # create some space to the left of the legend
    plot_list[[as.character(unique_categories[1])]] +
      theme(legend.box.margin = margin(0, 0, 0, 12))
  )
  
  # Combining the calibration plots without individual legends
  for(i in unique_categories){
    plot_list[[i]] <- plot_list[[i]] + theme(legend.position="none")
  }
  
  # Set number of columns when combining plots
  if(length(unique_categories) >2){number_of_col <- 3}else{number_of_col <- length(unique_categories)}
  calibration_plots_combined <- plot_grid(
    plotlist =  plot_list,
    #labels = unique_categories, label_size = 9, # Labels not required if we are using subtitles for each plot
    ncol = number_of_col)
  
  # Adding title
  combined_plot <- plot_grid(title,
                             calibration_plots_combined,
                             ncol = 1,  rel_heights = c(0.2, 2))
  
  # Add shared x and y axis
  y.grob <- textGrob("Risks (%)",
                     gp=gpar(fontface="bold", col="black", fontsize=10), rot=90)
  
  x.grob <- textGrob("Age group",
                     gp=gpar(fontface="bold", col="black", fontsize=10))
  
  combined_plot <- arrangeGrob(combined_plot, left = y.grob, bottom = x.grob)
  
  # Add legend
  combined_plot <-  plot_grid(combined_plot,
                              legend, ncol = 1, rel_heights = c(8, 1.2))
  
  # Return finalised plot
  return(combined_plot)
  
}



