/*  

This script estimates weighted C-indices for case-cohorts

Author: Carmen Petitjean
Date: 2024-07-23

*/


* Clear environment
clear *

* I believe this allows packages to be accessed
do "C:\Program Files\Stata17\SetPackagesLocation.do" 

* Set working directory
cd "D:\PhotonUser\My Files\Home Folder\collab\CCU004_03\Analysis\case_cohort_validation\cohort2"

* Set up log file to store results
local logdate = string( d(`c(current_date)'), "%dCY-N-D" ) // save the date
log using "output\casecohort_discrimination_ethnicity_c02_`logdate'.log", replace

* Read in predicted risk file
import delimited using  "data\predicted_risk_imputed_score2_c02_2025-02-03.csv", clear
tempfile score2 //declare the temporary file
save `score2' //save the temporary file

import delimited using  "data\predicted_risk_imputed_score2OP_c02_2025-02-03.csv", clear //import the second file
append using `score2' //append (both now .dta) 
tempfile score2score2op //declare the temporary file
save `score2score2op' //save the temporary file

import delimited using  "data\predicted_risk_imputed_score2diab_c02_2025-02-03.csv", clear //import the second file
append using `score2score2op' //append (both now .dta)  

* Create a temporary file to store the results
tempname handle
tempfile results
postfile `handle' str20 modeln str20 sex_subgroup str20 subgroup str20 subgroup_cat impn c_index c_se ci_lower ci_upper using "`results'"

* Store the different models you have in the dataset as a local variable
levelsof score, l(interested_models)
* Store the different imputation numbers you have in the dataset as a local variable
levelsof imp, l(interested_imp) 

* Storing subgroup informations
* Store the different of subgroups you have in the dataset as a local variable
levelsof ethnic_cat, l(interested_ethnicities)
*levelsof hxcovid_hosp_primary, l(interested_hxcovid_hosp_primary)
*levelsof goodvacc, l(interested_vacc)
*levelsof region_uk, l(interested_region)


* Here we'll create a for loop to estimate C-indices in all models of interest
foreach modeln in `interested_models' {
	foreach ethn in `interested_ethnicities' {
		foreach impn in `interested_imp' {
		
			* Preserve the datset
			preserve
		
			* Prepare the data subset
			keep if (imp == `impn' & score == "`modeln'" & ethnic_cat == "`ethn'") 
			
			* Print which subset indicator we are
			display " `modeln':  imputation  `impn' subgroup: `ethn'  ------------------------------- "

			* Estimate weighted Harrell's C-index for each imputed case-cohort
			somersd fu_compete opp_risk  [iw=weights], transf(c) cenind(cen_ind) level(95)
			
			* Extract the Somers' D and compute C-index
			local c_index = _b[opp_risk]
			local c_se = _se[opp_risk] 
			local ci_lower = `c_index' - (1.96*`c_se') 
			local ci_upper = `c_index' + (1.96*`c_se') 
		
			* Create a temporary dataset with results for the current subgroup
			local sex_subgroup = "all"
			local subgroup = "ETHNIC_CAT"	
			local subgroup_cat = "`ethn'"
			
			* Post the results
			post `handle' ("`modeln'") ("`sex_subgroup'") ("`subgroup'") ("`subgroup_cat'") (`impn') (`c_index') (`c_se') (`ci_lower') (`ci_upper')
			
			* Restore the dataset
			restore
		
		}
	}

}

* Close the postfile to finalize the results
postclose `handle'

* Load the results and display them
use `results', clear

* Optionally, you can export the results to a file, e.g., CSV
export delimited using "output\c_index_results_c02_byethnicity_`logdate'.csv", replace			

* Close log file
log close