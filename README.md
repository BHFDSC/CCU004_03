# Investigating the performance of the SCORE2 family-of-models during different phases of the COVID-19 pandemic

## Project description

Cardiovascular diseases (CVDs) are the leading cause of mortality globally1,2.  In 2019, it is estimated that CVDs accounted for 2.2 and 1.9 million deaths in women and men respectively in member countries of the European Society of Cardiology (ESC)3.  Risk estimation is a key component of many guidelines for primary prevention of CVDs, helping to identify high-risk individuals who might benefit from preventive measures4.  The ESC recommends 10-year CVD risk estimation using the SCORE25 algorithm for healthy individuals (those without established ASCVD [Atherosclerotic CVD], type 2 diabetes or severe conditions) aged 40 to 69 years, SCORE2-OP6 algorithm for  healthy individuals aged 70 years and over, and SCORE2-Diabetes7 algorithm for individuals with type 2 diabetes4,8,9,  collectively known as the SCORE2 family-of-models (SCORE2-FOMs). An appealing design feature of the SCORE2-FOMs is their integral recalibration system to tailor the models to different risk regions (for low, moderate, high and very high-risk countries) across Europe based on summary data (e.g. registry data). The models can be readily updated to reflect recent CVD incidence and risk factor profiles of target populations (e.g. region or country) using age- and sex-specific event data to rapidly revise the models10.
 
Recent research findings suggest the COVID-19 pandemic has indirectly and directly been associated with higher incidence of CVD11–13. COVID-19 diagnosis and hospitalisations have been found to be associated with higher CVD incidence11,13. In addition, during the COVID-19 pandemic, through changes in healthcare provision and healthcare-seeking behaviours, opportunities for treatments of major CVD risk factors have often been missed, which may result in increased future CVD incidence12,14–17. The performance of the SCORE2 models have not been assessed using recent data collected during and following the COVID-19 pandemic. Current versions of the models may underestimate risk given the potentially higher CVD rates. 


The availability of population-wide electronic health records (EHRs) for COVID-19 and CVD research offers the unique opportunity to execute ‘real-time’ validation of CVD risk models and compare the risks they estimate to detailed contemporary incidence rates by age and sex for the population. The representative risk-factor and incidence estimates yielded can, if deemed necessary, be used for the recalibration of risk prediction models, which have previously generated reliable approximations of fatal and non-fatal CVD incidence based on disease registries, research cohorts and WHO mortality databases18. Whilst leveraging EHRs, this study outlines a framework demonstrating how these contemporary data with short-term follow-up can be used for the rapid assessment and adaptation of prediction models of longer-term CVD risk. Such a framework can also serve as a useful tool for future pandemic preparedness where similar models would be used.
 
We aimed to externally validate the SCORE2-FOMs using population-wide EHRs from the whole of England and assess the need for adaptation to account for changing CVD incidence and risk factor levels during and following the COVID-19 pandemic.

## How to cite this work
> Citation details to follow

## Contents

* [View the analysis code used in NHS England's SDE for England](https://github.com/BHFDSC/CCU004_03/tree/main/code)
* [View the phenotyping algorithms and codelists used in NHS England's SDE for England](https://github.com/BHFDSC/CCU004_03/tree/main/phenotypes)

## Project approval

This is a sub-project of [project CCU004](https://github.com/BHFDSC/CCU004) approved by the CVD-COVID-UK / COVID-IMPACT Approvals & Oversight Board (sub-project: CCU004_03).

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
