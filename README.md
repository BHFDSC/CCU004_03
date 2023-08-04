# Investigating the performance of the SCORE2 family-of-models during different phases of the COVID-19 pandemic

## Project description

Cardiovascular diseases (CVDs) are the leading cause of death globally, representing 1 in 3 global deaths. CVD deaths increased from 12.1 million in 1990 to 18.6 million in 2019. Moreover, disability-adjusted life years (DALYs) and years of life lost also increased significantly globally with years lived with a disability doubling from 17.7 million to 34.4 million between 1990 and 2019, which highlights the importance of early identification of high-risk individuals for recommendation of appropriate forms of effective primary prevention. 

CVD risk prediction tools are primarily used to guide the uptake of prevention interventions, including behavioural (e.g., smoking cessation) and pharmaceutical interventions (e.g., statins) that aim to reduce the underlying CVD risk in the general population using a risk-stratification approach. These tools have the potential to also guide the intensity of interventions for CVD prevention for high-risk groups such as people with type 2 diabetes who are often already prescribed statins. The use of risk prediction tools is currently recommended in adults with no previous history of CVD every five years from 40 years of age as part of the NHS Health Check and in guidelines from National Institute for Health and Care Excellence (NICE) and the European Society of Cardiology (ESC). CVD primary prevention guidelines for the UK (NICE) and those from the ESC recommend the use of CVD risk prediction tools which consider demographic, lifestyle, physiological and biochemical parameters. NICE currently recommends the QRISK2 tool, whereas the ESC recommends the SCORE2 tool to assess ten-year CVD risk. Model variations of SCORE2 exist, with SCORE2-OP for individuals over 70 years of age and SCORE2-Diabetes for individuals with type 2 diabetes.  

Recent research findings from the BHF Data Science Centre suggest the COVID-19 pandemic has indirectly and directly induced a higher incidence of CVD. In particular, coronary heart disease and stroke incidences have been steadily rising since June 2020 and remain above the pre-pandemic rates. The reasons for this are multiple. First, the use of medications to manage CVD was disrupted during the pandemic, resulting in thousands of individuals who are likely to have missed medication for CVD prevention and management. Second, the risk of CVD has been shown to be higher immediately following a COVID-19 diagnosis and remains elevated for at least 12 months. Elevated risks are highest among individuals who remain unvaccinated or under-vaccinated against COVID-19, estimated at around 30% of the adult population. Third, access to primary and secondary care has changed during the pandemic, resulting in lower primary care consultations per person and higher emergency hospital presentations. 

An appealing design feature of the SCORE2 family-of-models is their integral recalibration system to tailor the models to different risk-regions (for low, moderate, high and very high-risk countries) across Europe based on registry data. Thus, the models can be readily updated to reflect recent CVD incidence and risk factor profiles of target populations. Hence, descriptive age- and sex-specific epidemiological data can be easily incorporated to rapidly revise models at a country or region level when needed. However, SCORE2 model performance has not been assessed in recent data collected since the COVID-19 pandemic and current versions of the models may under-estimate risk given the newly increased CVD rates.

In this study, we seek to investigate whether the SCORE2 family-of-models need recalibration to account for the changing CVD incidence (and possibly risk factor levels) due to the COVID-19 pandemic and, if so, to perform this recalibration for the population of England (and potentially Wales and Scotland). Furthermore, we aim to establish a pipeline that could rapidly update the model to account for future geographical, temporal, or demographical changes in CVD event rates.  

## How to cite this work
> Citation details to follow

## Contents

* [View the analysis code used in NHS England's SDE for England](https://github.com/BHFDSC/CCU004_03/tree/main/code)
* [View the phenotyping algorithms and codelists used in NHS England's SDE for England](https://github.com/BHFDSC/CCU004_03/tree/main/phenotypes)

## Project approval

This is a sub-project of [project CCU004](https://github.com/BHFDSC/CCU004) approved by the CVD-COVID-UK / COVID-IMPACT Approvals & Oversight Board (sub-project: CCU004_03).

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
