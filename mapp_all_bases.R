library(googlesheets4)
library(tidyverse)
library(janitor)
library(lubridate, warn.conflicts = FALSE)
library(plyr)
library(data.table)
library(plyr)
library(dplyr)
library('DomoR')

DomoR::init('caremin', 'eb7b72130d8b6283fe1fb6d447f769ad0a91f3cb1d44ca89')
map_all <- DomoR::fetch('2e6c60c6-e758-407b-83a3-da70b8c97248')
sum(map_all$tf_graduated_participants)

# Coomunity overview data
commD <- DomoR::fetch('3d637c89-a96d-49eb-875e-187f62c19522')
commDt <- commD %>% filter(base_name != 'Guatemala')
sum(commDt$total_graduates)
a <- commDt[is.na(commDt$pastor_id),] #%>% select(base_name,branch_name,batch_name,pastor_id,district_id,total_graduates)
                                                 
names(a)

commDt[is.na(commDt$pastor_id) & commDt$total_graduates > 1,] %>% select(base_name,branch_name,batch_name,pastor_id,icm_community_id,sys_community_id,district_id,total_graduates)


