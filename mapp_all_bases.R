library(googlesheets4)
library(tidyverse)
library(janitor)
library(lubridate, warn.conflicts = FALSE)
library(plyr)
library(data.table)
library(plyr)
library(dplyr)

map_all <- DomoR::fetch('')
sum(map_all$tf_graduated_participants)

# Coomunity overview data
commD <- DomoR::fetch('')
commDt <- commD %>% filter(base_name != 'Guatemala')
sum(commDt$total_graduates)
a <- commDt[is.na(commDt$pastor_id),] #%>% select(base_name,branch_name,batch_name,pastor_id,district_id,total_graduates)
                                                 
names(a)

commDt[is.na(commDt$pastor_id) & commDt$total_graduates > 1,] %>% select(base_name,branch_name,batch_name,pastor_id,icm_community_id,sys_community_id,district_id,total_graduates)


