library(googlesheets4)
library(tidyverse)
library(janitor)
library(lubridate, warn.conflicts = FALSE)
library(plyr)
library(data.table)
library(plyr)
library(dplyr)
##################################################################################################################
# submitted data and monitoring data
survey_data <- read_sheet("https://docs.google.com/spreadsheets/d/1oxNW6u2FUdKs3xEUqONrWOjr9JFYVZPTWoCCvdu1cI4/edit#gid=0",sheet = "data") %>% clean_names()
monitoring_data <- read_sheet("https://docs.google.com/spreadsheets/d/1b51unjVH8p-H3mZq7M8t1Od2c3vk_45XWQRUeoAFc1o/edit#gid=1047514650",sheet = "Biz_monitoring_checks") %>% clean_names()    
#-- From monitoring data, we select the columns needed for cleaning and the key
vars_rep <- monitoring_data %>% select(action,dataclean_colname1,new_value_1,
                                       dataclean_colname2,new_value_2,
                                       dataclean_colname3,new_value_3,
                                       var_name1,var_name2,var_name3,key)

vars_rep <- vars_rep %>% filter(action == "Data Cleaning") # for data cleaning

vars_rep1 <- vars_rep %>% select(var_name1,new_value_1,key) %>% dplyr::rename(var_name = var_name1,new_value = new_value_1)
vars_rep2 <- vars_rep %>% select(var_name2,new_value_2,key) %>% dplyr::rename(var_name = var_name2,new_value = new_value_2)
vars_rep3 <- vars_rep %>% select(var_name3,new_value_3,key) %>% dplyr::rename(var_name = var_name3,new_value = new_value_3)

vars_rep_all <- rbind(vars_rep1,vars_rep2)
vars_rep_all <- rbind(vars_rep_all,vars_rep3)

vars_rep_all <- vars_rep_all[!is.na(vars_rep_all$var_name),]
vars_rep_all <- vars_rep_all %>% data.table()

# ---pivoting vars_rep_all 
data_vars_rep <- dcast(vars_rep_all,key ~ paste0(var_name,"_replacement"),value.var = "new_value" )

#-- merging survey_data with data_vars_rep by keys
survey_data_mod <- merge(survey_data,data_vars_rep,all.x = T, by="key", sort = F)
survey_data_mod <- survey_data_mod %>% data.table()

#-- variables to rreplace
values_to_replace <- unique(vars_rep_all$var_name)

#updating data
for(var in values_to_replace){
  survey_data_mod[key %in% vars_rep_all[var_name == var, key], (var) := get(paste0(var, "_replacement"))]  
  
  survey_data_mod <- survey_data_mod[, -c(paste0(var, "_replacement")), with = F]
}
#################################################################################################################
#Challenges
challenges <- survey_data_mod %>% select(key,sg_biz_challenges_1,sg_biz_challenges_2)
################################################################################################################
challenges <- data.table(challenges)
challenges <- challenges %>% mutate(lh_challenge = paste(challenges$sg_biz_challenges_1,challenges$sg_biz_challenges_2) )
challenges$lh_challenge <- str_split(challenges$lh_challenge,' ')



for(i in 1:dim(challenges)[1]){
  challenges$challenge_low_demand_from_consumer[i] <- fifelse("1" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_difficult_to_manage[i] <- fifelse("2" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_record_keeping[i] <- fifelse("3" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_no_manpower_to_manage_the_business[i] <- fifelse("4" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_recurring_expenses_to_run_the_business[i] <- fifelse("5" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  #challenges$challenge_record_keeping[i] <- fifelse("6" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_none[i] <- fifelse("7" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_some_sg_members_are_not_cooperative[i] <- fifelse("8" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_delayed_repayment[i] <- fifelse("9" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_unfavourable_climate[i] <- fifelse("10" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  #challenges$challenge_price_is_not_competitive [i] <- fifelse("11" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_others[i] <- fifelse("-66" %in% challenges$lh_challenge[[i]],1,0,na=NA)
}
challenges <- challenges %>% select(-c(sg_biz_challenges_1,sg_biz_challenges_2,lh_challenge) )
##############################################################################################################
tfsv2_data <- merge(survey_data_mod,challenges,all.x = T, by = "key",sort = F)

tfsv2_data[lh_cat_2 == 7,lh_cat_label_2 := "Services"] #editing lh_cat_label_2

tfsv2_data[lh_capex_any_1 == 1 & lh_exp_ques_1 == 1 & lh_capex_1 >= 1 & lh_exp_1 > lh_capex_1,lh_exp_1 := lh_exp_1 - lh_capex_1 ]
tfsv2_data[lh_capex_any_2 == 1 & lh_exp_ques_2 == 1 & lh_capex_2 >= 1 & lh_exp_2 > lh_capex_2,lh_exp_2 := lh_exp_2 - lh_capex_2 ]
tfsv2_data[lh_capex_1 == 0,lh_capex_1 := NA ]
## Filtering only needed row by key
unique_keys <- monitoring_data %>% select(key,community)

tfs_v2_clean <- semi_join(tfsv2_data,unique_keys, by = "key", sort = F)


##############################################################################################################

write_sheet(tfs_v2_clean,"https://docs.google.com/spreadsheets/d/12WVmyIElVfzTjfncjcLJQM1xjZr8RmHiRV_ESz8hGWY/edit#gid=0",sheet = "data")




