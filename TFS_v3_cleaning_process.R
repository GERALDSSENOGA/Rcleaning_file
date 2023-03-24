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
survey_data <- read_sheet("https://docs.google.com/spreadsheets/d/1bW1Bq2iqb4DZH5mA05A2u3wAG6ozoMbSdlm5__1edGw/edit?pli=1#gid=0",sheet = "data") %>% clean_names()
monitoring_data <- read_sheet("https://docs.google.com/spreadsheets/d/18aS5wGrscudKj1InH1kXvjdRdWIDnR_IvM0UYyhvqC0/edit#gid=1047514650",sheet = "Biz_monitoring_checks") %>% clean_names()
dup_data <- read_sheet("https://docs.google.com/spreadsheets/d/18aS5wGrscudKj1InH1kXvjdRdWIDnR_IvM0UYyhvqC0/edit#gid=1047514650",sheet = "Duplicates_check") %>% clean_names() %>% data.table()
# Get duplicates in the data
dups <- dup_data[dup_data$rows_to_remove == "Remove",] %>% select(base,surveyor_id,implmonth_num,community,small_group_leader_check,small_group_member,name_icm,branch_name,batch_name,pastor_name,key)

# Clean survey_data, remove duplicates
survey_data <- anti_join(survey_data,dups, all.x = T, by="key", sort = F)

#-- From monitoring data, we select the columns needed for cleaning and the key
vars_rep <- monitoring_data %>% select(action,dataclean_colname1,new_value_1,
           dataclean_colname2,new_value_2,
           dataclean_colname3,new_value_3,
           var_name1,var_name2,var_name3,key)


vars_rep <- vars_rep %>% filter(action == "Data Cleaning") # for data cleaning

vars_rep1 <- vars_rep %>% select(var_name1,new_value_1,key) %>% dplyr::rename(var_name = var_name1,new_value = new_value_1)
vars_rep2 <- vars_rep %>% select(var_name2,new_value_2,key) %>% dplyr::rename(var_name = var_name2,new_value = new_value_2)
vars_rep3 <- vars_rep %>% select(var_name3,new_value_3,key) %>% dplyr::rename(var_name = var_name3,new_value = new_value_3)

vars_rep_all <- rbind(vars_rep1,vars_rep2,vars_rep3)
#vars_rep_all <- rbind(vars_rep_all,vars_rep3)

vars_rep_all <- vars_rep_all[!is.na(vars_rep_all$var_name),]
vars_rep_all <- vars_rep_all %>% data.table()

# ---pivoting vars_rep_all 
data_vars_rep <- dcast(vars_rep_all,key ~ paste0(var_name,"_replacement"),value.var = "new_value" )

#-- merging survey_data with data_vars_rep by keys
survey_data_mod <- merge(survey_data,data_vars_rep,all.x = T, by="key", sort = F)
survey_data_mod <- survey_data_mod %>% data.table()
survey_data_mod$lh_challenge <- as.character(survey_data_mod$lh_challenge)
#-- variables to rreplace
values_to_replace <- unique(vars_rep_all$var_name)

#updating data
for(var in values_to_replace){
  survey_data_mod[key %in% vars_rep_all[var_name == var, key], (var) := get(paste0(var, "_replacement"))]  
  
  survey_data_mod <- survey_data_mod[, -c(paste0(var, "_replacement")), with = F]
}
##################################################################################################################

#################################################################################################################
#Challenges
challenges <- survey_data_mod %>% select(key,lh_challenge)
challenges <- data.table(challenges)
challenges$lh_challenge <- str_split(challenges$lh_challenge,' ')

for(i in 1:dim(challenges)[1]){
  challenges$challenge_limited_customer_base[i] <- fifelse("1" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_inconvenient_to_purchase_raw_materials_or_inventory[i] <- fifelse("2" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_distance_of_house_to_markets[i] <- fifelse("3" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_lack_of_available_transportation[i] <- fifelse("4" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_lack_of_knowledge_of_competitively_priced_suppliers[i] <- fifelse("5" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_record_keeping[i] <- fifelse("6" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_natural_disaster[i] <- fifelse("7" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_COVID19_restrictions[i] <- fifelse("8" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_theft[i] <- fifelse("9" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_spoilage[i] <- fifelse("10" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_price_is_not_competitive [i] <- fifelse("11" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_unsupportive_LGU[i] <- fifelse("12" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_health_related_problem[i] <- fifelse("13" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_unpaid_debt_from_customers[i] <- fifelse("14" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_delayed_payment[i] <- fifelse("15" %in% challenges$lh_challenge[[i]],1,0,na=NA)
  challenges$challenge_others[i] <- fifelse("-66" %in% challenges$lh_challenge[[i]],1,0,na=NA)
}
challenges <- challenges %>% select(-c(lh_challenge) )
# -----------------------------------------------updated small group business data -------------------------#

tfsv3_data <- merge(survey_data_mod,challenges,all.x = T, by = "key",sort = F)


#-- Replacing category and type labels with missing values
#Check for missing labels   
# tfsv3_data[!is.na(tfsv3_data$lh_cat_2) & is.na(tfsv3_data$lh_cat_label_2),]
#-- category labels
tfsv3_data <- tfsv3_data[tfsv3_data$lh_cat_2 == 2 & tfsv3_data$lh_cat_label_2 =="Buy and sell",lh_cat_label_2 := "Food production"]

tfsv3_data <- tfsv3_data[tfsv3_data$lh_cat_2 == 1 & is.na(tfsv3_data$lh_cat_label_2),lh_cat_label_2 := "Buy and sell"]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_cat_2 == 7 & is.na(tfsv3_data$lh_cat_label_2),lh_cat_label_2 := "Services"]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_cat_2 == 5 & is.na(tfsv3_data$lh_cat_label_2),lh_cat_label_2 := "Livestock fattening"]
#-- type labels
tfsv3_data <- tfsv3_data[tfsv3_data$lh_type_2 == 74 & is.na(tfsv3_data$lh_type_label_2),lh_type_label_2 := "Butane"]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_type_2 == 52 & is.na(tfsv3_data$lh_type_label_2),lh_type_label_2 := "Money lending"]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_type_2 == 63 & is.na(tfsv3_data$lh_type_label_2),lh_type_label_2 := "Oil"]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_type_2 == 2 & is.na(tfsv3_data$lh_type_label_2),lh_type_label_2 := "Rice "]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_type_2 == 4 & is.na(tfsv3_data$lh_type_label_2),lh_type_label_2 := "Sari-sari store"]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_type_2 == 62 & is.na(tfsv3_data$lh_type_label_2),lh_type_label_2 := "Egg selling business"]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_type_2 == 73 & is.na(tfsv3_data$lh_type_label_2),lh_type_label_2 := "Coffee products"]
tfsv3_data <- tfsv3_data[tfsv3_data$lh_type_2 == 29 & is.na(tfsv3_data$lh_type_label_2),lh_type_label_2 := "Pig"]

write_sheet(tfsv3_data,"https://docs.google.com/spreadsheets/d/1pJMw67pskiuoLLu3-Zd1_BKYp3kkVm17Q77UQzS584k/edit#gid=0",sheet = "data")

#################################################################################################################
# selecting bizz category and type for updating the preload, will consider the latest month [change this every month = july]

update_preload <- tfsv3_data %>% filter(implmonth == "December") %>% 
  select(small_group_leader,implmonth_num,lh_cat_1,lh_cat_label_1,lh_cat_2,lh_cat_label_2,lh_cat_3,lh_cat_label_3,lh_cat_4,lh_cat_label_4,
lh_type_1,lh_type_label_1,lh_type_2,lh_type_label_2,lh_type_3,lh_type_label_3,lh_type_4,lh_type_label_4,lh_desc_2,lh_desc_3,lh_desc_4) %>% dplyr::rename(small_group_leader_id = small_group_leader)


update_preload[lh_cat_2 == 1 & is.na(lh_cat_label_2),lh_cat_label_2 := "Buy and sell"]
update_preload[lh_cat_2 == 7 & is.na(lh_cat_label_2),lh_cat_label_2 := "Services"]
update_preload[lh_cat_2 == 5 & is.na(lh_cat_label_2),lh_cat_label_2 := "Livestock fattening"]

##################################################################################################################
tfs_v3_preload <- read_sheet("https://docs.google.com/spreadsheets/d/1vodNRgQvuKeIjZYIpAif2dmM8BVjxfun5iykH-P8oPQ/edit#gid=0", sheet = "data")
tfs_v3_preload_cols <- colnames(tfs_v3_preload)

preload_mod <- merge(tfs_v3_preload,update_preload,by="small_group_leader_id",all.x = T,sort = F) %>% data.table()
preload_mod$lh_category_2 <- as.integer(preload_mod$lh_category_2)
preload_mod$lh_category_3 <- as.integer(preload_mod$lh_category_3)
preload_mod$lh_category_2_label <- as.character(preload_mod$lh_category_2_label)
preload_mod$lh_category_3_label <- as.character(preload_mod$lh_category_3_label)
preload_mod$lh_type_2.x <- as.integer(preload_mod$lh_type_2.x)
preload_mod$lh_type_3.x <- as.integer(preload_mod$lh_type_3.x)
preload_mod$lh_type_2_label <- as.character(preload_mod$lh_type_2_label)
preload_mod$lh_type_3_label <- as.character(preload_mod$lh_type_3_label)
preload_mod$bus_plan_type_desc_2 <- as.character(preload_mod$bus_plan_type_desc_2)
preload_mod$bus_plan_type_desc_3 <- as.character(preload_mod$bus_plan_type_desc_3)
preload_mod$lh_category_4 <- as.integer(preload_mod$lh_category_4)
preload_mod$lh_category_4_label <- as.character(preload_mod$lh_category_4_label)
preload_mod$lh_type_4.x <- as.integer(preload_mod$lh_type_4.x)
preload_mod$lh_type_4_label <- as.character(preload_mod$lh_type_4_label)
preload_mod$bus_plan_type_desc_4 <- as.character(preload_mod$bus_plan_type_desc_4)

preload_mod[!is.na(preload_mod$implmonth),lh_category_1 := lh_cat_1]
preload_mod[!is.na(preload_mod$implmonth),lh_category_2 := lh_cat_2]
preload_mod[!is.na(preload_mod$implmonth),lh_category_3 := lh_cat_3]
preload_mod[!is.na(preload_mod$implmonth),lh_category_4 := lh_cat_4]
preload_mod[!is.na(preload_mod$implmonth),lh_category_1_label := lh_cat_label_1]
preload_mod[!is.na(preload_mod$implmonth),lh_category_2_label := lh_cat_label_2]
preload_mod[!is.na(preload_mod$implmonth),lh_category_3_label := lh_cat_label_3]
preload_mod[!is.na(preload_mod$implmonth),lh_category_4_label := lh_cat_label_4]
preload_mod[!is.na(preload_mod$implmonth),lh_type_1.x := lh_type_1.y]
preload_mod[!is.na(preload_mod$implmonth),lh_type_2.x := lh_type_2.y]
preload_mod[!is.na(preload_mod$implmonth),lh_type_3.x := lh_type_3.y]
preload_mod[!is.na(preload_mod$implmonth),lh_type_4.x := lh_type_4.y]
preload_mod[!is.na(preload_mod$implmonth),lh_type_1_label := lh_type_label_1]
preload_mod[!is.na(preload_mod$implmonth),lh_type_2_label := lh_type_label_2]
preload_mod[!is.na(preload_mod$implmonth),lh_type_3_label := lh_type_label_3]
preload_mod[!is.na(preload_mod$implmonth),lh_type_4_label := lh_type_label_4]
preload_mod[!is.na(preload_mod$implmonth),bus_plan_type_desc_2 := lh_desc_2]
preload_mod[!is.na(preload_mod$implmonth),bus_plan_type_desc_3 := lh_desc_3]
preload_mod[!is.na(preload_mod$implmonth),bus_plan_type_desc_4 := lh_desc_4]
#---change names from .x
names(preload_mod)[names(preload_mod) == "lh_type_1.x"] <- "lh_type_1"
names(preload_mod)[names(preload_mod) == "lh_type_2.x"] <- "lh_type_2"
names(preload_mod)[names(preload_mod) == "lh_type_3.x"] <- "lh_type_3"
names(preload_mod)[names(preload_mod) == "lh_type_4.x"] <- "lh_type_4"
#--- clean business description if -98
preload_mod[bus_plan_type_desc_3 == -98,bus_plan_type_desc_3 := "NA"]

preload_mod <- preload_mod %>% select(tfs_v3_preload_cols)
write_sheet(preload_mod,"https://docs.google.com/spreadsheets/d/1vodNRgQvuKeIjZYIpAif2dmM8BVjxfun5iykH-P8oPQ/edit#gid=1077750872",sheet = "data")
##################################################################################################################




