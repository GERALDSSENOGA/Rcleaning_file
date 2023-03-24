library(googlesheets4)
library(tidyverse)
library(janitor)
library(lubridate, warn.conflicts = FALSE)
library(plyr)
library(data.table)
library(plyr)
library(dplyr)

source("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/R script/pre_post functions.R")
##************************************************************************************************************************************************************************************************##
## Operations data from DOMO, Dataset link: https://caremin.domo.com/datasources/0917c8aa-a2a8-4f3a-837c-1f6fc9aa4d24/details/data/table
domo <- rdomo::Domo(client_id="a9be407f-02b5-4c6c-a767-9875a37c1ab1",
                    secret="5903c7704fc5a8604dba4b7d6077e39529725ba9d98b6603097fe8242b868ca3")

ops_data <- domo$ds_get('0917c8aa-a2a8-4f3a-837c-1f6fc9aa4d24')
names(ops_data)[names(ops_data) == "participant_id"] <- "hhid"
ops_data <- ops_data %>% mutate(dups_check = paste0(hhid,"_",batch_name))
ops_data <- ops_data[!duplicated(ops_data$dups_check),]
ops_data$participant.id <- ops_data$sys_participant_id
#************************************************************************************************************************************************************************************************##
## Pre-Post master-codebook
mstrcodebook_var_names <- read_sheet("https://docs.google.com/spreadsheets/d/1Fncf1UgXUefS0n_60Fq299S4Ag08eUHDMiw4JSqmK_o/edit#gid=0",sheet = "all_codebook_updated_based on rawdata") %>% clean_names()
mstrcodebook_var_names2 <- copy(mstrcodebook_var_names)

names(mstrcodebook_var_names)[1] <- "var_names"
mstrcodebook_var_names <- select(mstrcodebook_var_names,var_names,new)
mstrcodebook_var_names <- mstrcodebook_var_names[!is.na(mstrcodebook_var_names$var_names),]
mstrcodebook_var_names <- mstrcodebook_var_names[!duplicated(mstrcodebook_var_names$var_names),]
mstrcodebook_var_names <- mstrcodebook_var_names[mstrcodebook_var_names$var_names != "community",]#Removing community var_name since we don't change it's name
mstrcodebook_var_names <- mstrcodebook_var_names %>% data.table()

apd <- data.frame(var_names = c("type","year","name_id"),
                  new = c("type","year","name_id"))#appending new values to the codbook that do not exist t
mstrcodebook_var_names <- bind_rows(mstrcodebook_var_names,apd)
mstrcodebook_var_names <- mstrcodebook_var_names %>% data.table()
mstrcodebook_var_names <- mstrcodebook_var_names[!duplicated(mstrcodebook_var_names$var_names),]
#************************************************************************************************************************************************************************************************##
#B3 2021 TF RCT pre
b3_2021_pre_tf_rct <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch3_Transform RCT/1.Pre/06_Clean Data/PH_FY21_B3_Presurvey_de_dupli_consent_only_data.csv")  
B3_2021_trct_var <- select_available(b3_2021_pre_tf_rct)
B3_2021_trct_dups <- duplicated_data(B3_2021_trct_var)
B3_2021_trct_var <- data_needed(B3_2021_trct_dups,B3_2021_trct_var,"pre","2021")
B3_2021_trct_var <- edit_base_level(B3_2021_trct_var)
B3_2021_trct_var <- B3_2021_trct_var %>% data.table()

#Pre survey and choices
b3_trct_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1_B30_FcVnFTFif0PNHCgJAPUuqUZc_6870qQ8UdK6fo/edit#gid=2121457295",sheet="survey") %>%  clean_names() %>% clean_names()
b3_trct_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1_B30_FcVnFTFif0PNHCgJAPUuqUZc_6870qQ8UdK6fo/edit#gid=549137032",sheet="choices") %>%  clean_names() %>% clean_names()

#create option list - pre
b3_trct_opt_list <- opt_list(b3_trct_svy,b3_trct_choices)

b3_trct_opt_list <- b3_trct_opt_list %>% data.table()
b3_trct_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b3_trct_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
 
#Handling Continuous variables - pre
B3_2021_trct_int <- clean_integer_cols(B3_2021_trct_var,b3_trct_svy) %>% data.table()

#Handling Categorical variables - pre
B3_2021_trct_cat <- clean_categorical_cols(B3_2021_trct_var,b3_trct_opt_list)
B3_2021_trct_cat <- B3_2021_trct_cat %>% select(-c("community"))
# merging categorical & continuous variables together - pre
B3_2021_pre_trct_clean <- merge(B3_2021_trct_cat,B3_2021_trct_int, by="KEY",all.x = TRUE,all.y = TRUE)
#******************************************************************************************************************************
#B3 2021 TF RCT Mid
b3_2021_mid_tf_rct <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch3_Transform RCT/2.Mid/06_Clean Data/PH_FY21_B3_TransformRCT_Mid_de_dupli_consent_only_data.csv")

B3_2021_trct_mid_var <- select_available(b3_2021_mid_tf_rct)
B3_2021_trct_mid_dups <- duplicated_data_2(B3_2021_trct_mid_var)
B3_2021_trct_mid_var <- data_needed(B3_2021_trct_mid_dups,B3_2021_trct_mid_var,"post","2021")
B3_2021_trct_mid_var <- B3_2021_trct_mid_var %>% data.table()
B3_2021_trct_mid_var$program_name <- "Transform RCT" #adding program name for the function edit_base_level() to run since the column is required
B3_2021_trct_mid_var <- edit_base_level(B3_2021_trct_mid_var)
B3_2021_trct_mid_var <- B3_2021_trct_mid_var %>% select(-c("program_name"))

#Post survey and choices
b3_trct_mid_svy <- read_sheet("https://docs.google.com/spreadsheets/d/12TQ4v8zgrvd9GYsabL9NRsXyi7rrfknMzfQllOwdsxc/edit#gid=2121457295",sheet="survey") %>%  clean_names() %>% clean_names()
b3_trct_mid_choices <- read_sheet("https://docs.google.com/spreadsheets/d/12TQ4v8zgrvd9GYsabL9NRsXyi7rrfknMzfQllOwdsxc/edit#gid=549137032",sheet="choices") %>%  clean_names() %>% clean_names()

#create option list - post survey - post
b3_trct_mid_opt_list <- opt_list(b3_trct_mid_svy,b3_trct_mid_choices)

b3_trct_mid_opt_list <- b3_trct_mid_opt_list %>% data.table()
b3_trct_mid_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b3_trct_mid_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

#Handling Continuous variables - post
B3_2021_trct_mid_int <- clean_integer_cols(B3_2021_trct_mid_var,b3_trct_mid_svy) %>% data.table()


#Handling Categorical variables - post
B3_2021_trct_mid_cat <- clean_categorical_cols(B3_2021_trct_mid_var,b3_trct_mid_opt_list) #this function sometimes can get slow, it can take like 15minutes
B3_2021_trct_mid_cat <- B3_2021_trct_mid_cat %>% select(-c("community"))
# merging categorical & continuous variables together - post
B3_2021_mid_trct_clean <- merge(B3_2021_trct_mid_cat,B3_2021_trct_mid_int, by="KEY",all.x = TRUE,all.y = TRUE)

#******************************************************************************************************************************
#--changing column names
B3_2021_pre_trct_clean <- setnames(B3_2021_pre_trct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
B3_2021_mid_trct_clean <- setnames(B3_2021_mid_trct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
B3_2021_pre_trct_clean_ops <- merge(B3_2021_pre_trct_clean,ops_data,by = "participant.id",all.x = TRUE)
write_csv(B3_2021_pre_trct_clean_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b3_2021_pre_trct_ops.csv')
write_csv(B3_2021_pre_trct_clean_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/Transform RCT pre_post/b3_2021_pre_t-rct_ops.csv')

#post
B3_2021_mid_trct_clean_ops <- merge(B3_2021_mid_trct_clean,ops_data, by="participant.id",all.x = TRUE)
write_csv(B3_2021_mid_trct_clean_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b3_2021_post_trct.csv')
write_csv(B3_2021_mid_trct_clean_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/Transform RCT pre_post/b3_2021_post_t-rct.csv')

# PrePost_Long format
B3_2021_trct_clean_all <- rbind(B3_2021_pre_trct_clean_ops,B3_2021_mid_trct_clean_ops,
      fill = T)
write_csv(B3_2021_trct_clean_all,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Long format/b3_2021_pre_post_trct.csv')
write_csv(B3_2021_trct_clean_all,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/Transform RCT pre_post/b3_2021_pre_post_t-rct.csv')
#------------------------------------------------------------------------------------------------------------------

# Matched/unmatched
B3_2021_prepost_trct_merged <- merge(B3_2021_pre_trct_clean,B3_2021_mid_trct_clean, by = "participant.id",all.x = TRUE,all.y = TRUE)
B3_2021_prepost_trct_merged[B3_2021_prepost_trct_merged$type.x == "pre" & B3_2021_prepost_trct_merged$type.y == "post", prepost_match := "matched"]
B3_2021_prepost_trct_merged[is.na(B3_2021_prepost_trct_merged$prepost_match),prepost_match := "unmatched"]

B3_2021_prepost_trct_merged <- merge(B3_2021_prepost_trct_merged,ops_data,by="participant.id",all.x = TRUE) 
write.csv(B3_2021_prepost_trct_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/B3_2021_prepost_trct_merged.csv')

#matched
B3_2021_trct_prepost_matched <- prepost_match_2(B3_2021_pre_trct_clean,B3_2021_mid_trct_clean)

names(B3_2021_trct_prepost_matched)[names(B3_2021_trct_prepost_matched) == "hhid.x"] <- "hhid"
write.csv(B3_2021_trct_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/matched/B3_2021_trct_prepost_matched.csv') 

B3_2021_trct_prepost_matched <- B3_2021_trct_prepost_matched %>% select(-c("hhid.y"))
B3_2021_trct_prepost_matched_ops <- merge(B3_2021_trct_prepost_matched,ops_data, by = "participant.id") 
write.csv(B3_2021_trct_prepost_matched_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/matched/B3_2021_trct_prepost_matched_ops.csv')
write.csv(B3_2021_trct_prepost_matched_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/Transform RCT pre_post/B3_2021_trct_prepost_matched_ops.csv')





##* Create 2021 B1 codebook
b3_2021_prepost_trct_codebook <- create_codebook(b3_trct_svy,b3_trct_opt_list,B3_2021_trct_var,B3_2021_trct_mid_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b3_2021_prepost_trct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b3_2021_prepost_trct_codebook.csv')
write_csv(b3_2021_prepost_trct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/Transform RCT pre_post/b3_2021_prepost_t-rct_codebook.csv')

#To be used for a combined codebook for all batches   (2021B3 Pre)
b3_2021_pre_trct_codebook <- for_mstcdB(B3_2021_trct_var,"2021B3 Pre trct")
b3_2021_POST_trct_codebook <- for_mstcdB(B3_2021_trct_mid_var,"2021B3 Post trct")






