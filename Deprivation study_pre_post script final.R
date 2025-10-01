library(googlesheets4)
library(tidyverse)
library(janitor)
library(lubridate, warn.conflicts = FALSE)
library(plyr)
library(data.table)
library(plyr)
library(dplyr)
library(sjmisc)
source("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/R script/pre_post functions.R")
##************************************************************************************************************************************************************************************************##
## Operations data from DOMO, Dataset link: https://caremin.domo.com/datasources/0917c8aa-a2a8-4f3a-837c-1f6fc9aa4d24/details/data/table
domo <- rdomo::Domo(client_id="",
                    secret="")

ops_data <- domo$ds_get('0917c8aa-a2a8-4f3a-837c-1f6fc9aa4d24')
names(ops_data)[names(ops_data) == "participant_id"] <- "hhid"
ops_data <- ops_data %>% mutate(dups_check = paste0(hhid,"_",batch_name))
ops_data <- ops_data[!duplicated(ops_data$dups_check),]
##************************************************************************************************************************************************************************************************##


##________________________________----------------------------------------------------------------------------------------------__________________________________________________________________##
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
##________________________________----------------------------------------------------------------------------------------------__________________________________________________________________##



##************************************************************************************************************************************************************************************************##
## B1 2018 PRE (BASELINE)
b1_2018_pre <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B1/2018B1_PRE.csv")
#clean
b1_2018_pre_var <- select_available(b1_2018_pre)
b1_2018_pre_dups <- duplicated_data(b1_2018_pre_var)
b1_2018_pre_var <- data_needed(b1_2018_pre_dups,b1_2018_pre_var,"pre","2018")

b1_2018_pre_var$batch_name <- "FY18-19 B1"
b1_2018_pre_var$program_name <- "Transform Level 1" #Not sure of the program name, setting it to Transform Level 1 (default)
b1_2018_pre_var <- edit_base_level(b1_2018_pre_var)
#Pulling in the survey and the choices
b1_2018_pre_suvy <- read_sheet("https://docs.google.com/spreadsheets/d/1r9cpq7Zuy902oFUXFM1vx0LP6qvwzbi_XNXY2eMmt3Q/edit#gid=551421512",sheet="survey") %>%  clean_names()
b1_2018_pre_choi <- read_sheet("https://docs.google.com/spreadsheets/d/1r9cpq7Zuy902oFUXFM1vx0LP6qvwzbi_XNXY2eMmt3Q/edit#gid=1210400051",sheet="choices") %>%  clean_names()

# create a Choices and labels list
b1_2018_pre_opt_list <- opt_list(b1_2018_pre_suvy,b1_2018_pre_choi)

b1_2018_pre_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2018_pre_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

b1_2018_pre_var <- b1_2018_pre_var %>% data.table()

#Handling Continuous variables
b1_2018_pre_int <- clean_integer_cols(b1_2018_pre_var,b1_2018_pre_suvy) %>% data.table()# selecting integers and seting 0 and negavites to NULL .. int_cols

#Handling Categorical variables
b1_2018_pre_cat <- clean_categorical_cols(b1_2018_pre_var,b1_2018_pre_opt_list) # replace values with labels

# merging categorical & continuous variables together
b1_2018_pre_all <- merge(b1_2018_pre_cat,b1_2018_pre_int, by="KEY",all.x = TRUE,all.y = TRUE) 
b1_2018_pre_all <- b1_2018_pre_all %>% select(-c(program_name))
b1_2018_pre_all <- b1_2018_pre_all %>% data.table()

#-----------------   update column names  ------------------------------------------------------------------
setnames(b1_2018_pre_all,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

### prepost + Ops data
ops_data_fy1819b1 <- ops_data[ops_data$batch_name == "FY18-19 B1",]

b1_2018_pre_ops <- merge(b1_2018_pre_all,ops_data_fy1819b1, by="hhid",all.x = TRUE) 

#save in pre_post
write_csv(b1_2018_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b1_2018_preops.csv')
#save in PrePost_Long format
write_csv(b1_2018_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Long format/b1_2018_pre.csv')

##_Merged
#b1 2018 prepost Merged
b1_2018_pre_clean <- b1_2018_pre_all %>% filter(type == "pre") 
b1_2018_post_clean <- b1_2018_pre_all %>% filter(type == "post") 

b1_2018_prepost_merged <- merge(b1_2018_pre_clean,b1_2018_post_clean, by = "hhid",all.x = TRUE,all.y = TRUE) # 

#Create prepost_match column with matched/unmatched
b1_2018_prepost_merged[b1_2018_prepost_merged$type.x == "pre" & b1_2018_prepost_merged$type.y == "post", prepost_match := "matched"]
b1_2018_prepost_merged[is.na(b1_2018_prepost_merged$prepost_match),prepost_match := "unmatched"]

#merged prepost + ops

b1_2018_prepost_merged <- merge(b1_2018_prepost_merged,ops_data, by="hhid",all.x = TRUE)


write.csv(b1_2018_prepost_merged,file="G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Merged/b1_2018_prepost_merged.csv")

##*****--------------------
##* Create 2018 B1 codebook
b1_2018_pre_codebook <- create_codebook(b1_2018_pre_suvy,b1_2018_pre_opt_list,b1_2018_pre,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b1_2018_pre_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Codebook/b1_2018_pre_codebook.csv')

#To be used for a combined codebook for all batches   (2023B1 Pre)
b1_2018_codebook <- for_mstcdB(b1_2018_pre,"2018B1 Pre")
##************************************************************************************************************************************************************************************************##
#rm(b1_2018_codebook,b1_2018_pre_var,b1_2018_pre_dups,ops_data_fy1819b1,b1_2018_pre_clean,b1_2018_post_clean,b1_2018_pre_cat,b1_2018_pre_int)


############################################################################################################################################################################################################################################
#FY201819 Batch 2
b2_2018_pre <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B2/2018B2_PRE.csv")
 
b2_2018_pre_var <- select_available(b2_2018_pre)
b2_2018_pre_dups <- duplicated_data(b2_2018_pre_var)
b2_2018_pre_var <- data_needed(b2_2018_pre_dups,b2_2018_pre_var,"pre","2018")
b2_2018_pre_var$program_name <- "Transform Level 1"
b2_2018_pre_var <- mutate(b2_2018_pre_var,name_id = paste0(name_icm,"_",community))

## B2 2018 Post (BASELINE)
b2_2018_post_tf1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B2/2018B2_POST_L1.csv")
 
b2_2018_post_tf1 <- b2_2018_post_tf1 %>% mutate(name_id = paste0(name_icm,"_",community))
b2_2018_post_tf1 <- b2_2018_post_tf1 %>% filter(status == 1 || status == 2) %>% filter(consent_a == 1)
b2_2018_post_tf1_dups <- b2_2018_post_tf1 %>% get_dupes(name_id) %>% select(KEY,start_time,community,base,status,consent_a,name_id,participant_id)
b2_2018_post_tf1_var <- select_available(b2_2018_post_tf1)

b2_2018_post_tf1_var$program_name <- "Transform Level 1"
b2_2018_post_tf1_var <- data_needed(b2_2018_post_tf1_dups,b2_2018_post_tf1_var,"post","2018")
#TF2
b2_2018_post_tf2 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B2/2018B2_POST_L2.csv") 
 
b2_2018_post_tf2_var <- b2_2018_post_tf2 %>% mutate(name_id = paste0(name_icm,"_",community))
b2_2018_post_tf2_var <- b2_2018_post_tf2_var %>% filter(status == 1 || status == 2) %>% filter(consent_a == 1)
b2_2018_post_tf2_dups <- b2_2018_post_tf2_var %>% get_dupes(name_id) %>% select(KEY,start_time,community,base,status,consent_a,name_id,participant_id)
b2_2018_post_tf2_var <- select_available(b2_2018_post_tf2_var)

b2_2018_post_tf2_var$program_name <- "Transform Level 2"
b2_2018_post_tf2_var <- data_needed(b2_2018_post_tf2_dups,b2_2018_post_tf2_var,"post","2018")
#combining pre, post
b2_2018_post_var <- bind_rows(b2_2018_post_tf1_var,b2_2018_post_tf2_var)
b2_2018_pre_post <- bind_rows(b2_2018_pre_var,b2_2018_post_var)

b2_2018_pre_post$batch_name <- "FY18-19 B2"

#Update base names and program names
b2_2018_pre_post <- edit_base_level(b2_2018_pre_post)

#survey and choices
b2_2018_srvy <- read_sheet("https://docs.google.com/spreadsheets/d/16pAI87fDu_9fWrdeSGN1PhhrcBDg0GWJnFkVvzSIHu0/edit#gid=653315037",sheet="survey") %>%  clean_names()
b2_2018_choices <- read_sheet("https://docs.google.com/spreadsheets/d/16pAI87fDu_9fWrdeSGN1PhhrcBDg0GWJnFkVvzSIHu0/edit#gid=1885337514",sheet="choices") %>%  clean_names()
b2_2018_prepost_opt_list <- opt_list(b2_2018_srvy,b2_2018_choices)

b2_2018_prepost_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b2_2018_prepost_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

#Continuous variables
b2_2018_pre_post_int <- clean_integer_cols(b2_2018_pre_post,b2_2018_srvy) %>% data.table()
b2_2018_pre_post_int <- b2_2018_pre_post_int %>% data.table()

#Categorical variables

b2_2018_pre_post <- b2_2018_pre_post %>% data.table()
b2_2018_prepost_cols_cat <- clean_categorical_cols(b2_2018_pre_post,b2_2018_prepost_opt_list) # replace values with labels

# integer variable + categorical variables
b2_2018_prepost_clean <- merge(b2_2018_prepost_cols_cat,b2_2018_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)


# update column names  
setnames(b2_2018_prepost_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#------------------------------------------------------------------------------------------------------------
# pre + ops
b2_2018_pre_clean <- b2_2018_prepost_clean %>% filter(type == "pre")
b2_2018_pre_ops <- merge(b2_2018_pre_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b2_2018_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b2_2018_pre_ops.csv')
#post + ops
b2_2018_post_clean <- b2_2018_prepost_clean %>% filter(type == "post")
b2_2018_post_ops <- merge(b2_2018_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b2_2018_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b2_2018_post_ops.csv')
#prepost + ops
b2_2018_prepost_ops <- merge(b2_2018_prepost_clean,ops_data, by="hhid",all.x = TRUE) 
write_csv(b2_2018_prepost_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Long format/b2_2018_prepost.csv')

# --------------------------------------------------------------------------------------------------------------
#1. batch 2 2018 prepost Matched/unmatched
b2_2018_pre_clean <- b2_2018_pre_clean[!is.na(b2_2018_pre_clean$name_id),]
b2_2018_prepost_merged <- merge(b2_2018_pre_clean,b2_2018_post_clean, by = "name_id",all.x = TRUE,all.y = TRUE) # 

b2_2018_prepost_merged[b2_2018_prepost_merged$type.x == "pre" & b2_2018_prepost_merged$type.y == "post", prepost_match := "matched"]
b2_2018_prepost_merged[is.na(b2_2018_prepost_merged$prepost_match),prepost_match := "unmatched"]

#Matched/unmatched prepost + ops
ops_2018B2 <- ops_data %>% filter(batch_name == "FY18-19 B2") %>% mutate(hhid.x = hhid) #adding column hhid.x to match column in b2_2018_prepost_merged
b2_2018_prepost_merged <- b2_2018_prepost_merged[!is.na(b2_2018_prepost_merged$hhid.x),] #removing rows without hhids since they cause duplication after merging
b2_2018_prepost_merged <- merge(b2_2018_prepost_merged,ops_2018B2, by="hhid.x",all.x = TRUE)

write.csv(b2_2018_prepost_merged,file="G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Merged/b2_2018_prepost_merged.csv")

# 2. batch 2 2018 prepost Matched
b2_2018_prepost_matched <- prepost_match(b2_2018_pre_clean,b2_2018_post_clean)
b2_2018_prepostops_matched <- merge(b2_2018_prepost_matched,ops_2018B2,by="hhid.x")
write.csv(b2_2018_prepostops_matched,file="G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Merged/matched/b2_2018_prepostops_matched.csv")

##* Create 2018 B1 codebook
b2_2018_prepost_codebook <- create_codebook(b2_2018_srvy,b2_2018_prepost_opt_list,b2_2018_pre_var,b2_2018_post_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b2_2018_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Codebook/b2_2018_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2023B1 Pre)
b2_2018_pre_codebook <- for_mstcdB(b2_2018_pre_var,"2018B2 Pre")
b2_2018_post_codebook <- for_mstcdB(b2_2018_post_var,"2018B2 Post")
#******************************************************************************************************************************************************************************************************************************
#rm(b2_2018_pre_clean,b2_2018_post_clean,b2_2018_prepost_codebook,b2_2018_prepost_cols_cat,b2_2018_pre_post_int,b2_2018_pre_dups,b2_2018_post_tf1_dups,b2_2018_post_tf2_dups,b2_2018_pre_post,b2_2018_prepostops_matched,b2_2018_post_tf1,b2_2018_post_tf1_var,b2_2018_post_tf2_var)


#####################################################################################################################
#Batch 3 2018 pre
b3_2018_pre_tf1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B3/2018B3_PRE_L1.csv") 
b3_2018_pre_tf2 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B3/2018B3_PRE_L2.csv") 

#--TF1 pre
b3_2018_pre_tf1_var <- b3_2018_pre_tf1 %>% mutate(name_id = paste0(name_icm,"_",community))
b3_2018_pre_tf1_var <- b3_2018_pre_tf1_var %>% filter(status == 1 || status == 2) %>% filter(consent_a == 1)
b3_2018_pre_tf1_dups <- b3_2018_pre_tf1_var %>% get_dupes(name_id) %>% select(KEY,start_time,community,base,status,consent_a,name_id,participant_id)
b3_2018_pre_tf1_var <- select_available(b3_2018_pre_tf1_var)
b3_2018_pre_tf1_var <- data_needed_name_id(b3_2018_pre_tf1_dups,b3_2018_pre_tf1_var,"pre","2018")
b3_2018_pre_tf1_var$program_name <- "Transform Level 1"

#--TF2 pre
b3_2018_pre_tf2_var <- b3_2018_pre_tf2 %>% mutate(name_id = paste0(name_icm,"_",community))
b3_2018_pre_tf2_var <- b3_2018_pre_tf2_var %>% filter(status == 1 || status == 2) %>% filter(consent_a == 1)
b3_2018_pre_tf2_dups <- b3_2018_pre_tf2_var %>% get_dupes(name_id) %>% select(KEY,start_time,community,base,status,consent_a,name_id,participant_id)
b3_2018_pre_tf2_var <- select_available(b3_2018_pre_tf2_var)
b3_2018_pre_tf2_var <- data_needed_name_id(b3_2018_pre_tf2_dups,b3_2018_pre_tf2_var,"pre","2018")
b3_2018_pre_tf2_var$program_name <- "Transform Level 2"
#-- TF1 + TF2
b3_2018_pre <- bind_rows(b3_2018_pre_tf1_var,b3_2018_pre_tf2_var)

#--TF1 post
b3_2018_post_tf1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B3/2018B3_POST_L1.csv")  
b3_2018_post_tf1_var <- select_available(b3_2018_post_tf1)
b3_2018_post_tf1_var <- b3_2018_post_tf1_var %>% mutate(name_id = paste0(name_icm,"_",community))
b3_2018_post_tf1_dups <- b3_2018_post_tf1_var %>% get_dupes(name_id) %>% select(KEY,start_time,community,base,status,consent_a,name_id,participant_id)
b3_2018_post_tf1_var <- data_needed_name_id(b3_2018_post_tf1_dups,b3_2018_post_tf1_var,"post","2018")
b3_2018_post_tf1_var$program_name <- "Transform Level 1"

#--TF1 post
b3_2018_post_tf2 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B3/2018B3_POST_L2.csv") 
b3_2018_post_tf2_var <- select_available(b3_2018_post_tf2)
b3_2018_post_tf2_var <- b3_2018_post_tf2_var %>% mutate(name_id = paste0(name_icm,"_",community))
b3_2018_post_tf2_dups <- b3_2018_post_tf2_var %>% get_dupes(name_id) %>% select(KEY,start_time,community,base,status,consent_a,name_id,participant_id)
b3_2018_post_tf2_var <- data_needed_name_id(b3_2018_post_tf2_dups,b3_2018_post_tf2_var,"post","2018")
b3_2018_post_tf2_var$program_name <- "Transform Level 2"

#-- TF1 + TF2
b3_2018_post <- bind_rows(b3_2018_post_tf1_var,b3_2018_post_tf2_var) %>% data.table()

#-- pre + post
b3_2018_pre_post <- bind_rows(b3_2018_pre,b3_2018_post) %>% data.table()
b3_2018_pre_post$batch_name <- "FY18-19 B3"
b3_2018_pre_post <- edit_base_level(b3_2018_pre_post)

b3_2018_svy <- read_sheet("https://docs.google.com/spreadsheets/d/14S7HPXe0givfU7V6eFVtBZNsZMcwmaFE4h6c_Tn6x-M/edit#gid=116594203",sheet="survey") %>%  clean_names()
b3_2018_choices <- read_sheet("https://docs.google.com/spreadsheets/d/14S7HPXe0givfU7V6eFVtBZNsZMcwmaFE4h6c_Tn6x-M/edit#gid=409870170",sheet="choices") %>%  clean_names()

#Create an option list
b3_2018_prepost_opt_list <- opt_list(b3_2018_svy,b3_2018_choices)

b3_2018_prepost_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b3_2018_prepost_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

# Handling Continuous variables
b3_2018_pre_post_int <- clean_integer_cols(b3_2018_pre_post,b3_2018_svy) %>% data.table()

# Handling Categorical variables
b3_2018_pre_post_cat <- clean_categorical_cols(b3_2018_pre_post,b3_2018_prepost_opt_list) # replace values with labels

# merging categorical & continuous variables together
b3_2018_pre_post_clean <- merge(b3_2018_pre_post_cat,b3_2018_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)
#--changing column names

b3_2018_pre_post_clean <- setnames(b3_2018_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
#-----------------------------------------------------------------------------------------------------------------
# pre + ops
b3_2018_pre_clean <- b3_2018_pre_post_clean %>% filter(type == "pre")
ops_data_2018B3 <- ops_data %>% filter(batch_name == "FY18-19 B3")
b3_2018_pre_ops <- merge(b3_2018_pre_clean,ops_data_2018B3, by="hhid",all.x = TRUE)
write_csv(b3_2018_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b3_2018_pre_ops.csv')
#post + ops
b3_2018_post_clean <- b3_2018_pre_post_clean %>% filter(type == "post")
b3_2018_post_ops <- merge(b3_2018_post_clean,ops_data_2018B3, by="hhid",all.x = TRUE)
write_csv(b3_2018_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b3_2018_post_ops.csv')

#prepost + ops
b3_2018_pre_post_ops <- merge(b3_2018_pre_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b3_2018_pre_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Long format/b3_2018_pre_post.csv')
##### ----------------  Matched ------------------------------------------------------------------------------------------
#1.1 With no participants ids
b3_2018_pre_clean_no_id <- b3_2018_pre_clean[is.na(b3_2018_pre_clean$hhid),]
b3_2018_post_clean_no_id <- b3_2018_post_clean[is.na(b3_2018_post_clean$hhid),]
b3_2018_prepost_merged_no_id <- merge(b3_2018_pre_clean_no_id,b3_2018_post_clean_no_id, by = "name_id",all.x = TRUE,all.y = TRUE)
#***
b3_2018_prepost_merged_no_id[b3_2018_prepost_merged_no_id$type.x == "pre" & b3_2018_prepost_merged_no_id$type.y == "post", prepost_match := "matched"]
b3_2018_prepost_merged_no_id[is.na(b3_2018_prepost_merged_no_id$prepost_match),prepost_match := "unmatched"]
b3_2018_prepost_merged_no_id <- b3_2018_prepost_merged_no_id %>% select(-c("hhid.y"))
names(b3_2018_prepost_merged_no_id)[names(b3_2018_prepost_merged_no_id) == "hhid.x"] <- "hhid"

#1.2 With participants ids
b3_2018_pre_clean_id <- b3_2018_pre_clean[!is.na(b3_2018_pre_clean$hhid),]
b3_2018_post_clean_id <- b3_2018_post_clean[!is.na(b3_2018_post_clean$hhid),]

b3_2018_prepost_merged_id <- merge(b3_2018_pre_clean_id,b3_2018_post_clean_id, by = "hhid",all.x = TRUE,all.y = TRUE)
b3_2018_prepost_merged_id[b3_2018_prepost_merged_id$type.x == "pre" & b3_2018_prepost_merged_id$type.y == "post", prepost_match := "matched"]
b3_2018_prepost_merged_id[is.na(b3_2018_prepost_merged_id$prepost_match),prepost_match := "unmatched"]
# With no participants ids + With participants ids
b3_2018_prepost_merged <- bind_rows(b3_2018_prepost_merged_no_id,b3_2018_prepost_merged_id)

# merged + ops
b3_2018_prepost_merged <- merge(b3_2018_prepost_merged,ops_data,by="hhid",all.x = TRUE)

#b3_2018_prepost_merged
write.csv(b3_2018_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Merged/b3_2018_prepost_merged.csv')

# 2. batch 3 2018 prepost Matched
b3_2018_prepost_matched <- prepost_match2(b3_2018_pre_clean,b3_2018_post_clean) %>% mutate(hhid = hhid.x)
b3_2018_prepostops_matched <- merge(b3_2018_prepost_matched,ops_data,by="hhid")
write.csv(b3_2018_prepostops_matched,file="G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Merged/matched/b3_2018_prepostops_matched.csv")

##* Create 2018 B3 codebook
b3_2018_prepost_codebook <- create_codebook(b3_2018_svy,b3_2018_prepost_opt_list,b3_2018_pre,b3_2018_post,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b3_2018_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Codebook/b3_2018_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2018B3 Pre)
b3_2018_pre_codebook <- for_mstcdB(b3_2018_pre,"2018B3 Pre")
b3_2018_post_codebook <- for_mstcdB(b3_2018_post,"2018B3 Post")
#***********************************************************************************************************************************************************************************************************************************#


# Batch 1 2019 pre --  TF 1
b1_2019_pre_tf1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B1/2019B1_PRE_L1.csv")  

b1_2019_pre_tf1_var <- select_available(b1_2019_pre_tf1)
b1_2019_pre_tf1_dups <- duplicated_data(b1_2019_pre_tf1_var)
b1_2019_pre_tf1_var <- data_needed(b1_2019_pre_tf1_dups,b1_2019_pre_tf1_var,"pre","2019")
b1_2019_pre_tf1_var$program_name <- "Transform Level 1"

#--- TF 2
b1_2019_pre_tf2 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B1/2019B1_PRE_L2.csv") 
    
b1_2019_pre_tf2_var <- select_available(b1_2019_pre_tf2)
b1_2019_pre_tf2_dups <- duplicated_data(b1_2019_pre_tf2_var)
b1_2019_pre_tf2_var <- data_needed(b1_2019_pre_tf2_dups,b1_2019_pre_tf2_var,"pre","2019")
b1_2019_pre_tf2_var$program_name <- "Transform Level 2"
b1_2019_pre_tf2_var$oinc5_oth <- as.character(b1_2019_pre_tf2_var$oinc5_oth)

# TF1 PRE + TF2 PRE
b1_2019_pre <-  bind_rows(b1_2019_pre_tf1_var,b1_2019_pre_tf2_var)
#Batch 1 2019 post
# ---  TF 1
b1_2019_post_tf1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B1/2019B1_POST_L1.csv") 
b1_2019_post_tf1_var <- select_available(b1_2019_post_tf1)
b1_2019_post_tf1_dups <- duplicated_data(b1_2019_post_tf1_var)
b1_2019_post_tf1_var <- data_needed(b1_2019_post_tf1_dups,b1_2019_post_tf1_var,"post","2019")
b1_2019_post_tf1_var$program_name <- "Transform Level 1"

# ---  TF 2
b1_2019_post_tf2 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B1/2019B1_POST_L2.csv")  
b1_2019_post_tf2_var <- select_available(b1_2019_post_tf2)
b1_2019_post_tf2_dups <- duplicated_data(b1_2019_post_tf2_var)
b1_2019_post_tf2_var <- data_needed(b1_2019_post_tf2_dups,b1_2019_post_tf2_var,"post","2019")
b1_2019_post_tf2_var$program_name <- "Transform Level 2"

#TF1 POST + TF2 POST
b1_2019_post <- bind_rows(b1_2019_post_tf1_var,b1_2019_post_tf2_var)
#pre + post
b1_2019_pre_post <- bind_rows(b1_2019_pre,b1_2019_post)
b1_2019_pre_post <- edit_base_level(b1_2019_pre_post)
b1_2019_pre_post <- b1_2019_pre_post %>% data.table()

#-----Reading in the Survey questionnaire and Choices
b1_2019_pre_post_suvy <- read_sheet("https://docs.google.com/spreadsheets/d/1KPWvLBxcQtqNJKf9TdrX9ipQhDlcnmnFR3BOf6tsofU/edit#gid=1529439020",sheet="survey") %>%  clean_names() 
b1_2019_pre_post_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1KPWvLBxcQtqNJKf9TdrX9ipQhDlcnmnFR3BOf6tsofU/edit#gid=1286014469",sheet="choices") %>%  clean_names() 

#create option list
b1_2019_opt_list <- opt_list(b1_2019_pre_post_suvy,b1_2019_pre_post_choices)

b1_2019_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2019_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
b1_2019_opt_list <- b1_2019_opt_list[b1_2019_opt_list$MERGE_NAME != "community",]

#-- Handling Continuous variables
b1_2019_pre_post_suvy <- b1_2019_pre_post_suvy %>% data.table()
b1_2019_pre_post_int <- clean_integer_cols(b1_2019_pre_post,b1_2019_pre_post_suvy) %>% data.table()#integers

#Handling Categorical variables
b1_2019_pre_post_cat <- clean_categorical_cols(b1_2019_pre_post,b1_2019_opt_list)

#merging categorical & continuous variables together
b1_2019_pre_post_clean <- merge(b1_2019_pre_post_cat,b1_2019_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)
b1_2019_pre_post_clean$batch <- "FY19-20 B1"

##--changing column names
b1_2019_pre_post_clean <- setnames(b1_2019_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre + Ops data
b1_2019_pre_clean <- b1_2019_pre_post_clean %>% filter(type == "pre")
b1_2019_pre_ops <- merge(b1_2019_pre_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2019_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b1_2019_pre_ops.csv')

#post + Ops data
b1_2019_post_clean <- b1_2019_pre_post_clean %>% filter(type == "post")
b1_2019_post_ops <- merge(b1_2019_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2019_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b1_2019_post_ops.csv')

#prepost + ops
b1_2019_pre_post_ops <- merge(b1_2019_pre_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2019_pre_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Long format/b1_2019_pre_post.csv')
#-----------------------------------------------------------------------------------------------------------------
##Matched/unmatched  b1_2019_prepost_matched
b1_2019_prepost_merged <- merge(b1_2019_pre_clean,b1_2019_post_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b1_2019_prepost_merged[b1_2019_prepost_merged$type.x == "pre" & b1_2019_prepost_merged$type.y == "post", prepost_match := "matched"]
b1_2019_prepost_merged[is.na(b1_2019_prepost_merged$prepost_match),prepost_match := "unmatched"]
# + ops data
b1_2019_prepost_merged <- merge(b1_2019_prepost_merged,ops_data,by="hhid",all.x = TRUE)
write.csv(b1_2019_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/b1_2019_prepost_merged.csv')


##*Matched
b1_2019_prepost_matched <- prepost_match(b1_2019_pre_clean,b1_2019_post_clean)
ops_data$hhid.x <- ops_data$hhid
b1_2019_prepost_matched <- merge(b1_2019_prepost_matched,ops_data, by = "hhid.x")
write.csv(b1_2019_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/matched/b1_2019_prepost_matched.csv')

##* Create 2018 B1 codebook
b1_2019_prepost_codebook <- create_codebook(b1_2019_pre_post_suvy,b1_2019_opt_list,b1_2019_pre,b1_2019_post,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b1_2019_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b1_2019_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2019B1 Pre)
b1_2019_pre_codebook <- for_mstcdB(b1_2019_pre,"2019B1 Pre")
b1_2019_post_codebook <- for_mstcdB(b1_2019_post,"2019B1 Post")
##*************************************************************************************************************************************************************************************************************************************


#****************************************************************************************************************************************************************************************************************************###
#Batch 2 2019 pre TF 1
b2_2019_pre_tf1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B2/OFFICIAL_presurvey_2019_batch2_level1_WIDE.csv") 

b2_2019_pre_tf1_var <- select_available(b2_2019_pre_tf1)
b2_2019_pre_tf1_dups <- duplicated_data(b2_2019_pre_tf1_var)
b2_2019_pre_tf1_var <- data_needed(b2_2019_pre_tf1_dups,b2_2019_pre_tf1_var,"pre","2019")
b2_2019_pre_tf1_var$program_name <- "Transform Level 1"
b2_2019_pre_tf1_var$oinc5_oth <- as.character(b2_2019_pre_tf1_var$oinc5_oth)
#TF2
b2_2019_pre_tf2 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B2/OFFICIAL_presurvey_2019_batch2_level2_WIDE.csv") 

b2_2019_pre_tf2_var <- select_available(b2_2019_pre_tf2)
b2_2019_pre_tf2_dups <- duplicated_data(b2_2019_pre_tf2_var)
b2_2019_pre_tf2_var <- data_needed(b2_2019_pre_tf2_dups,b2_2019_pre_tf2_var,"pre","2019")
b2_2019_pre_tf2_var$program_name <- "Transform Level 2"
b2_2019_pre_tf2_var$oinc5_oth <- as.character(b2_2019_pre_tf2_var$oinc5_oth)
# TF1 + TF2
b2_2019_pre <- bind_rows(b2_2019_pre_tf1_var,b2_2019_pre_tf2_var)
b2_2019_pre$surveyor_id <- as.character(b2_2019_pre$surveyor_id)

#TF1 + TF2 POST
b2_2019_post_tf12 <- read.csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B2/Official_Postsurvey_2019_Batch2_WIDE.csv") 

b2_2019_post_tf12_var <- select_available(b2_2019_post_tf12)
b2_2019_post_tf12_dups <- duplicated_data(b2_2019_post_tf12_var)
b2_2019_post_tf12_var <- data_needed(b2_2019_post_tf12_dups,b2_2019_post_tf12_var,"post","2019") %>% data.table()

#--PRE+POST
b2_2019_pre_post <- bind_rows(b2_2019_pre,b2_2019_post_tf12_var) %>% data.table()
b2_2019_pre_post <- edit_base_level(b2_2019_pre_post)

#survey and choices
b2_2019_post_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1Gl5RDp0qp7E7rfDCfcxD6vjo8Stl-3DlktGL1sgvkS0/edit#gid=1334303542",sheet="survey") %>%  clean_names()
b2_2019__choices <- read_sheet("https://docs.google.com/spreadsheets/d/1Gl5RDp0qp7E7rfDCfcxD6vjo8Stl-3DlktGL1sgvkS0/edit#gid=2032008205",sheet="choices") %>%  clean_names()

#Create option list
b2_2019_opt_list <- opt_list(b2_2019_post_svy,b2_2019__choices)
b2_2019_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b2_2019_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

#Remove duplicated options
b2_2019_opt_list[!is.na(b2_2019_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b2_2019_opt_list <- b2_2019_opt_list[!duplicated(b2_2019_opt_list$dup_check),]

#Handling Continuous variables
b2_2019_pre_post_int <- clean_integer_cols(b2_2019_pre_post,b2_2019_post_svy) #%>% data.table()

#Handling Categorical variables
b2_2019_pre_post_cat <- clean_categorical_cols(b2_2019_pre_post,b2_2019_opt_list)

# merging categorical & continuous variables together
b2_2019_pre_post_clean <- merge(b2_2019_pre_post_cat,b2_2019_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)
b2_2019_pre_post_clean$batch <- "FY19-20 B2"

##--changing column names
b2_2019_pre_post_clean <- setnames(b2_2019_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b2_2019_pre_clean <- b2_2019_pre_post_clean %>% filter(type == "pre")
b2_2019_pre_ops <- merge(b2_2019_pre_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b2_2019_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b2_2019_pre_ops.csv')

#post
b2_2019_post_clean <- b2_2019_pre_post_clean %>% filter(type == "post")
b2_2019_post_ops <- merge(b2_2019_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b2_2019_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b2_2019_post_ops.csv')

#Both matched $ unmatched
b2_2019_pre_post_ops <- merge(b2_2019_pre_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b2_2019_pre_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Long format/b2_2019_pre_post.csv')
#-----------------------------------------------------------------------------------------------------------------
#  Matched
b2_2019_prepost_merged <- merge(b2_2019_pre_clean,b2_2019_post_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b2_2019_prepost_merged[b2_2019_prepost_merged$type.x == "pre" & b2_2019_prepost_merged$type.y == "post", prepost_match := "matched"]
b2_2019_prepost_merged[is.na(b2_2019_prepost_merged$prepost_match),prepost_match := "unmatched"]

b2_2019_prepost_merged <- merge(b2_2019_prepost_merged,ops_data,by="hhid",all.x = TRUE)
# save b2_2019_prepost_merged
write.csv(b2_2019_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/b2_2019_prepost_merged.csv')

##*Matched
b2_2019_prepost_matched <- prepost_match(b2_2019_pre_clean,b2_2019_post_clean)
b2_2019_prepost_matched <- merge(b2_2019_prepost_matched,ops_data, by = "hhid.x")
write.csv(b2_2019_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/matched/b2_2019_prepost_matched.csv')

##* Create 2018 B1 codebook
b2_2019_prepost_codebook <- create_codebook(b2_2019_post_svy,b2_2019_opt_list,b2_2019_pre,b2_2019_post_tf12_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b2_2019_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b2_2019_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2019B1 Pre)
b2_2019_pre_codebook <- for_mstcdB(b2_2019_pre,"2019B2 Pre")
b2_2019_post_codebook <- for_mstcdB(b2_2019_post_tf12_var,"2019B2 Post")
#*************************************************************************************************************************************************************************************************************************************


#####################################################################################################################
#Batch 3 2019 pre
b3_2019_pre <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B3/official_presurvey_2019_batch3_WIDE.csv")    
            
b3_2019_pre_var <- select_available(b3_2019_pre)
b3_2019_pre_dups <- duplicated_data(b3_2019_pre_var)
b3_2019_pre_var <- data_needed(b3_2019_pre_dups,b3_2019_pre_var,"pre","2019")

#Batch 3 2019 post
b3_2019_post <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B3/Official_2019_B3_Post_Survey_WIDE.csv")   
            
b3_2019_post_var <- select_available(b3_2019_post)

b3_2019_post_dups <- duplicated_data(b3_2019_post_var)
b3_2019_post_var <- data_needed(b3_2019_post_dups,b3_2019_post_var,"post","2019")
b3_2019_post_var$a_q2_1 <- as.character(b3_2019_post_var$a_q2_1)
b3_2019_post_var$a_q2_2 <- as.character(b3_2019_post_var$a_q2_2)
b3_2019_post_var$a_q2_3 <- as.character(b3_2019_post_var$a_q2_3)
b3_2019_post_var$a_q2_4 <- as.character(b3_2019_post_var$a_q2_4)
b3_2019_post_var$a_q2_5 <- as.character(b3_2019_post_var$a_q2_5)
b3_2019_post_var$a_q2_6 <- as.character(b3_2019_post_var$a_q2_6)
b3_2019_post_var$a_q2_7 <- as.character(b3_2019_post_var$a_q2_7)
b3_2019_post_var$a_q2_8 <- as.character(b3_2019_post_var$a_q2_8)
b3_2019_post_var$a_q2_9 <- as.character(b3_2019_post_var$a_q2_9)
b3_2019_post_var$a_q2_10 <- as.character(b3_2019_post_var$a_q2_10)
b3_2019_post_var$a_q2_11 <- as.character(b3_2019_post_var$a_q2_11)
b3_2019_post_var$a_q2_12 <- as.character(b3_2019_post_var$a_q2_12)
b3_2019_post_var$a_q2_13 <- as.character(b3_2019_post_var$a_q2_13)

# pre+ post
b3_2019_pre_post <- bind_rows(b3_2019_pre_var,b3_2019_post_var)
b3_2019_pre_post <- edit_base_level(b3_2019_pre_post)
# survey and choices
b3_2019_pre_post_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1r5leAPD9Pev4IP6eGJUlSIyw3IxozPEWiw50KJlSi-I/edit#gid=0",sheet="survey") %>%  clean_names()
b3_2019_pre_post_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1r5leAPD9Pev4IP6eGJUlSIyw3IxozPEWiw50KJlSi-I/edit#gid=1509898419",sheet="choices") %>%  clean_names()

#create option list to update the prepost categorical variable from
b3_2019_opt_list <- opt_list(b3_2019_pre_post_svy,b3_2019_pre_post_choices)

b3_2019_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b3_2019_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
#Remove duplicated options
b3_2019_opt_list[!is.na(b3_2019_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b3_2019_opt_list <- b3_2019_opt_list[!duplicated(b3_2019_opt_list$dup_check),]

#Handling Continuous variables
b3_2019_pre_post_int <- clean_integer_cols(b3_2019_pre_post,b3_2019_pre_post_svy) %>% data.table()

#Handling Categorical variables
b3_2019_pre_post_cat <- clean_categorical_cols(b3_2019_pre_post,b3_2019_opt_list)

# merging categorical & continuous variables together
b3_2019_pre_post_clean <- merge(b3_2019_pre_post_cat,b3_2019_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)
b3_2019_pre_post_clean$batch <- "FY19-20 B3"

##--changing column names
b3_2019_pre_post_clean <- setnames(b3_2019_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b3_2019_pre_clean <- b3_2019_pre_post_clean %>% filter(type == "pre")
b3_2019_pre_ops <- merge(b3_2019_pre_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b3_2019_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b3_2019_pre_ops.csv')

#post
b3_2019_post_clean <- b3_2019_pre_post_clean %>% filter(type == "post")
b3_2019_post_ops <- merge(b3_2019_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b3_2019_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b3_2019_post_ops.csv')

#prepost
b3_2019_pre_post_ops <- merge(b3_2019_pre_post_clean,ops_data, by="hhid",all.x = TRUE)

write_csv(b3_2019_pre_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Long format/b3_2019_pre_post.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched
b3_2019_prepost_merged <- merge(b3_2019_pre_clean,b3_2019_post_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b3_2019_prepost_merged[b3_2019_prepost_merged$type.x == "pre" & b3_2019_prepost_merged$type.y == "post", prepost_match := "matched"]
b3_2019_prepost_merged[is.na(b3_2019_prepost_merged$prepost_match),prepost_match := "unmatched"]

b3_2019_prepost_merged <- merge(b3_2019_prepost_merged,ops_data,by="hhid",all.x = TRUE)
# save b3_2019_prepost_merged
write.csv(b3_2019_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/b3_2019_prepost_merged.csv')

#matched
b3_2019_prepost_matched <- prepost_match(b3_2019_pre_clean,b3_2019_post_clean)
b3_2019_prepost_matched <- merge(b3_2019_prepost_matched,ops_data, by = "hhid.x")
write.csv(b3_2019_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/matched/b3_2019_prepost_matched.csv')

##* Create 2018 B1 codebook
b3_2019_prepost_codebook <- create_codebook(b3_2019_pre_post_svy,b3_2019_opt_list,b3_2019_pre_var,b3_2019_post_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b3_2019_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b3_2019_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2019B3 Pre)
b3_2019_pre_codebook <- for_mstcdB(b3_2019_pre_var,"2019B3 Pre")
b3_2019_post_codebook <- for_mstcdB(b3_2019_post_var,"2019B3 Post")
#*************************************************************************************************************************************************************************************************************************************


#**************************************************************************************************************************************************************************************************************************************
##B1 2020 pre
b1_2020_pre <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/12.FY202021/2Rs/1.Pre/06_Clean Data/Official/de_dupli_consent_only_data.csv") 

names(b1_2020_pre)[names(b1_2020_pre) == "ops_2r_program"] <- "program_name" #
b1_2020_pre_var <- select_available(b1_2020_pre)
b1_2020_pre_var <- b1_2020_pre_var %>% mutate(name_id = paste0(name_icm,"_",community))
b1_2020_pre_var_dups <- b1_2020_pre_var %>% get_dupes(name_id) %>% select(KEY,start_time,community,base,status,consent_a,name_id)# No duplicate combinations found of: name_id
b1_2020_pre_var$type <- "pre"
b1_2020_pre_var$year <- "2020"
b1_2020_pre_var$calc_field1 <- as.character(b1_2020_pre_var$calc_field1)
b1_2020_pre_var$healthcare_participant_care_cost <- as.double(b1_2020_pre_var$healthcare_participant_care_cost)
b1_2020_pre_var$healthcare_hh_test_payment <- as.character(b1_2020_pre_var$healthcare_hh_test_payment)
b1_2020_pre_var$healthcare_hh_hospital_resources <- as.character(b1_2020_pre_var$healthcare_hh_hospital_resources)
b1_2020_pre_var <- edit_base_level(b1_2020_pre_var)
##B1 2020 post
b1_2020_post <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/12.FY202021/2Rs/2.Post/06_Clean Data/de_dupli_consent_only_data.csv") 

names(b1_2020_post)[names(b1_2020_post) == "ops_2r_program"] <- "program_name" 
b1_2020_post_var <- select_available(b1_2020_post)
b1_2020_post_var <- b1_2020_post_var %>% mutate(name_id = paste0(name_icm,"_",participant_id))
b1_2020_post_var_dups <- b1_2020_post_var %>% get_dupes(name_id) %>% select(KEY,start_time,community,base,status,consent_a,name_id)#No duplicate combinations found of: name_id
b1_2020_post_var <- b1_2020_post_var %>% data.table()
b1_2020_post_var$type <- "post"
b1_2020_post_var$year <- "2020"
b1_2020_post_var$calc_pre_a_q2_1 <- as.character(b1_2020_post_var$calc_pre_a_q2_1)
b1_2020_post_var$healthcare_hh_care_cost <- as.character(b1_2020_post_var$healthcare_hh_care_cost)
b1_2020_post_var$extra_support_hh_social_provider <- as.character(b1_2020_post_var$extra_support_hh_social_provider)
b1_2020_post_var$calc_pre_a_q2_head <- as.character(b1_2020_post_var$calc_pre_a_q2_head)
b1_2020_post_var$calc_pre_a_q2_resp <- as.character(b1_2020_post_var$calc_pre_a_q2_resp)
b1_2020_post_var$calc_pre_a_q2_respir_symp <- as.character(b1_2020_post_var$calc_pre_a_q2_respir_symp)
b1_2020_post_var <- edit_base_level(b1_2020_post_var)
# Pre + Post
b1_2020_pre_post <- bind_rows(b1_2020_pre_var,b1_2020_post_var)

# Pulling in the survey and choices
b1_2020_svy <- read_sheet("https://docs.google.com/spreadsheets/d/15I85iZDF7Z1BG_uQO4ywgAnuh0K_PRUyIu-8e4_BJVo/edit#gid=2121457295",sheet="survey") %>%  clean_names() 
b1_2020_choices <- read_sheet("https://docs.google.com/spreadsheets/d/15I85iZDF7Z1BG_uQO4ywgAnuh0K_PRUyIu-8e4_BJVo/edit#gid=549137032",sheet="choices") %>%  clean_names() 

#create option list
b1_2020_opt_list <- opt_list(b1_2020_svy,b1_2020_choices)

b1_2020_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2020_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
#Remove duplicated options
b1_2020_opt_list[!is.na(b1_2020_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b1_2020_opt_list <- b1_2020_opt_list[!duplicated(b1_2020_opt_list$dup_check),]

#Handling Continuous variables
b1_2020_pre_post_int <- clean_integer_cols(b1_2020_pre_post,b1_2020_svy) %>% data.table()

#Handling Categorical variables
b1_2020_pre_post_cat <- clean_categorical_cols(b1_2020_pre_post,b1_2020_opt_list)

#Categorical + Continuous variables
b1_2020_pre_post_clean <- merge(b1_2020_pre_post_cat,b1_2020_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)

#--changing column names
b1_2020_pre_post_clean <- setnames(b1_2020_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
## Ops data
#pre
b1_2020_pre_clean <- b1_2020_pre_post_clean %>% filter(type == "pre")
write_csv(b1_2020_pre_clean,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/pre_post/b1_2020_pre_clean.csv')
#post
b1_2020_post_clean <- b1_2020_pre_post_clean %>% filter(type == "post")
write_csv(b1_2020_post_clean,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/pre_post/b1_2020_post_clean.csv')

#prepost
write_csv(b1_2020_pre_post_clean,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Long format/b_2020_pre_post.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched
b1_2020_prepost_merged <- merge(b1_2020_pre_clean,b1_2020_post_clean, by = "name.icm",all.x = TRUE,all.y = TRUE) 
b1_2020_prepost_merged[b1_2020_prepost_merged$type.x == "pre" & b1_2020_prepost_merged$type.y == "post", prepost_match := "matched"]
b1_2020_prepost_merged[is.na(b1_2020_prepost_merged$prepost_match),prepost_match := "unmatched"]

#b1_2020_prepost_merged
write.csv(b1_2020_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Merged/b1_2020_prepost_merged.csv')

#matched
b1_2020_prepost_matched <- prepost_match2(b1_2020_pre_clean,b1_2020_post_clean)
write.csv(b1_2020_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Merged/matched/b1_2020_prepost_matched.csv')

##* Create 2018 B1 codebook
b123_2020_prepost_codebook <- create_codebook(b1_2020_svy,b1_2020_opt_list,b1_2020_pre_var,b1_2020_post_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b123_2020_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Codebook/b123_2020_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2020B1 Pre)
b1_2020_pre_codebook <- for_mstcdB(b1_2020_pre_var,"2020B1 Pre")
b1_2020_post_codebook <- for_mstcdB(b1_2020_post_var,"2020B1 Post")
#****************************************************************************************************************************************************************************************


#########################################################################################################################
# B1 2021 pre stretch
b1_2021_pre_strech_v1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch1_Transform Stretch V1/1.Pre/06_Clean Data/PH_FY21_B1_Stretch_Presurvey_de_dupli_consent_only_data.csv") 

b1_2021_pre_strech_v1_var <- select_available(b1_2021_pre_strech_v1)
b1_2021_pre_strech_v1_dups <- duplicated_data(b1_2021_pre_strech_v1_var)
b1_2021_pre_strech_v1_var <- data_needed(b1_2021_pre_strech_v1_dups,b1_2021_pre_strech_v1_var,"pre","2021")
b1_2021_pre_strech_v1_var$calc_field1 <- as.character(b1_2021_pre_strech_v1_var$calc_field1)
b1_2021_pre_strech_v1_var$j_q16_modes <- as.character(b1_2021_pre_strech_v1_var$j_q16_modes)

##B1 2021 post stretch
b1_2021_post_strech_v1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch1_Transform Stretch V1/3.Post/06_Clean Data/PH_FY21_B1_Stretch_V1_Postsurvey_de_dupli_consent_only_data.csv")    

b1_2021_post_strech_v1_var <- select_available(b1_2021_post_strech_v1)
b1_2021_post_strech_v1_dups <- duplicated_data(b1_2021_post_strech_v1_var)
b1_2021_post_strech_v1_var <- data_needed(b1_2021_post_strech_v1_dups,b1_2021_post_strech_v1_var,"post","2021")
b1_2021_post_strech_v1_var$name_icm_regpart_pos <- as.character(b1_2021_post_strech_v1_var$name_icm_regpart_pos)
b1_2021_post_strech_v1_var$surveyor_id <- as.character(b1_2021_post_strech_v1_var$surveyor_id)
b1_2021_post_strech_v1_var$surveyor_id_check <- as.character(b1_2021_post_strech_v1_var$surveyor_id_check)

# stretch pre+post
b1_2021_prepost_strech_v1 <- bind_rows(b1_2021_pre_strech_v1_var,b1_2021_post_strech_v1_var)
b1_2021_prepost_strech_v1 <- edit_base_level(b1_2021_prepost_strech_v1)
#survey and choices
b1_2021_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1t7hZxqCDtqTNzXNcdFvruImcwb_ZpvtZ2ZdtI7XacQ0/edit#gid=2121457295",sheet="survey") %>%  clean_names()
b1_2021_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1t7hZxqCDtqTNzXNcdFvruImcwb_ZpvtZ2ZdtI7XacQ0/edit#gid=549137032",sheet="choices") %>%  clean_names()

#Create option list
b1_2021_tfs_opt_list <- opt_list(b1_2021_svy,b1_2021_choices)

b1_2021_tfs_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2021_tfs_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
#Remove duplicated options
b1_2021_tfs_opt_list[!is.na(b1_2021_tfs_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b1_2021_tfs_opt_list <- b1_2021_tfs_opt_list[!duplicated(b1_2021_tfs_opt_list$dup_check),]

#Handling Continuous variables
b1_2021_prepost_strech_v1_int <- clean_integer_cols(b1_2021_prepost_strech_v1,b1_2021_svy) %>% data.table()

#Handling Categorical variables
b1_2021_prepost_strech_v1 <- b1_2021_prepost_strech_v1 %>% data.table()
b1_2021_strechv1_cat <- clean_categorical_cols2(b1_2021_prepost_strech_v1,b1_2021_tfs_opt_list) #using function clean_categorical_cols2 , clean_categorical_cols doesn't work for unknown reasons

# merging categorical & continuous variables together
b1_2021_strechv1_cat <- b1_2021_strechv1_cat %>% select(-c(sys_participant_id))
b1_2021_strech_v1_clean <- merge(b1_2021_strechv1_cat,b1_2021_prepost_strech_v1_int, by="KEY",all.x = TRUE,all.y = TRUE)

b1_2021_strech_v1_clean <- b1_2021_strech_v1_clean %>% select(-c(sys_participant_id))
#--changing column names
b1_2021_strech_v1_clean <- setnames(b1_2021_strech_v1_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b1_2021_pre_tsv1_clean <- b1_2021_strech_v1_clean %>% filter(type == "pre")
b1_2021_pre_tsv1_ops <- merge(b1_2021_pre_tsv1_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b1_2021_pre_tsv1_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b1_2021_pre_tsv1_ops.csv')

#post
b1_2021_post_tsv1_clean <- b1_2021_strech_v1_clean %>% filter(type == "post")
b1_2021_post_tsv1_ops <- merge(b1_2021_post_tsv1_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b1_2021_post_tsv1_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b1_2021_post_tsv1_ops.csv')

#prepost
b1_2021_strech_v1_prepost_ops <- merge(b1_2021_strech_v1_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2021_strech_v1_prepost_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Long format/strech_v1_pre_post.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched
b1_2021_prepost_tfsv1_merged <- merge(b1_2021_pre_tsv1_clean,b1_2021_post_tsv1_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b1_2021_prepost_tfsv1_merged[b1_2021_prepost_tfsv1_merged$type.x == "pre" & b1_2021_prepost_tfsv1_merged$type.y == "post", prepost_match := "matched"]
b1_2021_prepost_tfsv1_merged[is.na(b1_2021_prepost_tfsv1_merged$prepost_match),prepost_match := "unmatched"]

b1_2021_prepost_tfsv1_merged <- merge(b1_2021_prepost_tfsv1_merged,ops_data,by="hhid",all.x = TRUE)
#b1_2021_prepost_tfsv1_merged
write.csv(b1_2021_prepost_tfsv1_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/b1_2021_prepost_tfsv1_merged.csv')

#matched
b1_2021_tsv1_prepost_matched <- prepost_match(b1_2021_pre_tsv1_clean,b1_2021_post_tsv1_clean)
b1_2021_tsv1_prepost_matched <- merge(b1_2021_tsv1_prepost_matched,ops_data, by = "hhid.x")
write.csv(b1_2021_tsv1_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/matched/b1_2021_tsv1_prepost_matched.csv')

##* Create 2018 B1 codebook
b1_2021_prepost_tfsv1_codebook <- create_codebook(b1_2021_svy,b1_2021_tfs_opt_list,b1_2021_pre_strech_v1_var,b1_2021_post_strech_v1_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b1_2021_prepost_tfsv1_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b1_2021_prepost_tfsv1_codebook.csv')

#To be used for a combined codebook for all batches   (2021B1 Pre)
b1_2021_pre_tsv1_codebook <- for_mstcdB(b1_2021_pre_strech_v1_var,"2021B1 Pre tsv1")
b1_2021_post_tsv1_codebook <- for_mstcdB(b1_2021_post_strech_v1_var,"2021B1 Post tsv1")
#****************************************************************************************************************************************************************************************


########################################################################################################################
# B2 2021 pre D-RCT
b2_2021_pre_drct <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch2_Documentary RCT/1.Pre/06_Clean Data/PH_FY21_B2_Presurvey_de_dupli_consent_only_data.csv") 

b2_2021_pre_drct_var <- select_available(b2_2021_pre_drct)
b2_2021_pre_drct_dups <- duplicated_data(b2_2021_pre_drct_var)
b2_2021_pre_drct_var <- data_needed(b2_2021_pre_drct_dups,b2_2021_pre_drct_var,"pre","2021")
b2_2021_pre_drct_var$preload_date <- as.character(b2_2021_pre_drct_var$preload_date)
b2_2021_pre_drct_var$calc_field1 <- as.character(b2_2021_pre_drct_var$calc_field1)

##B2 2021 post D-RCT
b2_2021_post_drct <-  read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch2_Documentary RCT/2.Post/06_Clean Data/PH_FY21B2_Postsurvey_de_dupli_consent_only_data.csv") 
b2_2021_post_drct_var <- select_available(b2_2021_post_drct)
b2_2021_post_drct_dups <- duplicated_data(b2_2021_post_drct_var)
b2_2021_post_drct_var <- data_needed(b2_2021_post_drct_dups,b2_2021_post_drct_var,"post","2021")
b2_2021_post_drct_var$name_icm_regpart_pos <- as.character(b2_2021_post_drct_var$name_icm_regpart_pos)
b2_2021_post_drct_var$preload_date <- as.character(b2_2021_post_drct_var$preload_date)

# Documentary RCT pre+post
b2_2021_prepost_drct <- bind_rows(b2_2021_pre_drct_var,b2_2021_post_drct_var)
b2_2021_prepost_drct <- edit_base_level(b2_2021_prepost_drct)

#-----Reading in the Survey questionnaire and Choices
b2_2021_pre_post_drct_suvy <- read_sheet("https://docs.google.com/spreadsheets/d/1uvmIntWgDgx1qQgz68EUJhzuy1PzI3dfHJkR7680z60/edit#gid=2121457295",sheet="survey") %>%  clean_names() 
b2_2021_pre_post_drct_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1uvmIntWgDgx1qQgz68EUJhzuy1PzI3dfHJkR7680z60/edit#gid=549137032",sheet="choices") %>%  clean_names() 

#Create option list
b2_2021_drct_opt_list <- opt_list(b2_2021_pre_post_drct_suvy,b2_2021_pre_post_drct_choices)
b2_2021_drct_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b2_2021_drct_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

#Remove duplicated options
b2_2021_drct_opt_list[!is.na(b2_2021_drct_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b2_2021_drct_opt_list <- b2_2021_drct_opt_list[!duplicated(b2_2021_drct_opt_list$dup_check),]

#Handling Continuous variables
b2_2021_prepost_drct_int <- clean_integer_cols(b2_2021_prepost_drct,b2_2021_pre_post_drct_suvy) %>% data.table()

#Handling Categorical variables
b2_2021_prepost_drct <- b2_2021_prepost_drct %>% data.table()
b2_2021_drct_cat <- clean_categorical_cols(b2_2021_prepost_drct,b2_2021_drct_opt_list)

#---merging b2_2021_prepost_drct_int with b2_2021_drct_cat
b2_2021_drct_cat <- b2_2021_drct_cat %>% select(-c(community))
b2_2021_prepost_drct_int <- b2_2021_prepost_drct_int %>% select(-c(sys_participant_id))
b2_2021_drct_clean <- merge(b2_2021_drct_cat,b2_2021_prepost_drct_int, by="KEY",all.x = TRUE,all.y = TRUE)

#--changing column names
b2_2021_drct_clean <- setnames(b2_2021_drct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b2_2021_pre_drct_clean <- b2_2021_drct_clean %>% filter(type == "pre")
b2_2021_pre_drct_ops <- merge(b2_2021_pre_drct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b2_2021_pre_drct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b2_2021_pre_drct_ops.csv')

#post
b2_2021_post_drct_clean <- b2_2021_drct_clean %>% filter(type == "post")
b2_2021_post_drct_ops <- merge(b2_2021_post_drct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b2_2021_post_drct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b2_2021_post_drct_ops.csv')

#pre/post
b2_2021_drct_prepost_ops <- merge(b2_2021_drct_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b2_2021_drct_prepost_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Long format/b2_2021_drct_pre_post.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched
b2_2021_drct_prepost_merged <- merge(b2_2021_pre_drct_clean,b2_2021_post_drct_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b2_2021_drct_prepost_merged[b2_2021_drct_prepost_merged$type.x == "pre" & b2_2021_drct_prepost_merged$type.y == "post", prepost_match := "matched"]
b2_2021_drct_prepost_merged[is.na(b2_2021_drct_prepost_merged$prepost_match),prepost_match := "unmatched"]

b2_2021_drct_prepost_merged <- merge(b2_2021_drct_prepost_merged,ops_data,by="hhid",all.x = TRUE)
#b2_2021_drct_prepost_merged
write.csv(b2_2021_drct_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/b2_2021_drct_prepost_merged.csv')

#matched
b2_2021_drct_prepost_matched <- prepost_match(b2_2021_pre_drct_clean,b2_2021_post_drct_clean)
b2_2021_drct_prepost_matched <- merge(b2_2021_drct_prepost_matched,ops_data, by = "hhid.x")
write.csv(b2_2021_drct_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/matched/b2_2021_drct_prepost_matched.csv')

##* Create 2018 B1 codebook
b2_2021_prepost_drct_codebook <- create_codebook(b2_2021_pre_post_drct_suvy,b2_2021_drct_opt_list,b2_2021_pre_drct_var,b2_2021_post_drct_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b2_2021_prepost_drct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b2_2021_prepost_drct_codebook.csv')

#To be used for a combined codebook for all batches   (2021B2 Pre)
b2_2021_pre_drct_codebook <- for_mstcdB(b2_2021_pre_drct_var,"2021B2 Pre drct")
b2_2021_post_drct_codebook <- for_mstcdB(b2_2021_post_drct_var,"2021B2 Post drct")
#****************************************************************************************************************************************************************************************



##########################################################################################################################
# B2 2021 pre stretch v2
b2_2021_pre_stretch <-  read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch2_Transform Stretch V2/1.Pre/06_Clean Data/PH_FY21_B2_Stretch V2_Presurvey_de_dupli_consent_only_data.csv")  

b2_2021_pre_stretch_v2_var <- select_available(b2_2021_pre_stretch)
b2_2021_pre_stretch_v2_dups <- duplicated_data(b2_2021_pre_stretch_v2_var)
b2_2021_pre_stretch_v2_var <- data_needed(b2_2021_pre_stretch_v2_dups,b2_2021_pre_stretch_v2_var,"pre","2021")
b2_2021_pre_stretch_v2_var$calc_field1 <- as.character(b2_2021_pre_stretch_v2_var$calc_field1)

# POST
b2_2021_post_stretch <-  read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch2_Transform Stretch V2/2.Post/05_Raw Data/PH_FY21_B2_Stretch_V2_Post_Official_WIDE.csv")  

b2_2021_post_stretch_v2_var <- select_available(b2_2021_post_stretch)
b2_2021_post_stretch_v2_dups <- duplicated_data(b2_2021_post_stretch_v2_var)
b2_2021_post_stretch_v2_var <- data_needed(b2_2021_post_stretch_v2_dups,b2_2021_post_stretch_v2_var,"post","2021")
b2_2021_post_stretch_v2_var$oinc5_oth <- as.character(b2_2021_post_stretch_v2_var$oinc5_oth)
## Pre + Post
b2_2021_prepost_stretch_v2 <- bind_rows(b2_2021_pre_stretch_v2_var,b2_2021_post_stretch_v2_var)
b2_2021_prepost_stretch_v2 <- edit_base_level(b2_2021_prepost_stretch_v2)

#Reading in the survey and choices
b2_stretch_v2_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1lq7v_NDPkxTeE2Uu7x8VY5F3WIPHSwwDx4m7w8H1kns/edit#gid=230369463",sheet="survey") %>%  clean_names()
b2_stretch_v2_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1lq7v_NDPkxTeE2Uu7x8VY5F3WIPHSwwDx4m7w8H1kns/edit#gid=230369463",sheet="choices") %>%  clean_names()

#create option list
b2_2021_stretchv2_opt_list <- opt_list(b2_stretch_v2_svy,b2_stretch_v2_choices)

b2_2021_stretchv2_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b2_2021_stretchv2_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
#Remove duplicated options
b2_2021_stretchv2_opt_list[!is.na(b2_2021_stretchv2_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b2_2021_stretchv2_opt_list <- b2_2021_stretchv2_opt_list[!duplicated(b2_2021_stretchv2_opt_list$dup_check),]

#Handling Continuous variables
b2_2021_pre_stretch_v2_int <- clean_integer_cols(b2_2021_prepost_stretch_v2,b2_stretch_v2_svy) %>% data.table()

#Handling Categorical variables
b2_2021_prepost_stretch_v2 <- b2_2021_prepost_stretch_v2 %>% data.table()
b2_2021_pre_stretch_v2_cat <- clean_categorical_cols2(b2_2021_prepost_stretch_v2,b2_2021_stretchv2_opt_list)
b2_2021_pre_stretch_v2_cat <- b2_2021_pre_stretch_v2_cat %>% select(-c(sys_participant_id))

b2_2021_prepost_stretch_v2_clean <- merge(b2_2021_pre_stretch_v2_cat,b2_2021_pre_stretch_v2_int, by="KEY",all.x = TRUE,all.y = TRUE)
#--changing column names
b2_2021_prepost_stretch_v2_clean <- setnames(b2_2021_prepost_stretch_v2_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b2_2021_pre_tsv2_clean <- b2_2021_prepost_stretch_v2_clean %>% filter(type == "pre")
b2_2021_pre_tsv2_ops <- merge(b2_2021_pre_tsv2_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b2_2021_pre_tsv2_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b2_2021_pre_tsv2_ops.csv')

#post
b2_2021_post_tsv2_clean <- b2_2021_prepost_stretch_v2_clean %>% filter(type == "post")
b2_2021_post_tsv2_ops <- merge(b2_2021_post_tsv2_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b2_2021_post_tsv2_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b2_2021_post_tsv2_ops.csv')

#prepost
b2_2021_pre_stretch_v2_ops <- merge(b2_2021_prepost_stretch_v2_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b2_2021_pre_stretch_v2_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Long format/b2_2021_pre_stretch_v2.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched/unmatched
b2_2021_prepost_tfsv2_merged <- merge(b2_2021_pre_tsv2_clean,b2_2021_post_tsv2_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b2_2021_prepost_tfsv2_merged[b2_2021_prepost_tfsv2_merged$type.x == "pre" & b2_2021_prepost_tfsv2_merged$type.y == "post", prepost_match := "matched"]
b2_2021_prepost_tfsv2_merged[is.na(b2_2021_prepost_tfsv2_merged$prepost_match),prepost_match := "unmatched"]

b2_2021_prepost_tfsv2_merged <- merge(b2_2021_prepost_tfsv2_merged,ops_data,by="hhid",all.x = TRUE)
#b2_2021_prepost_tfsv2_merged
write.csv(b2_2021_prepost_tfsv2_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/b2_2021_prepost_tfsv2_merged.csv')
#matched
b2_2021_tsv2_prepost_matched <- prepost_match(b2_2021_pre_tsv2_clean,b2_2021_post_tsv2_clean)
b2_2021_tsv2_prepost_matched <- merge(b2_2021_tsv2_prepost_matched,ops_data, by = "hhid.x")
write.csv(b2_2021_tsv2_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/matched/b2_2021_tsv2_prepost_matched.csv')

##* Create 2021 B1 codebook
b2_2021_prepost_tfsv2_codebook <- create_codebook(b2_stretch_v2_svy,b2_2021_stretchv2_opt_list,b2_2021_pre_stretch_v2_var,b2_2021_post_stretch_v2_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b2_2021_prepost_tfsv2_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b2_2021_prepost_tfsv2_codebook.csv')

#To be used for a combined codebook for all batches   (2021B2 Pre)
b2_2021_pre_tsv2_codebook <- for_mstcdB(b2_2021_pre_stretch_v2_var,"2021B2 Pre tsv2")
b2_2021_post_tsv2_codebook <- for_mstcdB(b2_2021_post_stretch_v2_var,"2021B2 Post tsv2")
#****************************************************************************************************************************************************************************************


##########################################################################################################################
# B3 2021 pre chc tinkering
b3_2021_pre_chc_tk <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch3_CHC Tinkering/1.Pre/06_Clean Data/PH_FY21_B3_CHC_Presurvey_de_dupli_consent_only_data.csv")                      

b3_2021_pre_chc_t_var <- select_available(b3_2021_pre_chc_tk)
b3_2021_pre_chc_t_dups <- duplicated_data(b3_2021_pre_chc_t_var)
b3_2021_pre_chc_t_var <- data_needed(b3_2021_pre_chc_t_dups,b3_2021_pre_chc_t_var,"pre","2021")
b3_2021_pre_chc_t_var <- edit_base_level(b3_2021_pre_chc_t_var)
# survey and choices
b3_2021_chc_t_svy <- read_sheet("https://docs.google.com/spreadsheets/d/198Us23mfIZPrWfUrCx0h48O8MaGY6gnC5O2jviDyZus/edit#gid=2121457295",sheet="survey") %>%  clean_names() 
b3_2021_chc_t_choices <- read_sheet("https://docs.google.com/spreadsheets/d/198Us23mfIZPrWfUrCx0h48O8MaGY6gnC5O2jviDyZus/edit#gid=549137032",sheet="choices") %>%  clean_names() 

#create option list
b3_2021_chc_t_opt_list <- opt_list(b3_2021_chc_t_svy,b3_2021_chc_t_choices)

b3_2021_chc_t_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b3_2021_chc_t_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
b3_2021_chc_t_opt_list <- b3_2021_chc_t_opt_list %>% data.table()
#Remove duplicated options
b3_2021_chc_t_opt_list[!is.na(b3_2021_chc_t_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b3_2021_chc_t_opt_list <- b3_2021_chc_t_opt_list[!duplicated(b3_2021_chc_t_opt_list$dup_check),]

#Handling Continuous variables
b3_2021_pre_chc_t_int <- clean_integer_cols(b3_2021_pre_chc_t_var,b3_2021_chc_t_svy) %>% data.table()

#Handling Categorical variables
b3_2021_pre_chc_t_var <- b3_2021_pre_chc_t_var %>% data.table()
b3_2021_chc_t_opt_cat <- clean_categorical_cols2(b3_2021_pre_chc_t_var,b3_2021_chc_t_opt_list)

# merging categorical & continuous variables together
b3_2021_chc_t_opt_cat <- b3_2021_chc_t_opt_cat %>% select(-c(sys_participant_id))
b3_2021_pre_chc_t_clean <- merge(b3_2021_chc_t_opt_cat,b3_2021_pre_chc_t_int, by="KEY",all.x = TRUE,all.y = TRUE)
#--changing column names
b3_2021_pre_chc_t_clean <- setnames(b3_2021_pre_chc_t_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b3_2021_pre_chct_clean <- b3_2021_pre_chc_t_clean %>% filter(type == "pre")
b3_2021_pre_chct_ops <- merge(b3_2021_pre_chct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b3_2021_pre_chct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b3_2021_pre_chct_ops.csv')

#post
b3_2021_post_chct_clean <- b3_2021_pre_chc_t_clean %>% filter(type == "post")

#prepost
b3_2021_pre_chc_t_ops <- merge(b3_2021_pre_chc_t_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b3_2021_pre_chc_t_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Long format/b3_2021_pre_chc_t.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched
b3_2021_prepost_chc_t_merged <- merge(b3_2021_pre_chct_clean,b3_2021_post_chct_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b3_2021_prepost_chc_t_merged[b3_2021_prepost_chc_t_merged$type.x == "pre" & b3_2021_prepost_chc_t_merged$type.y == "post", prepost_match := "matched"]
b3_2021_prepost_chc_t_merged[is.na(b3_2021_prepost_chc_t_merged$prepost_match),prepost_match := "unmatched"]

b3_2021_prepost_chc_t_merged <- merge(b3_2021_prepost_chc_t_merged,ops_data,by="hhid",all.x = TRUE) 
#b3_2021_prepost_chc_t_merged
write.csv(b3_2021_prepost_chc_t_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/b3_2021_prepost_chc_t_merged.csv')

#matched

##* Create 2021 B1 codebook
b3_2021_prepost_chc_t_codebook <- create_codebook(b3_2021_chc_t_svy,b3_2021_chc_t_opt_list,b3_2021_pre_chc_t_var,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b3_2021_prepost_chc_t_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b3_2021_prepost_chc_t_codebook.csv')

#To be used for a combined codebook for all batches   (2021B3 Pre)
b3_2021_pre_chct_codebook <- for_mstcdB(b3_2021_pre_chc_t_var,"2021B3 Pre chct")
#b3_2021_post_chct_codebook <- for_mstcdB(,"2021B3 Post chct")
#****************************************************************************************************************************************************************************************


##########################################################################################################################
##B3 2021 TF RCT pre
b3_2021_pre_tf_rct <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch3_Transform RCT/1.Pre/06_Clean Data/PH_FY21_B3_Presurvey_de_dupli_consent_only_data.csv")  

B3_2021_trct_var <- select_available(b3_2021_pre_tf_rct)
B3_2021_trct_dups <- duplicated_data(B3_2021_trct_var)
B3_2021_trct_var <- data_needed(B3_2021_trct_dups,B3_2021_trct_var,"pre","2021")
B3_2021_trct_var <- edit_base_level(B3_2021_trct_var)

B3_2021_trct_var <- B3_2021_trct_var %>% data.table()

# survey and choices
b3_trct_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1_B30_FcVnFTFif0PNHCgJAPUuqUZc_6870qQ8UdK6fo/edit#gid=2121457295",sheet="survey") %>%  clean_names() %>% clean_names()
b3_trct_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1_B30_FcVnFTFif0PNHCgJAPUuqUZc_6870qQ8UdK6fo/edit#gid=549137032",sheet="choices") %>%  clean_names() %>% clean_names()

#create option list
b3_trct_opt_list <- opt_list(b3_trct_svy,b3_trct_choices)

b3_trct_opt_list <- b3_trct_opt_list %>% data.table()
b3_trct_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b3_trct_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
#Remove duplicated options
b3_trct_opt_list[!is.na(b3_trct_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b3_trct_opt_list <- b3_trct_opt_list[!duplicated(b3_trct_opt_list$dup_check),]

#Handling Continuous variables
B3_2021_trct_int <- clean_integer_cols(B3_2021_trct_var,b3_trct_svy) %>% data.table()

#Handling Categorical variables
B3_2021_trct_cat <- clean_categorical_cols(B3_2021_trct_var,b3_trct_opt_list)

# merging categorical & continuous variables together
B3_2021_trct_cat <- B3_2021_trct_cat %>% select(-c(sys_participant_id))
B3_2021_pre_trct_clean <- merge(B3_2021_trct_cat,B3_2021_trct_int, by="KEY",all.x = TRUE,all.y = TRUE)

#--changing column names
B3_2021_pre_trct_clean <- setnames(B3_2021_pre_trct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b3_2021_pre_trct_clean <- B3_2021_pre_trct_clean %>% filter(type == "pre")
b3_2021_pre_trct_ops <- merge(b3_2021_pre_trct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b3_2021_pre_trct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b3_2021_pre_trct_ops.csv')

#post
b3_2021_post_trct_clean <- B3_2021_pre_trct_clean %>% filter(type == "post")

#prepost
b3_2021_pre_trct_ops <- merge(B3_2021_pre_trct_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b3_2021_pre_trct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Long format/b3_2021_pre_trct.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched
B3_2021_prepost_trct_merged <- merge(b3_2021_pre_trct_clean,b3_2021_post_trct_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
B3_2021_prepost_trct_merged[B3_2021_prepost_trct_merged$type.x == "pre" & B3_2021_prepost_trct_merged$type.y == "post", prepost_match := "matched"]
B3_2021_prepost_trct_merged[is.na(B3_2021_prepost_trct_merged$prepost_match),prepost_match := "unmatched"]

B3_2021_prepost_trct_merged <- merge(B3_2021_prepost_trct_merged,ops_data,by="hhid",all.x = TRUE) 
write.csv(B3_2021_prepost_trct_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/B3_2021_prepost_trct_merged.csv')

#matched

##* Create 2021 B1 codebook
b3_2021_prepost_trct_codebook <- create_codebook(b3_trct_svy,b3_trct_opt_list,B3_2021_trct_var,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b3_2021_prepost_trct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b3_2021_prepost_trct_codebook.csv')

#To be used for a combined codebook for all batches   (2021B3 Pre)
b3_2021_pre_trct_codebook <- for_mstcdB(B3_2021_trct_var,"2021B3 Pre trct")
#b3_2021_post_trct_codebook <- for_mstcdB(,"2021B3 Post trct")
#****************************************************************************************************************************************************************************************
#------------------------------------------------------------------------------------------------------------------



##########################################################################################################################################################
b1_2022_pre_irct <-  read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/14.FY202223/Batch1_Integrated RCT/1.Pre/06_Clean Data/PH_FY22_B1_Presurvey_de_dupli_consent_only_data.csv") 
  
b1_2022_pre_irct_var <- select_available(b1_2022_pre_irct)
b1_2022_pre_irct_dups <- duplicated_data(b1_2022_pre_irct_var)
b1_2022_pre_irct_var <- data_needed(b1_2022_pre_irct_dups,b1_2022_pre_irct_var,"pre","2022")
b1_2022_pre_irct_var <- edit_base_level(b1_2022_pre_irct_var) %>% data.table()

# survey and choices
b1_2022_irct_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1Wunal-BhoEzjaKwlzZL79589ZLnLR5WzCOaYhwk0QQQ/edit#gid=2121457295",sheet="survey") %>%  clean_names() %>% clean_names()
b1_2022_irct_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1Wunal-BhoEzjaKwlzZL79589ZLnLR5WzCOaYhwk0QQQ/edit#gid=549137032",sheet="choices") %>%  clean_names() %>% clean_names()

#create option list
b1_2022_irct_opt_list <- opt_list(b1_2022_irct_svy,b1_2022_irct_choices)

b1_2022_irct_opt_list <- b1_2022_irct_opt_list %>% data.table()
b1_2022_irct_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2022_irct_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
#Remove duplicated options
b1_2022_irct_opt_list[!is.na(b1_2022_irct_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b1_2022_irct_opt_list <- b1_2022_irct_opt_list[!duplicated(b1_2022_irct_opt_list$dup_check),]

#Handling Continuous variables
b1_2022_pre_irct_int <- clean_integer_cols(b1_2022_pre_irct_var,b1_2022_irct_svy) %>% data.table()

#Handling Categorical variables
b1_2022_pre_irct_cat <- clean_categorical_cols(b1_2022_pre_irct_var,b1_2022_irct_opt_list)

# merging categorical & continuous variables together
b1_2022_pre_irct_cat <- b1_2022_pre_irct_cat %>% select(-c(sys_participant_id))
b1_2022_pre_irct_clean <- merge(b1_2022_pre_irct_cat,b1_2022_pre_irct_int, by="KEY",all.x = TRUE,all.y = TRUE)

#--changing column names
b1_2022_pre_irct_clean <- setnames(b1_2022_pre_irct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b1_2022_pre_irct_clean <- b1_2022_pre_irct_clean %>% filter(type == "pre")
b1_2022_pre_irct_ops <- merge(b1_2022_pre_irct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b1_2022_pre_irct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/pre_post/b1_2022_pre_irct_ops.csv')

#post
b1_2022_post_irct_clean <- b1_2022_pre_irct_clean %>% filter(type == "post")

#prepost
b1_2022_pre_irct__ops <- merge(b1_2022_pre_irct_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2022_pre_irct__ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Long format/b1_2022_pre_irct.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched
b1_2022_prepost_irct_merged <- merge(b1_2022_pre_irct_clean,b1_2022_post_irct_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b1_2022_prepost_irct_merged[b1_2022_prepost_irct_merged$type.x == "pre" & b1_2022_prepost_irct_merged$type.y == "post", prepost_match := "matched"]
b1_2022_prepost_irct_merged[is.na(b1_2022_prepost_irct_merged$prepost_match),prepost_match := "unmatched"]

b1_2022_prepost_irct_merged <- merge(b1_2022_prepost_irct_merged,ops_data,by="hhid",all.x = TRUE) 
#b1_2022_prepost_irct_merged
write.csv(b1_2022_prepost_irct_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Merged/b1_2022_prepost_irct_merged.csv')

#matched

##* Create 2021 B1 codebook
b1_2022_prepost_irct_codebook <- create_codebook(b1_2022_irct_svy,b1_2022_irct_opt_list,b1_2022_pre_irct_var,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b1_2022_prepost_irct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Codebook/b1_2022_prepost_irct_codebook.csv')

#To be used for a combined codebook for all batches   (2022B1 Pre)
b1_2022_pre_irct_codebook <- for_mstcdB(b1_2022_pre_irct_var,"2022B1 Pre irct")
#b1_2022_post_irct_codebook <- for_mstcdB(,"2022B1 Post irct")
#****************************************************************************************************************************************************************************************



##########################################################################################################################################################
# Batch 2 I-RCT 2022
b2_2022_pre_irct <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/14.FY202223/Batch2_Integrated RCT/1.Pre/06_Clean Data/PH_FY22_B2_Presurvey_de_dupli_consent_only_data.csv") 

b2_2022_pre_irct_var <- select_available(b2_2022_pre_irct)
b2_2022_pre_irct_dups <- duplicated_data(b2_2022_pre_irct_var)
b2_2022_pre_irct_var <- data_needed(b2_2022_pre_irct_dups,b2_2022_pre_irct_var,"pre","2022")
b2_2022_pre_irct_var <- edit_base_level(b2_2022_pre_irct_var)  %>% data.table()

# survey and choices
b2_2022_irct_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1cIhYVum6kI66NOnBc6QC5XWX_bIJdaTy47BDOfx9kGY/edit#gid=2121457295",sheet="survey") %>%  clean_names() %>% clean_names()
b2_2022_irct_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1cIhYVum6kI66NOnBc6QC5XWX_bIJdaTy47BDOfx9kGY/edit#gid=549137032",sheet="choices") %>%  clean_names() %>% clean_names()

#CREATE OPTION LIST
b2_2022_irct_opt_list <- opt_list(b2_2022_irct_svy,b2_2022_irct_choices)

b2_2022_irct_opt_list <- b2_2022_irct_opt_list %>% data.table()
b2_2022_irct_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b2_2022_irct_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
#Remove duplicated options
b2_2022_irct_opt_list[!is.na(b2_2022_irct_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b2_2022_irct_opt_list <- b2_2022_irct_opt_list[!duplicated(b2_2022_irct_opt_list$dup_check),]
b2_2022_irct_opt_list <- b2_2022_irct_opt_list %>% data.table()

#Handling Continuous variables
b2_2022_pre_irct_int <- clean_integer_cols(b2_2022_pre_irct_var,b2_2022_irct_svy) %>% data.table()

#Handling Categorical variables
b2_2022_pre_irct_cat <- clean_categorical_cols(b2_2022_pre_irct_var,b2_2022_irct_opt_list)

# merging categorical & continuous variables together
b2_2022_pre_irct_cat <- b2_2022_pre_irct_cat %>% select(-c(sys_participant_id))
b2_2022_pre_irct_clean <- merge(b2_2022_pre_irct_cat,b2_2022_pre_irct_int, by="KEY",all.x = TRUE,all.y = TRUE)

#--changing column names
b2_2022_pre_irct_clean <- setnames(b2_2022_pre_irct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b2_2022_pre_irct_clean <- b2_2022_pre_irct_clean %>% filter(type == "pre")
b2_2022_pre_irct_ops <- merge(b2_2022_pre_irct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b2_2022_pre_irct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/pre_post/b2_2022_pre_irct_ops.csv')

#post
b2_2022_post_irct_clean <- b2_2022_pre_irct_clean %>% filter(type == "post")

#prepost
b2_2022_pre_irct__ops <- merge(b2_2022_pre_irct_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b2_2022_pre_irct__ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Long format/b2_2022_pre_irct.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched/unmatched
b2_2022_prepost_irct_merged <- merge(b2_2022_pre_irct_clean,b2_2022_post_irct_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b2_2022_prepost_irct_merged[b2_2022_prepost_irct_merged$type.x == "pre" & b2_2022_prepost_irct_merged$type.y == "post", prepost_match := "matched"]
b2_2022_prepost_irct_merged[is.na(b2_2022_prepost_irct_merged$prepost_match),prepost_match := "unmatched"]

b2_2022_prepost_irct_merged <- merge(b2_2022_prepost_irct_merged,ops_data,by="hhid",all.x = TRUE) 
write.csv(b2_2022_prepost_irct_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Merged/b2_2022_prepost_irct_merged.csv')
#matched

##* Create 2022 B1 codebook
b2_2022_prepost_irct_codebook <- create_codebook(b2_2022_irct_svy,b2_2022_irct_opt_list,b2_2022_pre_irct_var,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b2_2022_prepost_irct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Codebook/b2_2022_prepost_irct_codebook.csv')

#To be used for a combined codebook for all batches   (2022B1 Pre)
b2_2022_pre_irct_codebook <- for_mstcdB(b2_2022_pre_irct_var,"2022B2 Pre irct")
#b2_2022_post_irct_codebook <- for_mstcdB(,"2022B2 Post irct")
#****************************************************************************************************************************************************************************************



################################################################################################################################################################
#Recover RCT 2022
b3_2022_pre_rrct <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/14.FY202223/Batch3_Recover RCT/1.Pre/06_Clean Data/PH_FY22_B2_RecRCT_Presurvey_de_dupli_consent_only_data.csv")

b3_2022_pre_rrct_var <- select_available(b3_2022_pre_rrct)
b3_2022_pre_rrct_dups <- duplicated_data(b3_2022_pre_rrct_var)
b3_2022_pre_rrct_var <- data_needed(b3_2022_pre_rrct_dups,b3_2022_pre_rrct_var,"pre","2022")
b3_2022_pre_rrct_var <- edit_base_level(b3_2022_pre_rrct_var) %>% data.table()

#Since status is calculated not selected like in other surveys, editing it from here
b3_2022_pre_rrct_var$status <- as.character(b3_2022_pre_rrct_var$status)
b3_2022_pre_rrct_stts <- b3_2022_pre_rrct_var[status == 1,status := "Participant is available"]
b3_2022_pre_rrct_stts <- b3_2022_pre_rrct_var[status == 2,status := "Participant's spouse or partner is available"]
b3_2022_pre_rrct_stts <- b3_2022_pre_rrct_stts %>% select(KEY,status)

# survey and choices
b3_2022_rrct_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1kne593fhcqDZ2Fjap4fPQmejlhCyIDuDl7gwtmT-xwM/edit#gid=2121457295",sheet="survey") %>%  clean_names() %>% clean_names()
b3_2022_rrct_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1kne593fhcqDZ2Fjap4fPQmejlhCyIDuDl7gwtmT-xwM/edit#gid=549137032",sheet="choices") %>%  clean_names() %>% clean_names()

b3_2022_rrct_opt_list <- opt_list(b3_2022_rrct_svy,b3_2022_rrct_choices)
#Remove duplicated options
b3_2022_rrct_opt_list[!is.na(b3_2022_rrct_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b3_2022_rrct_opt_list <- b3_2022_rrct_opt_list[!duplicated(b3_2022_rrct_opt_list$dup_check),]
b3_2022_rrct_opt_list <- b3_2022_rrct_opt_list %>% data.table()

#Handling Continuous variables
b3_2022_pre_rrct_int <- clean_integer_cols(b3_2022_pre_rrct_var,b3_2022_rrct_svy) %>% data.table()
#Attaching the status column to -- b3_2022_pre_rrct_int
b3_2022_pre_rrct_int <- merge(b3_2022_pre_rrct_int,b3_2022_pre_rrct_stts, by="KEY",all.x = TRUE,all.y = TRUE)

#Handling Categorical variables
b3_2022_pre_rrct_cat <- clean_categorical_cols(b3_2022_pre_rrct_var,b3_2022_rrct_opt_list)

# merging categorical & continuous variables together
b3_2022_pre_rrct_cat <- b3_2022_pre_rrct_cat %>% select(-c(sys_participant_id))
b3_2022_pre_rrct_clean <- merge(b3_2022_pre_rrct_cat,b3_2022_pre_rrct_int, by="KEY",all.x = TRUE,all.y = TRUE)

#changing column names
b3_2022_pre_rrct_clean <- setnames(b3_2022_pre_rrct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b3_2022_pre_r_rct_clean <- b3_2022_pre_rrct_clean %>% filter(type == "pre")
b3_2022_pre_r_rct_ops <- merge(b3_2022_pre_r_rct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b3_2022_pre_r_rct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/pre_post/b3_2022_pre_r_rct_ops.csv')

#post
b3_2022_post_r_rct_clean <- b3_2022_pre_rrct_clean %>% filter(type == "post")

#prepost
b3_2022_pre_rrct__ops <- merge(b3_2022_pre_rrct_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b3_2022_pre_rrct__ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Long format/b3_2022_pre_rrct.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched $ unmatched
b3_2022_prepost_rrct_merged <- merge(b3_2022_pre_r_rct_clean,b3_2022_post_r_rct_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b3_2022_prepost_rrct_merged[b3_2022_prepost_rrct_merged$type.x == "pre" & b3_2022_prepost_rrct_merged$type.y == "post", prepost_match := "matched"]
b3_2022_prepost_rrct_merged[is.na(b3_2022_prepost_rrct_merged$prepost_match),prepost_match := "unmatched"]

b3_2022_prepost_rrct_merged <- merge(b3_2022_prepost_rrct_merged,ops_data,by="hhid",all.x = TRUE) 
write.csv(b3_2022_prepost_rrct_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Merged/b3_2022_prepost_rrct_merged.csv')

#matched

##* Create 2022 B1 codebook
b3_2022_prepost_rrct_codebook <- create_codebook(b3_2022_rrct_svy,b3_2022_rrct_opt_list,b3_2022_pre_rrct_var,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b3_2022_prepost_rrct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Codebook/b3_2022_prepost_rrct_codebook.csv')

#To be used for a combined codebook for all batches   (2022B3 Pre)
b3_2022_pre_rrct_codebook <- for_mstcdB(b3_2022_pre_rrct_var,"2022B3 Pre rrct")
#b3_2022_post_rrct_codebook <- for_mstcdB(,"2022B3 Post R-rct")
##****************************************************************************************************************************************************************************************

#################################################################################################################################################################

