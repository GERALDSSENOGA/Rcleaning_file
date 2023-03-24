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
##************************************************************************************************************************************************************************************************##

## Pre-Post master-codebook _______________________________----------------------------------------------------------------------------------------------__________________________________________________________________##
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
#________________________________----------------------------------------------------------------------------------------------________________________________________________________________
#***********************************************************************************************************************************************************************************************

# FY2018-2019 B2 - pre/post SURVEY
b2_2018_pre <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B2/2018B2_PRE.csv")

b2_2018_pre_var <- select_available(b2_2018_pre)
b2_2018_pre_dups <- duplicated_data(b2_2018_pre_var)
b2_2018_pre_var <- data_needed(b2_2018_pre_dups,b2_2018_pre_var,"pre","2018")
b2_2018_pre_var$program_name <- "Transform Level 1"
b2_2018_pre_var <- mutate(b2_2018_pre_var,name_id = paste0(name_icm,"_",community))

# B2 2018 Post (BASELINE)
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

#Update base names and program names
b2_2018_pre_post$batch_name <- "FY18-19 B2"
b2_2018_pre_post <- edit_base_level(b2_2018_pre_post) %>% data.table()
b2_2018_pre_post <- tag_gif_programs(b2_2018_pre_post,"none-gif","none-gif") %>% data.table()
# saving a raw file for b2_2019_pre_post
write_csv(b2_2018_pre_post,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b2_2018_pre_post_raw.csv')

#survey and choices
b2_2018_srvy <- read_sheet("https://docs.google.com/spreadsheets/d/16pAI87fDu_9fWrdeSGN1PhhrcBDg0GWJnFkVvzSIHu0/edit#gid=653315037",sheet="survey") %>%  clean_names()
b2_2018_choices <- read_sheet("https://docs.google.com/spreadsheets/d/16pAI87fDu_9fWrdeSGN1PhhrcBDg0GWJnFkVvzSIHu0/edit#gid=1885337514",sheet="choices") %>%  clean_names()

b2_2018_prepost_opt_list <- opt_list(b2_2018_srvy,b2_2018_choices)
b2_2018_prepost_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b2_2018_prepost_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

#Continuous variables
b2_2018_pre_post_int <- clean_integer_cols(b2_2018_pre_post,b2_2018_srvy) %>% data.table()

#Categorical variables
b2_2018_prepost_cols_cat <- clean_categorical_cols(b2_2018_pre_post,b2_2018_prepost_opt_list) # replace values with labels

# integer variable + categorical variables
b2_2018_prepost_clean <- merge(b2_2018_prepost_cols_cat,b2_2018_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)

# update column names  
setnames(b2_2018_prepost_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b2_2018_prepost_clean <- tag_gif_programs(b2_2018_prepost_clean,"none-gif","none-gif")
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
b2_2018_codebook <- bind_rows(b2_2018_pre_codebook,b2_2018_post_codebook)
write_csv(b2_2018_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Codebook/b2_2018_codebook.csv')
#******************************************************************************************************************************************************************************************************************************

