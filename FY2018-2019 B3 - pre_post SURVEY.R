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
domo <- rdomo::Domo(client_id="",
                    secret="")

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
b3_2018_pre_post <- tag_gif_programs(b3_2018_pre_post,"none-gif","none-gif") %>% data.table()
write_csv(b3_2018_pre_post,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b3_2018_pre_post_raw.csv')

#survey and choices
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
b3_2018_pre_post_clean <- tag_gif_programs(b3_2018_pre_post_clean,"none-gif","none-gif")
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
b3_2018_prepost_merged <- b3_2018_prepost_merged %>% select(-c(name_id.x,name_id.y))

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
b3_2018_codebook <- bind_rows(b3_2018_pre_codebook,b3_2018_post_codebook)
write_csv(b3_2018_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Codebook/b3_2018_codebook.csv')
#***********************************************************************************************************************************************************************************************************************************#
