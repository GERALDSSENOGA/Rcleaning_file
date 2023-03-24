# FY2018-2019 B1 - pre SURVEY
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
##________________________________----------------------------------------------------------------------------------------------__________________________________________________________________##

#************************************************************************************************************************************************************************************************##
# B1 2018 PRE (BASELINE)
b1_2018_pre <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2018B1/2018B1_PRE.csv")
#clean
b1_2018_pre_var <- select_available(b1_2018_pre)
b1_2018_pre_dups <- duplicated_data(b1_2018_pre_var)
b1_2018_pre_var <- data_needed(b1_2018_pre_dups,b1_2018_pre_var,"pre","2018")

b1_2018_pre_var$batch_name <- "FY18-19 B1"
b1_2018_pre_var$program_name <- "Transform Level 1" #Not sure of the program name, setting it to Transform Level 1 (default)
b1_2018_pre_var <- edit_base_level(b1_2018_pre_var)  %>% data.table()
b1_2018_pre_var <- tag_gif_programs(b1_2018_pre_var,"none-gif","none-gif")

# saving a raw file for b2_2018_pre_post
write_csv(b1_2018_pre_var,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b1_2018_pre_raw.csv')

#Pulling in the survey and the choices
b1_2018_pre_suvy <- read_sheet("https://docs.google.com/spreadsheets/d/1r9cpq7Zuy902oFUXFM1vx0LP6qvwzbi_XNXY2eMmt3Q/edit#gid=551421512",sheet="survey") %>%  clean_names()
b1_2018_pre_choi <- read_sheet("https://docs.google.com/spreadsheets/d/1r9cpq7Zuy902oFUXFM1vx0LP6qvwzbi_XNXY2eMmt3Q/edit#gid=1210400051",sheet="choices") %>%  clean_names()

# create a Choices and labels list
b1_2018_pre_opt_list <- opt_list(b1_2018_pre_suvy,b1_2018_pre_choi)

b1_2018_pre_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2018_pre_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

#Handling Continuous variables
b1_2018_pre_int <- clean_integer_cols(b1_2018_pre_var,b1_2018_pre_suvy) %>% data.table()# selecting integers and seting 0 and negavites to NULL .. int_cols

#Handling Categorical variables
b1_2018_pre_cat <- clean_categorical_cols(b1_2018_pre_var,b1_2018_pre_opt_list) # replace values with labels

# merging categorical & continuous variables together
b1_2018_pre_all <- merge(b1_2018_pre_cat,b1_2018_pre_int, by="KEY",all.x = TRUE,all.y = TRUE) 
b1_2018_pre_all <- b1_2018_pre_all %>% select(-c(program_name))
b1_2018_pre_all <- b1_2018_pre_all %>% data.table()
b1_2018_pre_all$gif_tag <- "none-gif"
b1_2018_pre_all$gif_program_name <- "none-gif"
#-----------------   update column names  ------------------------------------------------------------------
setnames(b1_2018_pre_all,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)

### prepost + Ops data
ops_data_fy1819b1 <- ops_data[ops_data$batch_name == "FY18-19 B1",]

b1_2018_pre_ops <- merge(b1_2018_pre_all,ops_data_fy1819b1, by="hhid",all.x = TRUE) 

#save in pre_post
write_csv(b1_2018_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/pre_post/b1_2018_preops.csv')

# save in PrePost_Long format
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

##* Create 2018 B1 codebook
b1_2018_pre_codebook <- create_codebook(b1_2018_pre_suvy,b1_2018_pre_opt_list,b1_2018_pre,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b1_2018_pre_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Codebook/b1_2018_pre_codebook.csv')

#To be used for a combined codebook for all batches   (2023B1 Pre)
b1_2018_codebook <- for_mstcdB(b1_2018_pre,"2018B1 Pre")
write_csv(b1_2018_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/1.FY201819/PrePost_Codebook/b1_2018_codebook.csv')
#************************************************************************************************************************************************************************************************##



