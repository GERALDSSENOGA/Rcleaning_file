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

###############################################################################################################################
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

