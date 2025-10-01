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
#########################################################################################################################
#  Recover RCT 2022
b3_2022_pre_rrct <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/14.FY202223/Batch3_Recover RCT/1.Pre/06_Clean Data/PH_FY22_B2_RecRCT_Presurvey_de_dupli_consent_only_data.csv")

b3_2022_pre_rrct_var <- select_available(b3_2022_pre_rrct)
b3_2022_pre_rrct_dups <- duplicated_data(b3_2022_pre_rrct_var)
b3_2022_pre_rrct_var <- data_needed(b3_2022_pre_rrct_dups,b3_2022_pre_rrct_var,"pre","2022")
b3_2022_pre_rrct_var <- edit_base_level(b3_2022_pre_rrct_var) %>% data.table()
b3_2022_pre_rrct_var <- tag_gif_programs(b3_2022_pre_rrct_var,"gif","Recover RCT") 
write_csv(b3_2022_pre_rrct_var,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/pre_post/b3_2022_pre_rrct_raw.csv')

# Since status is calculated not selected like in other surveys, editing it from here
b3_2022_pre_rrct_var$status <- as.character(b3_2022_pre_rrct_var$status)
b3_2022_pre_rrct_stts <- b3_2022_pre_rrct_var[status == 1,status := "Participant is available"]
b3_2022_pre_rrct_stts <- b3_2022_pre_rrct_var[status == 2,status := "Participant's spouse or partner is available"]
b3_2022_pre_rrct_stts <- b3_2022_pre_rrct_stts %>% select(KEY,status)

# survey and choices
b3_2022_rrct_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1kne593fhcqDZ2Fjap4fPQmejlhCyIDuDl7gwtmT-xwM/edit#gid=2121457295",sheet="survey") %>%  clean_names() %>% clean_names()
b3_2022_rrct_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1kne593fhcqDZ2Fjap4fPQmejlhCyIDuDl7gwtmT-xwM/edit#gid=549137032",sheet="choices") %>%  clean_names() %>% clean_names()

b3_2022_rrct_opt_list <- opt_list(b3_2022_rrct_svy,b3_2022_rrct_choices)
# Remove duplicated options
b3_2022_rrct_opt_list[!is.na(b3_2022_rrct_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b3_2022_rrct_opt_list <- b3_2022_rrct_opt_list[!duplicated(b3_2022_rrct_opt_list$dup_check),]
b3_2022_rrct_opt_list <- b3_2022_rrct_opt_list %>% data.table()

# Handling Continuous variables
b3_2022_pre_rrct_int <- clean_integer_cols(b3_2022_pre_rrct_var,b3_2022_rrct_svy) %>% data.table()
#Attaching the status column to -- b3_2022_pre_rrct_int
b3_2022_pre_rrct_int <- merge(b3_2022_pre_rrct_int,b3_2022_pre_rrct_stts, by="KEY",all.x = TRUE,all.y = TRUE)

# Handling Categorical variables
b3_2022_pre_rrct_cat <- clean_categorical_cols(b3_2022_pre_rrct_var,b3_2022_rrct_opt_list)

# merging categorical & continuous variables together

b3_2022_pre_rrct_clean <- merge(b3_2022_pre_rrct_cat,b3_2022_pre_rrct_int, by="KEY",all.x = TRUE,all.y = TRUE)

#changing column names
b3_2022_pre_rrct_clean <- setnames(b3_2022_pre_rrct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b3_2022_pre_rrct_clean <- tag_gif_programs(b3_2022_pre_rrct_clean,"gif","Recover RCT")
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
write_csv(b3_2022_pre_rrct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Codebook/b3_2022_rrct_codebook.csv')
