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
# -----------------------------------------------------------------------

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
b2_2021_prepost_drct <- tag_gif_programs(b2_2021_prepost_drct,"gif","Documentary RCT") 
# save raw file
write_csv(b2_2021_prepost_drct,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b2_2021_prepost_drct_raw.csv')

# Reading in the Survey questionnaire and Choices
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
b2_2021_drct_clean <- tag_gif_programs(b2_2021_drct_clean,"gif","Documentary RCT") 
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
names(b2_2021_drct_prepost_matched)[names(b2_2021_drct_prepost_matched) == "hhid.x"] <- "hhid"
b2_2021_drct_prepost_matched <- merge(b2_2021_drct_prepost_matched,ops_data, by = "hhid")
write.csv(b2_2021_drct_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/matched/b2_2021_drct_prepost_matched.csv')

##* Create 2018 B1 codebook
b2_2021_prepost_drct_codebook <- create_codebook(b2_2021_pre_post_drct_suvy,b2_2021_drct_opt_list,b2_2021_pre_drct_var,b2_2021_post_drct_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b2_2021_prepost_drct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b2_2021_prepost_drct_codebook.csv')

#To be used for a combined codebook for all batches   (2021B2 Pre)
b2_2021_pre_drct_codebook <- for_mstcdB(b2_2021_pre_drct_var,"2021B2 Pre drct")
b2_2021_post_drct_codebook <- for_mstcdB(b2_2021_post_drct_var,"2021B2 Post drct")
b2_2021_drct_codebook <- bind_rows(b2_2021_pre_drct_codebook,b2_2021_post_drct_codebook)
write_csv(b2_2021_drct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b2_2021_drct_codebook.csv')
