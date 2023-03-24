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
b1_2021_prepost_strech_v1 <- tag_gif_programs(b1_2021_prepost_strech_v1,"gif","Transform Stretch v1") %>% data.table()
# saving a raw file for b1 2021 _pre_post
write_csv(b1_2021_prepost_strech_v1,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/b1_2021_prepost_strech_v1_raw.csv')

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
b1_2021_strech_v1_clean <- merge(b1_2021_strechv1_cat,b1_2021_prepost_strech_v1_int, by="KEY",all.x = TRUE,all.y = TRUE)

b1_2021_strech_v1_clean <- b1_2021_strech_v1_clean %>% select(-c(sys_participant_id))
#--changing column names
b1_2021_strech_v1_clean <- setnames(b1_2021_strech_v1_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b1_2021_strech_v1_clean <- tag_gif_programs(b1_2021_strech_v1_clean,"gif","Transform Stretch v1")
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
names(b1_2021_tsv1_prepost_matched)[names(b1_2021_tsv1_prepost_matched) == "hhid.x"] <- "hhid"
b1_2021_tsv1_prepost_matched <- merge(b1_2021_tsv1_prepost_matched,ops_data, by = "hhid")
write.csv(b1_2021_tsv1_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/matched/b1_2021_tsv1_prepost_matched.csv')

##* Create 2018 B1 codebook
b1_2021_prepost_tfsv1_codebook <- create_codebook(b1_2021_svy,b1_2021_tfs_opt_list,b1_2021_pre_strech_v1_var,b1_2021_post_strech_v1_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b1_2021_prepost_tfsv1_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b1_2021_prepost_tfsv1_codebook.csv')

#To be used for a combined codebook for all batches   (2021B1 Pre)
b1_2021_pre_tsv1_codebook <- for_mstcdB(b1_2021_pre_strech_v1_var,"2021B1 Pre tsv1")
b1_2021_post_tsv1_codebook <- for_mstcdB(b1_2021_post_strech_v1_var,"2021B1 Post tsv1")
b1_2021_tsv1_codebook <- bind_rows(b1_2021_pre_tsv1_codebook,b1_2021_post_tsv1_codebook)
write_csv(b1_2021_tsv1_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b1_2021_tsv1_codebook.csv')
