library(googlesheets4)
library(tidyverse)
library(janitor)
library(lubridate, warn.conflicts = FALSE)
library(plyr)
library(data.table)
library(plyr)
library(dplyr)
library(lubridate, warn.conflicts = FALSE)

source("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/R script/pre_post functions.R")
#***********************************************************************************************************************************************************************************************##
# Operations data from DOMO, Dataset link: https://caremin.domo.com/datasources/0917c8aa-a2a8-4f3a-837c-1f6fc9aa4d24/details/data/table
domo <- rdomo::Domo(client_id="a9be407f-02b5-4c6c-a767-9875a37c1ab1",
                    secret="5903c7704fc5a8604dba4b7d6077e39529725ba9d98b6603097fe8242b868ca3")

ops_data <- domo$ds_get('0917c8aa-a2a8-4f3a-837c-1f6fc9aa4d24')
names(ops_data)[names(ops_data) == "participant_id"] <- "hhid"
ops_data <- ops_data %>% mutate(dups_check = paste0(hhid,"_",batch_name))
ops_data <- ops_data[!duplicated(ops_data$dups_check),]
#***********************************************************************************************************************************************************************************************##

# Pre-Post master-codebook _______________________________----------------------------------------------------------------------------------------------__________________________________________________________________##
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

#B1 2020 pre
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
b1_2020_pre_post <- tag_gif_programs(b1_2020_pre_post,"none-gif","none-gif") %>% data.table()
# saving a raw file for b_2020_pre_post
write_csv(b1_2020_pre_post,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/pre_post/b1_2020_pre_post_raw.csv')

# Pulling in the survey and choices
b1_2020_svy <- read_sheet("https://docs.google.com/spreadsheets/d/15I85iZDF7Z1BG_uQO4ywgAnuh0K_PRUyIu-8e4_BJVo/edit#gid=2121457295",sheet="survey") %>%  clean_names() 
b1_2020_choices <- read_sheet("https://docs.google.com/spreadsheets/d/15I85iZDF7Z1BG_uQO4ywgAnuh0K_PRUyIu-8e4_BJVo/edit#gid=549137032",sheet="choices") %>%  clean_names() 

# create option list
b1_2020_opt_list <- opt_list_2(b1_2020_svy,b1_2020_choices)

b1_2020_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2020_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
# Remove duplicated options
b1_2020_opt_list[!is.na(b1_2020_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b1_2020_opt_list <- b1_2020_opt_list[!duplicated(b1_2020_opt_list$dup_check),]

# Handling Continuous variables
b1_2020_pre_post_int <- clean_integer_cols(b1_2020_pre_post,b1_2020_svy) %>% data.table()

# Handling Categorical variables
b1_2020_pre_post_cat <- clean_categorical_cols(b1_2020_pre_post,b1_2020_opt_list)

# Categorical + Continuous variables
b1_2020_pre_post_clean <- merge(b1_2020_pre_post_cat,b1_2020_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)

# changing column names
b1_2020_pre_post_clean <- setnames(b1_2020_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b1_2020_pre_post_clean <- tag_gif_programs(b1_2020_pre_post_clean,"none-gif","none-gif")
#----------------------------------------------------------------------------------------------------------------
# Ops data
# pre
b1_2020_pre_clean <- b1_2020_pre_post_clean %>% filter(type == "pre")
write_csv(b1_2020_pre_clean,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/pre_post/b1_2020_pre_clean.csv')
# post
b1_2020_post_clean <- b1_2020_pre_post_clean %>% filter(type == "post")
write_csv(b1_2020_post_clean,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/pre_post/b1_2020_post_clean.csv')

#prepost
write_csv(b1_2020_pre_post_clean,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Long format/b_2020_pre_post.csv')
#------------------------------------------------------------------------------------------------------------------
#  Merged
b1_2020_prepost_merged <- merge(b1_2020_pre_clean,b1_2020_post_clean, by = "name.icm",all.x = TRUE,all.y = TRUE) 
b1_2020_prepost_merged[b1_2020_prepost_merged$type.x == "pre" & b1_2020_prepost_merged$type.y == "post", prepost_match := "matched"]
b1_2020_prepost_merged[is.na(b1_2020_prepost_merged$prepost_match),prepost_match := "unmatched"]

#b1_2020_prepost_merged
write.csv(b1_2020_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Merged/b1_2020_prepost_merged.csv')

#matched
b1_2020_prepost_matched <- prepost_match2(b1_2020_pre_clean,b1_2020_post_clean)
write.csv(b1_2020_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Merged/matched/b1_2020_prepost_matched.csv')

# Create 2018 B1 codebook
b123_2020_prepost_codebook <- create_codebook(b1_2020_svy,b1_2020_opt_list,b1_2020_pre_var,b1_2020_post_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b123_2020_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Codebook/b123_2020_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2020B1 Pre)
b1_2020_pre_codebook <- for_mstcdB(b1_2020_pre_var,"2020B1 Pre")
b1_2020_post_codebook <- for_mstcdB(b1_2020_post_var,"2020B1 Post")
b1_2020_codebook <- bind_rows(b1_2020_pre_codebook,b1_2020_post_codebook)
write_csv(b1_2020_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/3.FY202021/PrePost_Codebook/b1_2020_codebook.csv')
