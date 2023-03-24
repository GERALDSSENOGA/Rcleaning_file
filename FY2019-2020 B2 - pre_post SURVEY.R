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
b2_2019_pre_post <- tag_gif_programs(b2_2019_pre_post,"none-gif","none-gif") %>% data.table()
# saving a raw file for b2_2019_pre_post
write_csv(b2_2019_pre_post,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b2_2019_pre_post_raw.csv')

#survey and choices
b2_2019_post_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1Gl5RDp0qp7E7rfDCfcxD6vjo8Stl-3DlktGL1sgvkS0/edit#gid=1334303542",sheet="survey") %>%  clean_names()
b2_2019__choices <- read_sheet("https://docs.google.com/spreadsheets/d/1Gl5RDp0qp7E7rfDCfcxD6vjo8Stl-3DlktGL1sgvkS0/edit#gid=2032008205",sheet="choices") %>%  clean_names()

#Create option list
b2_2019_opt_list <- opt_list(b2_2019_post_svy,b2_2019__choices)
b2_2019_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b2_2019_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

#Handling Continuous variables
b2_2019_pre_post_int <- clean_integer_cols(b2_2019_pre_post,b2_2019_post_svy) #%>% data.table()

#Handling Categorical variables
b2_2019_pre_post_cat <- clean_categorical_cols(b2_2019_pre_post,b2_2019_opt_list)

# merging categorical & continuous variables together
b2_2019_pre_post_clean <- merge(b2_2019_pre_post_cat,b2_2019_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)
b2_2019_pre_post_clean$batch <- "FY19-20 B2"

##--changing column names
b2_2019_pre_post_clean <- setnames(b2_2019_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b2_2019_pre_post_clean <- tag_gif_programs(b2_2019_pre_post_clean,"none-gif","none-gif")
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
names(b2_2019_prepost_matched)[names(b2_2019_prepost_matched) == "hhid.x"] <- "hhid"
b2_2019_prepost_matched <- merge(b2_2019_prepost_matched,ops_data, by = "hhid")
write.csv(b2_2019_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/matched/b2_2019_prepost_matched.csv')

##* Create 2018 B1 codebook
b2_2019_prepost_codebook <- create_codebook(b2_2019_post_svy,b2_2019_opt_list,b2_2019_pre,b2_2019_post_tf12_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b2_2019_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b2_2019_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2019B1 Pre)
b2_2019_pre_codebook <- for_mstcdB(b2_2019_pre,"2019B2 Pre")
b2_2019_post_codebook <- for_mstcdB(b2_2019_post_tf12_var,"2019B2 Post")
b2_2019_codebook <- bind_rows(b2_2019_pre_codebook,b2_2019_post_codebook)
write_csv(b2_2019_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b2_2019_codebook.csv')
