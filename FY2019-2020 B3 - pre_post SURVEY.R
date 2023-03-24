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
b3_2019_pre_post <- tag_gif_programs(b3_2019_pre_post,"none-gif","none-gif") %>% data.table()
# saving a raw file for b3_2019_pre_post
write_csv(b3_2019_pre_post,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b3_2019_pre_post_raw.csv')

# survey and choices
b3_2019_pre_post_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1r5leAPD9Pev4IP6eGJUlSIyw3IxozPEWiw50KJlSi-I/edit#gid=0",sheet="survey") %>%  clean_names()
b3_2019_pre_post_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1r5leAPD9Pev4IP6eGJUlSIyw3IxozPEWiw50KJlSi-I/edit#gid=1509898419",sheet="choices") %>%  clean_names()

#create option list to update the prepost categorical variable from
b3_2019_opt_list <- opt_list(b3_2019_pre_post_svy,b3_2019_pre_post_choices)

b3_2019_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b3_2019_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]

#Handling Continuous variables
b3_2019_pre_post_int <- clean_integer_cols(b3_2019_pre_post,b3_2019_pre_post_svy) %>% data.table()

#Handling Categorical variables
b3_2019_pre_post_cat <- clean_categorical_cols(b3_2019_pre_post,b3_2019_opt_list)

# merging categorical & continuous variables together
b3_2019_pre_post_clean <- merge(b3_2019_pre_post_cat,b3_2019_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)
b3_2019_pre_post_clean$batch <- "FY19-20 B3"

##--changing column names
b3_2019_pre_post_clean <- setnames(b3_2019_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b3_2019_pre_post_clean <- tag_gif_programs(b3_2019_pre_post_clean,"none-gif","none-gif")
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
#  Merged
b3_2019_prepost_merged <- merge(b3_2019_pre_clean,b3_2019_post_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b3_2019_prepost_merged[b3_2019_prepost_merged$type.x == "pre" & b3_2019_prepost_merged$type.y == "post", prepost_match := "matched"]
b3_2019_prepost_merged[is.na(b3_2019_prepost_merged$prepost_match),prepost_match := "unmatched"]

b3_2019_prepost_merged <- merge(b3_2019_prepost_merged,ops_data,by="hhid",all.x = TRUE)
# save b3_2019_prepost_merged
write.csv(b3_2019_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/b3_2019_prepost_merged.csv')

#matched
b3_2019_prepost_matched <- prepost_match(b3_2019_pre_clean,b3_2019_post_clean)
names(b3_2019_prepost_matched)[names(b3_2019_prepost_matched) == "hhid.x"] <- "hhid"
b3_2019_prepost_matched <- merge(b3_2019_prepost_matched,ops_data, by = "hhid")
write.csv(b3_2019_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/matched/b3_2019_prepost_matched.csv')

##* Create 2018 B1 codebook
b3_2019_prepost_codebook <- create_codebook(b3_2019_pre_post_svy,b3_2019_opt_list,b3_2019_pre_var,b3_2019_post_var,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b3_2019_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b3_2019_prepost_codebook.csv')

#To be used for a combined codebook for all batches   (2019B3 Pre)
b3_2019_pre_codebook <- for_mstcdB(b3_2019_pre_var,"2019B3 Pre")
b3_2019_post_codebook <- for_mstcdB(b3_2019_post_var,"2019B3 Post")
b3_2019_codebook <- bind_rows(b3_2019_pre_codebook,b3_2019_post_codebook)
write_csv(b3_2019_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b3_2019_codebookcsv')
