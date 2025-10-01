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

# Batch 1 2019 pre --  TF 1
b1_2019_pre_tf1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B1/2019B1_PRE_L1.csv")  

b1_2019_pre_tf1_var <- select_available(b1_2019_pre_tf1)
b1_2019_pre_tf1_dups <- duplicated_data(b1_2019_pre_tf1_var)
b1_2019_pre_tf1_var <- data_needed(b1_2019_pre_tf1_dups,b1_2019_pre_tf1_var,"pre","2019")
b1_2019_pre_tf1_var$program_name <- "Transform Level 1"

#--- TF 2
b1_2019_pre_tf2 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B1/2019B1_PRE_L2.csv") 

b1_2019_pre_tf2_var <- select_available(b1_2019_pre_tf2)
b1_2019_pre_tf2_dups <- duplicated_data(b1_2019_pre_tf2_var)
b1_2019_pre_tf2_var <- data_needed(b1_2019_pre_tf2_dups,b1_2019_pre_tf2_var,"pre","2019")
b1_2019_pre_tf2_var$program_name <- "Transform Level 2"
b1_2019_pre_tf2_var$oinc5_oth <- as.character(b1_2019_pre_tf2_var$oinc5_oth)

# TF1 PRE + TF2 PRE
b1_2019_pre <-  bind_rows(b1_2019_pre_tf1_var,b1_2019_pre_tf2_var)

#Batch 1 2019 post
# ---  TF 1
b1_2019_post_tf1 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B1/2019B1_POST_L1.csv") 
b1_2019_post_tf1_var <- select_available(b1_2019_post_tf1)
b1_2019_post_tf1_dups <- duplicated_data(b1_2019_post_tf1_var)
b1_2019_post_tf1_var <- data_needed(b1_2019_post_tf1_dups,b1_2019_post_tf1_var,"post","2019")
b1_2019_post_tf1_var$program_name <- "Transform Level 1"

# ---  TF 2
b1_2019_post_tf2 <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/2019B1/2019B1_POST_L2.csv")  
b1_2019_post_tf2_var <- select_available(b1_2019_post_tf2)
b1_2019_post_tf2_dups <- duplicated_data(b1_2019_post_tf2_var)
b1_2019_post_tf2_var <- data_needed(b1_2019_post_tf2_dups,b1_2019_post_tf2_var,"post","2019")
b1_2019_post_tf2_var$program_name <- "Transform Level 2"

#TF1 POST + TF2 POST
b1_2019_post <- bind_rows(b1_2019_post_tf1_var,b1_2019_post_tf2_var)
#pre + post
b1_2019_pre_post <- bind_rows(b1_2019_pre,b1_2019_post)
b1_2019_pre_post <- edit_base_level(b1_2019_pre_post)
b1_2019_pre_post <- b1_2019_pre_post %>% data.table()
b1_2019_pre_post <- tag_gif_programs(b1_2019_pre_post,"none-gif","none-gif") %>% data.table()
# saving a raw file for b2_2018_pre_post
write_csv(b1_2019_pre_post,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b1_2019_pre_post_raw.csv')

#-----Reading in the Survey questionnaire and Choices
b1_2019_pre_post_suvy <- read_sheet("https://docs.google.com/spreadsheets/d/1KPWvLBxcQtqNJKf9TdrX9ipQhDlcnmnFR3BOf6tsofU/edit#gid=1529439020",sheet="survey") %>%  clean_names() 
b1_2019_pre_post_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1KPWvLBxcQtqNJKf9TdrX9ipQhDlcnmnFR3BOf6tsofU/edit#gid=1286014469",sheet="choices") %>%  clean_names() 

#create option list
b1_2019_opt_list <- opt_list(b1_2019_pre_post_suvy,b1_2019_pre_post_choices)

b1_2019_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2019_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
b1_2019_opt_list <- b1_2019_opt_list[b1_2019_opt_list$MERGE_NAME != "community",]

#-- Handling Continuous variables
b1_2019_pre_post_suvy <- b1_2019_pre_post_suvy %>% data.table()
b1_2019_pre_post_int <- clean_integer_cols(b1_2019_pre_post,b1_2019_pre_post_suvy) %>% data.table()#integers

#Handling Categorical variables
b1_2019_pre_post_cat <- clean_categorical_cols(b1_2019_pre_post,b1_2019_opt_list)

#merging categorical & continuous variables together
b1_2019_pre_post_clean <- merge(b1_2019_pre_post_cat,b1_2019_pre_post_int, by="KEY",all.x = TRUE,all.y = TRUE)
b1_2019_pre_post_clean$batch <- "FY19-20 B1"

##--changing column names
b1_2019_pre_post_clean <- setnames(b1_2019_pre_post_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b1_2019_pre_post_clean <- tag_gif_programs(b1_2019_pre_post_clean,"none-gif","none-gif")
#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre + Ops data
b1_2019_pre_clean <- b1_2019_pre_post_clean %>% filter(type == "pre")
b1_2019_pre_ops <- merge(b1_2019_pre_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2019_pre_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b1_2019_pre_ops.csv')

#post + Ops data
b1_2019_post_clean <- b1_2019_pre_post_clean %>% filter(type == "post")
b1_2019_post_ops <- merge(b1_2019_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2019_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/pre_post/b1_2019_post_ops.csv')

# prepost + ops
b1_2019_pre_post_ops <- merge(b1_2019_pre_post_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2019_pre_post_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Long format/b1_2019_pre_post.csv')
#-----------------------------------------------------------------------------------------------------------------

# Matched/unmatched  b1_2019_prepost_matched
b1_2019_prepost_merged <- merge(b1_2019_pre_clean,b1_2019_post_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b1_2019_prepost_merged[b1_2019_prepost_merged$type.x == "pre" & b1_2019_prepost_merged$type.y == "post", prepost_match := "matched"]
b1_2019_prepost_merged[is.na(b1_2019_prepost_merged$prepost_match),prepost_match := "unmatched"]
# + ops data
b1_2019_prepost_merged <- merge(b1_2019_prepost_merged,ops_data,by="hhid",all.x = TRUE)
write.csv(b1_2019_prepost_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/b1_2019_prepost_merged.csv')

# Matched
b1_2019_prepost_matched <- prepost_match(b1_2019_pre_clean,b1_2019_post_clean)
ops_data$hhid.x <- ops_data$hhid
b1_2019_prepost_matched <- merge(b1_2019_prepost_matched,ops_data, by = "hhid.x")
write.csv(b1_2019_prepost_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Merged/matched/b1_2019_prepost_matched.csv')

# Create 2018 B1 codebook
b1_2019_prepost_codebook <- create_codebook(b1_2019_pre_post_suvy,b1_2019_opt_list,b1_2019_pre,b1_2019_post,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b1_2019_prepost_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b1_2019_prepost_codebook.csv')

#T o be used for a combined codebook for all batches   (2019B1 Pre)
b1_2019_pre_codebook <- for_mstcdB(b1_2019_pre,"2019B1 Pre")
b1_2019_post_codebook <- for_mstcdB(b1_2019_post,"2019B1 Post")
b1_2019_codebook <- bind_rows(b1_2019_pre_codebook,b1_2019_post_codebook)
write_csv(b1_2019_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/2.FY201920/PrePost_Codebook/b1_2019_codebook.csv')

