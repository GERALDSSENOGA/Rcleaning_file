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
# I-RCT B1 2022
#b1_2022_pre_irct <-  read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/14.FY202223/Batch1_Integrated RCT/1.Pre/06_Clean Data/PH_FY22_B1_Presurvey_de_dupli_consent_only_data.csv") 
b1_2022_pre_irct <- read_csv("G:/.shortcut-targets-by-id/16EhUWdTxBkf64D2mmSIBHpgc_JVRS4b8/Batch1_Integrated RCT/1.Pre/06_Clean Data/PH_FY22_B1_Presurvey_de_dupli_consent_only_data.csv")

b1_2022_pre_irct_var <- select_available(b1_2022_pre_irct)
b1_2022_pre_irct_dups <- duplicated_data(b1_2022_pre_irct_var)
b1_2022_pre_irct_var <- data_needed(b1_2022_pre_irct_dups,b1_2022_pre_irct_var,"pre","2022")
b1_2022_pre_irct_var <- edit_base_level(b1_2022_pre_irct_var) %>% data.table()
b1_2022_pre_irct_var <- tag_gif_programs(b1_2022_pre_irct_var,"gif","Integrated RCT")
write_csv(b1_2022_pre_irct_var,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/pre_post/b1_2022_pre_irct_raw.csv')

# survey and choices
b1_2022_irct_svy <- read_sheet("https://docs.google.com/spreadsheets/d/1Wunal-BhoEzjaKwlzZL79589ZLnLR5WzCOaYhwk0QQQ/edit#gid=2121457295",sheet="survey") %>%  clean_names() %>% clean_names()
b1_2022_irct_choices <- read_sheet("https://docs.google.com/spreadsheets/d/1Wunal-BhoEzjaKwlzZL79589ZLnLR5WzCOaYhwk0QQQ/edit#gid=549137032",sheet="choices") %>%  clean_names() %>% clean_names()

#create option list
b1_2022_irct_opt_list <- opt_list(b1_2022_irct_svy,b1_2022_irct_choices)

b1_2022_irct_opt_list <- b1_2022_irct_opt_list %>% data.table()
b1_2022_irct_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b1_2022_irct_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
#Remove duplicated options
b1_2022_irct_opt_list[!is.na(b1_2022_irct_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b1_2022_irct_opt_list <- b1_2022_irct_opt_list[!duplicated(b1_2022_irct_opt_list$dup_check),]

#Handling Continuous variables
b1_2022_pre_irct_int <- clean_integer_cols(b1_2022_pre_irct_var,b1_2022_irct_svy) %>% data.table()

#Handling Categorical variables
b1_2022_pre_irct_cat <- clean_categorical_cols(b1_2022_pre_irct_var,b1_2022_irct_opt_list)

# merging categorical & continuous variables together

b1_2022_pre_irct_clean <- merge(b1_2022_pre_irct_cat,b1_2022_pre_irct_int, by="KEY",all.x = TRUE,all.y = TRUE)

#--changing column names
b1_2022_pre_irct_clean <- setnames(b1_2022_pre_irct_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b1_2022_pre_irct_clean <- tag_gif_programs(b1_2022_pre_irct_clean,"gif","Integrated RCT")
#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b1_2022_pre_irct_clean <- b1_2022_pre_irct_clean %>% filter(type == "pre")
b1_2022_pre_irct_ops <- merge(b1_2022_pre_irct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b1_2022_pre_irct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/pre_post/b1_2022_pre_irct_ops.csv')

#post
b1_2022_post_irct_clean <- b1_2022_pre_irct_clean %>% filter(type == "post")

#prepost
b1_2022_pre_irct__ops <- merge(b1_2022_pre_irct_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b1_2022_pre_irct__ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Long format/b1_2022_pre_irct.csv')
#------------------------------------------------------------------------------------------------------------------
#  Matched
b1_2022_prepost_irct_merged <- merge(b1_2022_pre_irct_clean,b1_2022_post_irct_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b1_2022_prepost_irct_merged[b1_2022_prepost_irct_merged$type.x == "pre" & b1_2022_prepost_irct_merged$type.y == "post", prepost_match := "matched"]
b1_2022_prepost_irct_merged[is.na(b1_2022_prepost_irct_merged$prepost_match),prepost_match := "unmatched"]

b1_2022_prepost_irct_merged <- merge(b1_2022_prepost_irct_merged,ops_data,by="hhid",all.x = TRUE) 
#b1_2022_prepost_irct_merged
write.csv(b1_2022_prepost_irct_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Merged/b1_2022_prepost_irct_merged.csv')

#matched

##* Create 2021 B1 codebook
b1_2022_prepost_irct_codebook <- create_codebook(b1_2022_irct_svy,b1_2022_irct_opt_list,b1_2022_pre_irct_var,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b1_2022_prepost_irct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Codebook/b1_2022_prepost_irct_codebook.csv')

#To be used for a combined codebook for all batches   (2022B1 Pre)
b1_2022_pre_irct_codebook <- for_mstcdB(b1_2022_pre_irct_var,"2022B1 Pre irct")
#b1_2022_post_irct_codebook <- for_mstcdB(,"2022B1 Post irct")
write_csv(b1_2022_pre_irct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/5.FY202223/PrePost_Codebook/b1_2022_irct_codebook.csv')


