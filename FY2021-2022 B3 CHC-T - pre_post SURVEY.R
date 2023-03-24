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

# B3 2021 pre chc tinkering
b3_2021_pre_chc_tk <- read_csv("G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/1.VHLData/13.FY202122/batch3_CHC Tinkering/1.Pre/06_Clean Data/PH_FY21_B3_CHC_Presurvey_de_dupli_consent_only_data.csv")                      

b3_2021_pre_chc_t_var <- select_available(b3_2021_pre_chc_tk)
b3_2021_pre_chc_t_dups <- duplicated_data(b3_2021_pre_chc_t_var)
b3_2021_pre_chc_t_var <- data_needed(b3_2021_pre_chc_t_dups,b3_2021_pre_chc_t_var,"pre","2021")
b3_2021_pre_chc_t_var <- edit_base_level(b3_2021_pre_chc_t_var)
b3_2021_pre_chc_t_var <- tag_gif_programs(b3_2021_pre_chc_t_var,"gif","CHC Tinkering")
# save a raw file
write_csv(b3_2021_pre_chc_t_var,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b3_2021_pre_chc_t_raw.csv')

# survey and choices
b3_2021_chc_t_svy <- read_sheet("https://docs.google.com/spreadsheets/d/198Us23mfIZPrWfUrCx0h48O8MaGY6gnC5O2jviDyZus/edit#gid=2121457295",sheet="survey") %>%  clean_names() 
b3_2021_chc_t_choices <- read_sheet("https://docs.google.com/spreadsheets/d/198Us23mfIZPrWfUrCx0h48O8MaGY6gnC5O2jviDyZus/edit#gid=549137032",sheet="choices") %>%  clean_names() 

#create option list
b3_2021_chc_t_opt_list <- opt_list(b3_2021_chc_t_svy,b3_2021_chc_t_choices)

b3_2021_chc_t_opt_list[name_choices == 1 & MERGE_NAME == "status",label_english_choices := "Participant is available"]
b3_2021_chc_t_opt_list[name_choices == 2 & MERGE_NAME == "status",label_english_choices := "Participant's spouse or partner is available"]
b3_2021_chc_t_opt_list <- b3_2021_chc_t_opt_list %>% data.table()
#Remove duplicated options
b3_2021_chc_t_opt_list[!is.na(b3_2021_chc_t_opt_list$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
b3_2021_chc_t_opt_list <- b3_2021_chc_t_opt_list[!duplicated(b3_2021_chc_t_opt_list$dup_check),]

#Handling Continuous variables
b3_2021_pre_chc_t_int <- clean_integer_cols(b3_2021_pre_chc_t_var,b3_2021_chc_t_svy) %>% data.table()

#Handling Categorical variables
b3_2021_pre_chc_t_var <- b3_2021_pre_chc_t_var %>% data.table()
b3_2021_chc_t_opt_cat <- clean_categorical_cols2(b3_2021_pre_chc_t_var,b3_2021_chc_t_opt_list)

# merging categorical & continuous variables together
b3_2021_pre_chc_t_clean <- merge(b3_2021_chc_t_opt_cat,b3_2021_pre_chc_t_int, by="KEY",all.x = TRUE,all.y = TRUE)

#--changing column names
b3_2021_pre_chc_t_clean <- setnames(b3_2021_pre_chc_t_clean,mstrcodebook_var_names$var_names,mstrcodebook_var_names$new,skip_absent=TRUE)
b3_2021_pre_chc_t_clean <- tag_gif_programs(b3_2021_pre_chc_t_clean,"gif","CHC Tinkering")
#----------------------------------------------------------------------------------------------------------------
# prepost + Ops data
#pre
b3_2021_pre_chct_clean <- b3_2021_pre_chc_t_clean %>% filter(type == "pre")
b3_2021_pre_chct_ops <- merge(b3_2021_pre_chct_clean,ops_data,by = "hhid",all.x = TRUE)
write_csv(b3_2021_pre_chct_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/pre_post/b3_2021_pre_chct_ops.csv')

#post
b3_2021_post_chct_clean <- b3_2021_pre_chc_t_clean %>% filter(type == "post")

#prepost
b3_2021_pre_chc_t_ops <- merge(b3_2021_pre_chc_t_clean,ops_data, by="hhid",all.x = TRUE)
write_csv(b3_2021_pre_chc_t_ops,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Long format/b3_2021_pre_chc_t.csv')
#------------------------------------------------------------------------------------------------------------------
#  Full Merge
b3_2021_prepost_chc_t_merged <- merge(b3_2021_pre_chct_clean,b3_2021_post_chct_clean, by = "hhid",all.x = TRUE,all.y = TRUE)
b3_2021_prepost_chc_t_merged[b3_2021_prepost_chc_t_merged$type.x == "pre" & b3_2021_prepost_chc_t_merged$type.y == "post", prepost_match := "matched"]
b3_2021_prepost_chc_t_merged[is.na(b3_2021_prepost_chc_t_merged$prepost_match),prepost_match := "unmatched"]

b3_2021_prepost_chc_t_merged <- merge(b3_2021_prepost_chc_t_merged,ops_data,by="hhid",all.x = TRUE) 
#b3_2021_prepost_chc_t_merged
write.csv(b3_2021_prepost_chc_t_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Merged/b3_2021_prepost_chc_t_merged.csv')

#matched

# Create 2021 B1 codebook
b3_2021_prepost_chc_t_codebook <- create_codebook(b3_2021_chc_t_svy,b3_2021_chc_t_opt_list,b3_2021_pre_chc_t_var,NULL,mstrcodebook_var_names) # If no post survey data, type NULL
write_csv(b3_2021_prepost_chc_t_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b3_2021_prepost_chc_t_codebook.csv')

#To be used for a combined codebook for all batches   (2021B3 Pre)
b3_2021_pre_chct_codebook <- for_mstcdB(b3_2021_pre_chc_t_var,"2021B3 Pre chct")
#b3_2021_post_chct_codebook <- for_mstcdB(,"2021B3 Post chct")
write_csv(b3_2021_pre_chct_codebook,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/4.FY202122/PrePost_Codebook/b3_2021_chct_codebook.csv')

