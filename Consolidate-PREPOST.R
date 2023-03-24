library(data.table)
# For appended prepost
#*****************************************************************************************************************
all_pre_post_cleaned <- rbind.fill(
  #2018
  b1_2018_pre,
  b2_2018_prepost,
  b3_2018_pre_post,
  #2019
  b1_2019_pre_post,
  b2_2019_pre_post,
  b3_2019_pre_post,
  #2020
  b_2020_pre_post,
  #2021
  strech_v1_pre_post,
  b2_2021_drct_pre_post,
  b2_2021_pre_stretch_v2,
  b3_2021_pre_chc_t,
  b3_2021_pre_trct,
  #2022
  b1_2022_pre_irct,
  b2_2022_pre_irct,
  b3_2022_pre_rrct
) 

# all_pre_post_cleaned <- rbind(
#   #2018
#   b1_2018_pre_ops,
#   b2_2018_prepost_ops,
#   b3_2018_pre_post_ops,
#   #2019
#   b1_2019_pre_post_ops,
#   b2_2019_pre_post_ops,
#   b3_2019_pre_post_ops,
#   #2020
#   b1_2020_pre_post_clean,
#   #2021
#   b1_2021_strech_v1_prepost_ops,
#   b2_2021_drct_prepost_ops,
#   b2_2021_pre_stretch_v2_ops,
#   b3_2021_pre_chc_t_ops,
#   b3_2021_pre_trct_ops,
#   #2022
#   b1_2022_pre_irct__ops,
#   b2_2022_pre_irct__ops,
#   b3_2022_pre_rrct__ops, #
#   use.names=TRUE, fill = TRUE
# ) 

write.csv(all_pre_post_cleaned,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/prepost-combined/prepost_appended/all_pre_post_cleaned.csv')

########################################################################################################################

#******************************************************************************************************************
#For matched/unmatched prepost
remove_date <- function(data){ #remove date columns inorder to bind pre/post datasets, since dates disturbs the rbind function if of different data-types
  col_remove <- c(grep(names(data), pattern = "mem.birthday.", value = T),grep(names(data), pattern = "preload_date", value = T))
  data <- data[, -col_remove, with = F]
  return(data)
}

b1_2018_prepost_merged <- remove_date(b1_2018_prepost_merged)
b2_2018_prepost_merged <- remove_date(b2_2018_prepost_merged)
b3_2018_prepost_merged <- remove_date(b3_2018_prepost_merged)
b1_2019_prepost_merged <- remove_date(b1_2019_prepost_merged)
b2_2019_prepost_merged <- remove_date(b2_2019_prepost_merged)
b3_2019_prepost_merged <- remove_date(b3_2019_prepost_merged)
b1_2020_prepost_merged <- remove_date(b1_2020_prepost_merged)
b1_2021_prepost_tfsv1_merged <- remove_date(b1_2021_prepost_tfsv1_merged)
b2_2021_drct_prepost_merged <- remove_date(b2_2021_drct_prepost_merged)
b2_2021_prepost_tfsv2_merged <- remove_date(b2_2021_prepost_tfsv2_merged)
b3_2021_prepost_chc_t_merged <- remove_date(b3_2021_prepost_chc_t_merged)
B3_2021_prepost_trct_merged <- remove_date(B3_2021_prepost_trct_merged)
b1_2022_prepost_irct_merged <- remove_date(b1_2022_prepost_irct_merged)
b2_2022_prepost_irct_merged <- remove_date(b2_2022_prepost_irct_merged)
b3_2022_prepost_rrct_merged <- remove_date(b3_2022_prepost_rrct_merged)  

prepost_ops_all_merged <- rbind(
  #2018
  b1_2018_prepost_merged,b2_2018_prepost_merged,b3_2018_prepost_merged,
  #2019
   b1_2019_prepost_merged,b2_2019_prepost_merged,b3_2019_prepost_merged,
  #2020
   b1_2020_prepost_merged,
  #2021
   b1_2021_prepost_tfsv1_merged,
   b2_2021_drct_prepost_merged,b2_2021_prepost_tfsv2_merged,
   b3_2021_prepost_chc_t_merged, B3_2021_prepost_trct_merged,
  #2022
   b1_2022_prepost_irct_merged,b2_2022_prepost_irct_merged,b3_2022_prepost_rrct_merged,
  fill = TRUE,use.names=TRUE 
)

write.csv(prepost_ops_all_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/prepost-combined/prepost_matched/prepost_ops_all_merged.csv')

###########################################################################################################################

#matched prepost/ops (All match)
b2_2018_prepostops_matched <- b2_2018_prepostops_matched %>% data.table()
b2_2018_prepostops_matched <- flatten_lists_in_data(b2_2018_prepostops_matched)
#
b3_2018_prepostops_matched <- b3_2018_prepostops_matched %>% data.table()
b3_2018_prepostops_matched <- flatten_lists_in_data(b3_2018_prepostops_matched)
#
b1_2019_prepost_matched <- b1_2019_prepost_matched %>% data.table()
b1_2019_prepost_matched <- flatten_lists_in_data(b1_2019_prepost_matched)
#
b2_2019_prepost_matched <- b2_2019_prepost_matched %>% data.table()
b2_2019_prepost_matched <- flatten_lists_in_data(b2_2019_prepost_matched)
#
b3_2019_prepost_matched <- b3_2019_prepost_matched %>% data.table()
b3_2019_prepost_matched <- flatten_lists_in_data(b3_2019_prepost_matched)
#
b1_2020_prepost_matched <- b1_2020_prepost_matched %>% data.table()
b1_2020_prepost_matched <- flatten_lists_in_data(b1_2020_prepost_matched)
#
b1_2021_tsv1_prepost_matched <- b1_2021_tsv1_prepost_matched %>% data.table()
b1_2021_tsv1_prepost_matched <- flatten_lists_in_data(b1_2021_tsv1_prepost_matched)
#
b2_2021_drct_prepost_matched <- b2_2021_drct_prepost_matched %>% data.table()
b2_2021_drct_prepost_matched <- flatten_lists_in_data(b2_2021_drct_prepost_matched)
#
b2_2021_tsv2_prepost_matched <- b2_2021_tsv2_prepost_matched %>% data.table()
b2_2021_tsv2_prepost_matched <- flatten_lists_in_data(b2_2021_tsv2_prepost_matched)

prepost_ops_all_matched <- rbind.fill(
  #2018
  b2_2018_prepostops_matched,b3_2018_prepostops_matched,
  #2019
  b1_2019_prepost_matched,b2_2019_prepost_matched,b3_2019_prepost_matched,
  #2020
  b1_2020_prepost_matched,
  #2021
  b1_2021_tsv1_prepost_matched,b2_2021_drct_prepost_matched,b2_2021_tsv2_prepost_matched
  #use.names=TRUE, fill = TRUE
)
write.csv(prepost_ops_all_matched,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/prepost-combined/prepost_matched/prepost_ops_matched.csv') 


