###############################################################################
#-----------------Functions to use-------------------------------------
################################################################################
select_cols <- function(pre_post_data,var_names){
  pre_post_cols <- data.table(cols=colnames(pre_post_data))# get column names
  pre_post_cols <- pre_post_cols[,.(cols_updated = ifelse(cols %in% "level","program_name",ifelse(cols %in% "branch","branch_name",
                   ifelse(cols %in% "batch","batch_name",ifelse(cols %in% "program","program_name",
                   ifelse(cols %in% "ops_tf_branch","branch_name",ifelse(cols %in% "ops_tf_batch","batch_name",
                   ifelse(cols %in% "ops_tf_hhid","participant_id",cols)))))))) ]#updated column names
  
  setnames(pre_post_data,colnames(pre_post_data),pre_post_cols$cols_updated)#update column names in the data set
  pre_post_data <- pre_post_data %>% filter(status == 1 || status == 2) %>% filter(consent_a == 1) #respondent available or spouse and consent is yes
  pre_post_data <- pre_post_data[,which(colnames(pre_post_data)%in% var_names)]
  return(pre_post_data)
}
################################################################################
with_part_id <- function(data){
  data <- data[!is.na(data$participant_id),]
  return(data)
}
################################################################################
duplicated_data <- function(data){
  pre_post_dups <- data %>% get_dupes(participant_id)
  b1_2019_pre_dups <- pre_post_dups %>% select(key,start_time,community,base,status,consent_a,participant_id) %>%
    data.table() %>% dplyr::mutate(rownumber = row_number())
}
################################################################################
data_needed <- function(pre_post_dups,pre_post_data,type,year){
  pre_post_dups_arranged <- pre_post_dups %>% arrange(participant_id,start_time)
  dups_data <- pre_post_dups_arranged[duplicated(pre_post_dups_arranged$participant_id),]
  cleaned_data <- anti_join(pre_post_data,dups_data, by= "key")
  cleaned_data$type <- type
  cleaned_data$year <- year
  return(cleaned_data)
}
#data without participants ids
data_needed_name_id <- function(pre_post_dups,pre_post_data,type,year){
  pre_post_dups_arranged <- pre_post_dups %>% arrange(name_id,start_time)
  dups_data <- pre_post_dups_arranged[duplicated(pre_post_dups_arranged$name_id),]
  cleaned_data <- anti_join(pre_post_data,dups_data, by= "key")
  cleaned_data$type <- type
  cleaned_data$year <- year
  return(cleaned_data)
}
################################################################################
opt_list_2 <- function(survey,pre_post_choices){
  for (i in 1:dim(survey)[1]){
    survey$option_name[i] <- ifelse(is.na(str_split(survey$type, " ")[[i]][2]), ifelse(str_split(survey$type, " ")[[i]][1]=="integer", "integer", NA), str_split(survey$type, " ")[[i]][2])
  }
  survey <- survey %>% select(type,name,label_english,option_name) %>% data.table()
  survey$MERGE_NAME <- survey$name 
  
  pre_post_choices <- pre_post_choices[,1:3]
  setnames(pre_post_choices,colnames(pre_post_choices),c("option_name","name_choices","label_english_choices"))
  surv_choices <- left_join(survey,pre_post_choices,by="option_name") %>% data.table()
  surv_choices <- surv_choices[!surv_choices$label_english_choices == "NULL"] %>% data.table()
  non_rep_qns <- surv_choices #keeping a copy for non repeat questions
  
  #selecting questions in repeat groups, eg emp, a_q3_enroll_cur
  data_frame <- surv_choices %>% filter(MERGE_NAME%like%"emp") %>% dplyr::mutate(row_num = row_number())
  enrolled <- non_rep_qns %>% filter(MERGE_NAME%like%"a_q3_enroll_cur" )  #Filtering a_q3_enroll_cur separately
  a_q <- non_rep_qns %>% filter(MERGE_NAME%like%"a_q" )
  con <- non_rep_qns %>% filter(MERGE_NAME%like%"con" )
  e_q <- non_rep_qns %>% filter(MERGE_NAME%like%"e_q" )
  rel_hh <- non_rep_qns %>% filter(MERGE_NAME%like%"rel_hh" )
  overseas <- non_rep_qns %>% filter(MERGE_NAME%like%"overseas" )
  perm_home <- non_rep_qns %>% filter(MERGE_NAME%like%"perm_home" )
  check_age <- non_rep_qns %>% filter(MERGE_NAME%like%"check_age" )
  hh_age <- non_rep_qns %>% filter(MERGE_NAME%like%"hh_age" )
  data_frame <- bind_rows(data_frame,enrolled,a_q,con,e_q,rel_hh,overseas,perm_home,hh_age,check_age)
  
  vec <- seq_len(25)  #Number of time the input data frame to be duplicated
  leng <- dim(data_frame)[1]#getting length of the input data frame
  
  data_frame <- purrr::map_dfr(vec, ~data_frame)
  data_frame$occurances <- rep(1:dim(data_frame)[1], each = leng, len = dim(data_frame)[1])
  data_frame <- data_frame %>% data.table()
  data_frame[!(is.na(MERGE_NAME)),MERGE_NAME := paste0(MERGE_NAME,"_",occurances)]
  rep_qns <- data_frame %>% select(-c("occurances","row_num"))# for columns with _1,_2,_3,...._21
  
  opt_list_updated <- bind_rows(non_rep_qns,rep_qns) %>% data.table()
  drop_cols <- c("base","surveyor_id","surveyor_id_check","sys_community_id","sys_community_id_check","name_icm_id","name_icm_id_check","region","province","city","barangay","community","community_check","name_icm","name_icm_check")
  opt_list_updated <- opt_list_updated[!opt_list_updated$MERGE_NAME%in%drop_cols,]
  
  return(opt_list_updated)
  
}












