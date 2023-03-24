###########################################################################################################
#-----------------Functions to use in cleaning pre-post -----------------------
#-----------------Created by: Gerald S ----------------------------------------
#-----------------Year: 2022  -------------------------------------------------
############################################################################################################
select_available <- function(pre_post_data){
  pre_post_cols <- data.table(cols=colnames(pre_post_data))# get column names
  pre_post_cols <- pre_post_cols[,.(cols_updated = ifelse(cols %in% "level","program_name",ifelse(cols %in% "branch","branch_name",
                   ifelse(cols %in% "batch","batch_name",ifelse(cols %in% "program","program_name",
                   ifelse(cols %in% "ops_tf_branch","branch_name",ifelse(cols %in% "ops_tf_batch","batch_name",
                   ifelse(cols %in% "ops_tf_hhid","participant_id",cols)))))))) ]#updated column names
  
  setnames(pre_post_data,colnames(pre_post_data),pre_post_cols$cols_updated)#update column names in the data set
  pre_post_data <- pre_post_data %>% filter(status == 1 || status == 2) %>% filter(consent_a == 1) #respondent available or spouse and consent is yes
  return(pre_post_data)
}
############################################################################################################
duplicated_data <- function(data){
  pre_post_dups <- data %>% get_dupes(participant_id)
  b1_2019_pre_dups <- pre_post_dups %>% select(KEY,start_time,community,base,status,consent_a,participant_id) %>%
    data.table() %>% dplyr::mutate(rownumber = row_number())
}

duplicated_data_2 <- function(data){
  pre_post_dups <- data %>% get_dupes(sys_participant_id)
  b1_2019_pre_dups <- pre_post_dups %>% select(KEY,start_time,community,base,status,consent_a,participant_id) %>%
    data.table() %>% dplyr::mutate(rownumber = row_number())
}
############################################################################################################
data_needed <- function(pre_post_dups,pre_post_data,type,year){
  pre_post_dups_arranged <- pre_post_dups %>% arrange(start_time,participant_id)
  dups_data <- pre_post_dups_arranged[duplicated(pre_post_dups_arranged$participant_id),]
  cleaned_data <- anti_join(pre_post_data,dups_data, by= "KEY")
  cleaned_data$type <- type
  cleaned_data$year <- year
  return(cleaned_data)
}

#data without participants ids
data_needed_name_id <- function(pre_post_dups,pre_post_data,type,year){
  pre_post_dups_arranged <- pre_post_dups %>% arrange(name_id,start_time)
  dups_data <- pre_post_dups_arranged[duplicated(pre_post_dups_arranged$name_id),]
  cleaned_data <- anti_join(pre_post_data,dups_data, by= "KEY")
  cleaned_data$type <- type
  cleaned_data$year <- year
  return(cleaned_data)
}

############################################################################################################################
opt_list <- function(survey,pre_post_choices){
  #survey
  survey <- survey %>% data.table()
  survey <- survey[!is.na(name)]
  survey[type %like% "select_", option_name := tstrsplit(type, split = " ", keep = 2)]
  survey[, index := .I]
  survey <- survey %>% select(type,name,label_english,option_name,index) %>% data.table()
  #choices
  pre_post_choices <- pre_post_choices[,1:3]
  setnames(pre_post_choices,colnames(pre_post_choices),c("option_name","name_choices","label_english_choices"))
  #Merging the choices to the survey list_name to get the values and their corresponding option names
  surv_choices <- left_join(survey,pre_post_choices,by="option_name") %>% data.table()
  surv_choices$MERGE_NAME <- surv_choices$name
  #get questions in repeat groups
  for(i in unique(surv_choices[type %like% "repeat", name])){
    
    repeat_indexes <- surv_choices[type %like% "repeat" & name == i, index]
    if(length(repeat_indexes)== 2){   
      #look for bounds of the begin/repeat group 
      surv_choices[index %in% repeat_indexes[1]:repeat_indexes[2], `:=` (repeat_name = i)]
    }
    
  }
  # save repeat group variable and non repeat group variable separate
  surv_choices <- surv_choices[!surv_choices$label_english_choices == "NULL"] %>% data.table()
  non_repeat_qnz <- surv_choices[is.na(surv_choices$repeat_name),] %>% select(-c("repeat_name","index"))
  repeat_qnz <- surv_choices[!is.na(surv_choices$repeat_name),]
  #Each repeat group variable name to repeat 25X ,considering 25 to be the maximum household size
  vec <- seq_len(25)  #Number of times the input data frame to be duplicated
  leng <- dim(repeat_qnz)[1]#getting length of the input data frame
  
  repeat_qnz <- purrr::map_dfr(vec, ~ repeat_qnz)
  repeat_qnz$occurances <- rep(1:dim(repeat_qnz)[1], each = leng, len = dim(repeat_qnz)[1])
  repeat_qnz <- repeat_qnz %>% data.table()
  repeat_qnz[!(is.na(MERGE_NAME)),MERGE_NAME := paste0(MERGE_NAME,"_",occurances)]
  rep_qns <- repeat_qnz %>% select(-c("occurances","repeat_name","index"))# for columns with _1,_2,_3,...._25
  
  opt_list_updated <- bind_rows(non_repeat_qnz,rep_qns) %>% data.table()
  drop_cols <- c("base","surveyor_id","surveyor_id_check","sys_community_id","sys_community_id_check","name_icm_id","name_icm_id_check","region","province","city","sys_participant_id","name_icm_full","barangay","community","community_check","name_icm","name_icm_check")
  opt_list_updated <- opt_list_updated[!opt_list_updated$MERGE_NAME%in%drop_cols,]
  opt_list_updated <- opt_list_updated %>% select("type","name","label_english","option_name","MERGE_NAME","name_choices","label_english_choices")
  #Check and drop duplicates
  opt_list_updated[!is.na(opt_list_updated$name_choices),dup_check := paste0(name_choices,"-",label_english_choices,"-",MERGE_NAME)]
  opt_list_updated <- opt_list_updated[!duplicated(opt_list_updated$dup_check),] %>% select(-c("dup_check"))
  return(opt_list_updated)
  
}
#######################################################################################################################################
clean_integer_cols <- function(prepost_data,survey){
  constants <- c("type","year","base","branch_name","batch_name","program_name","sys_community_id","sys_participant_id","participant_id","name_id","name_icm","start_time","hh_age_1","hh_age_2","a_q2_1","a_q2_2")  # Columns that do not change  
  
  # Select from the survey where the type is decimal, integers and calculated
  int_dec_cal <- survey[survey$type%in%c("decimal","integer","calculate") ,]
  #other columns needed
  other_col <- survey[survey$type%in%c("geopoint","end","date","note","text","deviceid","today") ,]
  other_col <- other_col$name
  other_col <- c(other_col,constants,"KEY")
  other_dt <- subset(prepost_data, select = intersect(colnames(prepost_data),other_col) )
  # Get calculated variable names
  cal_cols <- int_dec_cal[int_dec_cal$type%in%"calculate",]
  cal_cols <- cal_cols$name
  # Get integers and decimal variable names
  int_dec_cols <- int_dec_cal[int_dec_cal$type%in%c("decimal","integer"),]
  int_dec_cols <- int_dec_cols$name
  # All columns needed without categorical variables at this level
  continuous_vars <- c(cal_cols,int_dec_cols,"KEY")
  continuous_vars <- union(continuous_vars,continuous_vars) #remove duplicate columns that might be there
  continuous_vars <- continuous_vars[!continuous_vars%in%other_dt]
  # selecting columns to take/clean
  dt1 <- subset(prepost_data, select = intersect(colnames(prepost_data),continuous_vars) ) %>% data.table()
  dt1 <- dt1 %>% select(-which(names(dt1)%in%constants))
  #--integers zeros and less
  zeroes_na_2null <- function(i){ # function to be applied to all integers and decimal variable names with zeros and NA
    ifelse(i < 0,"",i)
  }
  dt2 <- data.frame(lapply(dt1,zeroes_na_2null)) #setting all zeros and NA to NULL
  # Remove participants id sice we already selected it in dt1 by default
  remove_part_id <- function(a){
    for( i in names(a)){
      if(i == "participant_id"){
        a <- a %>% select(-c(i))
      }
    }
    return(a)
  }
  dt2 <- remove_part_id(dt2)
  #
  for( i in int_dec_cols){
    if(i %in% names(dt2)){
      dt2[,i] <- as.integer( dt2[,i] )
    }
  }
  ##
  dt2 <- merge(dt2,other_dt, by = "KEY",all.x = TRUE)
  
  return(dt2)
}

#########################################################################################################################################
edit_base_level <- function(data){
  data <- data %>% data.table()
    data[program_name == "L1",program_name := "Transform Level 1"]
    data[program_name == "L2",program_name := "Transform Level 2"]
  data[base == "01-Bacolod",base := "Bacolod"]
  data[base == "01 - Bacolod",base := "Bacolod"]
  data[base == "02 - Bohol",base := "Bohol"]
  data[base == "02-Bohol",base := "Bohol"]
  data[base == "03 - Dumaguete",base := "Dumaguete"]
  data[base == "03-Dumaguete",base := "Dumaguete"]
  data[base == "04 - General Santos",base := "General Santos"]
  data[base == "04 - Gensan",base := "General Santos"]
  data[base == "04-General Santos",base := "General Santos"]
  data[base == "05 - Koronadal",base := "Koronadal"]
  data[base == "05-Koronadal",base := "Koronadal"]
  data[base == "06 - Palawan",base := "Palawan"]
  data[base == "07 - Dipolog",base := "Dipolog"]
  data[base == "06-Palawan",base := "Palawan"]
  data[base == "07-Dipolog",base := "Dipolog"]
  data[base == "08 - Panay",base := "Iloilo"]
  data[base == "09 - Cebu",base := "Cebu"]
  data[base == "08-Iloilo",base := "Iloilo"]
  data[base == "08 - Iloilo",base := "Iloilo"]
  data[base == "09-Cebu",base := "Cebu"]
  data[base == "10-Kalibo",base := "Kalibo"]
  data[base == "10 - Kalibo",base := "Kalibo"]
  data[base == "13-Davao",base := "Davao"]
  data[base == "08 - Iloilo (Antique)",base := "Iloilo"]
  data[base == "10 - Roxas (Kalibo)",base := "Kalibo"]
  data[base == "10 - Roxas",base := "Kalibo"]
  data[base == "010 - Roxas",base := "Kalibo"]
  data[base == "CEB" ,base := "Cebu"]
  data[base == "BAC" ,base := "Bacolod"]
  data[base == "PAL" ,base := "Palawan"]
  data[base == "ILO" ,base := "Iloilo"]
  data[base == "BOH" ,base := "Bohol"]
  data[base == "KOR" ,base := "Koronadal"]
  data[base == "DIP" ,base := "Dipolog"]
  data[base == "DUM" ,base := "Dumaguete"]
  data[base == "KAL" ,base := "Kalibo"]
  data[base == "GEN" ,base := "General Santos"]
  data[base == "TAC",base := "Tacloban"]
}

#########################################################################################################################################
clean_categorical_cols <- function(prepost, opt_list){
  prepost <- prepost %>% data.table()
  dt_key_all <- prepost[,KEY,community]
  list_of_vars <- unique(opt_list$MERGE_NAME) # creating a list of columns to look for and clean all select_one variables
  
  for(varx in list_of_vars){
    
    if(varx %in% names(prepost)){
      name_status <- prepost[,KEY,varx]
      choices_status <- opt_list[opt_list$MERGE_NAME == varx,list(name_choices,label_english_choices)] #************
      setnames(choices_status,colnames(choices_status)[1],colnames(name_status)[1])
      dt_status <- merge.data.frame(name_status,choices_status,by=colnames(name_status)[1]) %>% data.table()
      dt_status <- dt_status[,which(!is.na(varx)) := label_english_choices] %>% select(all_of(varx),KEY)
      dt_key_all <- left_join(dt_key_all,dt_status,by="KEY")
    } 
    
  }
  print(dt_key_all)
  dt_key_all <- dt_key_all[, lapply(.SD, as.character)]
  # ###
  # for( i in colnames(dt_key_all)){
  #   dt_key_all[[i]] <- as.character( dt_key_all[[i]] )
  # }
  
  return(dt_key_all)
}

################################################################################################################################
clean_categorical_cols2 <- function(prepost,opt_list){
  dt_key_all <- prepost[,KEY,community] %>% data.table()
  list_of_vars <- unique(opt_list$MERGE_NAME) # creating a list of columns to look for and clean all select_one variables
  
  for(varx in list_of_vars){
    
    if(varx %in% names(prepost)){
      name_status <- prepost[,.(KEY,get(varx))]
      name_status <- name_status %>% setcolorder(c("V2","KEY")) 
      setnames(name_status, "V2", varx)
      choices_status <- opt_list[opt_list$MERGE_NAME%in%varx,list(name_choices,label_english_choices)]
      setnames(choices_status,colnames(choices_status)[1],colnames(name_status)[1])
      dt_status <- merge.data.frame(name_status,choices_status,by=colnames(name_status)[1]) %>% data.table()
      dt_status <- dt_status[,which(!is.na(varx)) := label_english_choices] %>% select(all_of(varx),KEY) 
      dt_key_all <- left_join(dt_key_all,dt_status,by="KEY")
    } 
    
  }
  ###
  for( i in colnames(dt_key_all)){
    dt_key_all[[i]] <- as.character( dt_key_all[[i]] )
  }
  
  return(dt_key_all)
}

##############################################################################################################################################################################################################################
create_codebook <- function(survey,option_list,pre_data,post_data,mstrcodebook){
  survey <- survey %>% select(type,name,label_english)
  survey <- survey %>% filter(!row_number() %in%grep("select_one", survey$type) ) #select all rows in the survey except "select_one" questions
  survey <- survey[!duplicated(survey$name),]
  # select_one questions
  select_one_qns <- option_list %>% select(type,name,label_english,MERGE_NAME)
  select_one_qns <- select_one_qns[!duplicated(select_one_qns$MERGE_NAME),]
  select_one_qns[!is.na(select_one_qns$MERGE_NAME),name := MERGE_NAME]
  select_one_qns <- select_one_qns %>% select(-c("MERGE_NAME"))
  # All possible responses
  codebook0 <- bind_rows(survey,select_one_qns)
  # pre data var names
  pre_data_cols <- as.data.frame(names(pre_data))
  names(pre_data_cols)[names(pre_data_cols) == "names(pre_data)"] <-  "name"
  pre_data_cols$pre_survey <- "pre"
  # post data var names
  post_var <- function(post_data){
    if( is.null(post_data) ){
      name <- c("NA")
      post_survey <- c("No post")
      post_data1 <- data.frame(name,post_survey)
    } else {
      post_data1 <- as.data.frame(names(post_data))
      names(post_data1)[names(post_data1) == "names(post_data)"] <-  "name"
      post_data1$post_survey <- "post"
      
    }
    
    return(post_data1)
  }
  
  post_data_cols <- post_var(post_data)
  
  # merge All possible responses with pre data var names and post data var names
  codebook1 <- merge(codebook0,pre_data_cols, by = "name",all.x = TRUE,all.y = TRUE)
  codebook2 <- merge(codebook1,post_data_cols, by = "name",all.x = TRUE,all.y = TRUE)
  # Duplicate column name, renames it var_names, to be used for merging
  codebook2 <- codebook2 %>% data.table()
  codebook2[!is.na(codebook2$name),var_names := name]
  codebook <- merge(codebook2,mstrcodebook,by = "var_names",all.x = TRUE) %>% select(-c("var_names"))
  names(codebook)[names(codebook) == "name"] <-  "variable_name"
  names(codebook)[names(codebook) == "new"] <-  "descriptive_name"
 
  
  return(codebook)
}
#####################################################################################################################################

## AnOther codebook function,  
#  for: prepost_tag... example: "2023B1 Pre"

  for_mstcdB <- function(prepost_data,prepost_tag){
    #pre
    prepost_data <- prepost_data[1,]
    prepost_data <- data.frame(lapply(prepost_data, as.character), stringsAsFactors=FALSE)
    prepost_data[1, ] <- prepost_tag
    
    
    return(prepost_data)
    
  }
#########################################################################################################################################
# merge pre/post by name_id = name_community and hhid = participant_id

prepost_match <- function(pre,post){
  #step 1: create column name_id
  data_pre <- pre %>% mutate(name_id = as.character(paste(name.icm,"_",community)) )
  data_post <- post %>% mutate(name_id = as.character(paste(name.icm,"_",community)) )
  #
  # Create a temporary column to use as the merge key
  data_pre$temp_key <- as.character(ifelse(data_pre$hhid %in% data_post$hhid, data_pre$hhid, data_pre$name_id))
  data_post$temp_key <- as.character(ifelse(data_post$hhid %in% data_pre$hhid, data_post$hhid, data_post$name_id))
  
  # Merge the data frames using the temporary column as the merge key
  prepost_merged <- merge(data_pre, data_post, by = "temp_key")
  
  
  return(prepost_merged)
}
##################################################################################################################################
prepost_match2 <- function(pre,post){
  #step 1: create column name_id
  data_pre <- pre %>% mutate(name_id = paste(name.icm,"_",community)) 
  data_post <- post %>% mutate(name_id = paste(name.icm,"_",community))
  
  #step 2: get pre observations that exist in post by name_id created in step 1
  data_pre_name_id <- data_pre[data_pre$name_id%in%data_post$name_id,] #**
  data_pre_name_id$name_id_merge <- data_pre_name_id$name_id #duplicate name_id
  
  # Repeat step 2 for post
  #step 2: get post observations that exist in pre by name_id created in step 1
  data_post_name_id <- data_post[data_post$name_id%in%data_pre$name_id,] #**
  data_post_name_id$name_id_merge <- data_post_name_id$name_id #duplicate var name_id
  
  #step 4: merging by name_id
  data_prepost_name_id <- merge(data_pre_name_id,data_post_name_id, by="name_id_merge") %>% select(-c(name_id_merge)) %>% data.table()
  data_prepost_name_id[data_prepost_name_id$type.x == "pre" & data_prepost_name_id$type.y == "post",prepost_match := "matched"]
  
  return(data_prepost_name_id)
}
########################################################################################################################################
prepost_match_2 <- function(pre,post){
  pre_post <- pre_post %>% mutate(name_id = as.character(paste(name.icm,"_",community)) )
  post <- post %>% mutate(name_id = as.character(paste(name.icm,"_",community)))

  # Create a temporary column to use as the merge key
  pre_post$temp_key <- ifelse(pre_post$participant.id %in% post$participant.id, pre_post$participant.id, pre_post$name_id)
  post$temp_key <- ifelse(post$participant.id %in% pre_post$participant.id, post$participant.id, post$name_id)
  names(post)[names(post) == "name_id"] <- "name_id_ops"
  names(post)[names(post) == "participant.id"] <- "participant.id_ops"
  # Merge the data frames using the temporary column as the merge key
  prepost_ops <- merge(pre_post, post, by = "temp_key")


  return(prepost_ops)
}
########################################################################################################################################
tag_gif_programs <-  function(data,gif_tag,gif_program_name){
  # none-gif , gif
  data$gif_tag <- gif_tag
  data$gif_program_name <- gif_program_name
  return(data)
}
########################################################################################################################################
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


flatten_lists_in_data <- function(data){
  for(col in names(data)){
    if("list" %in% class(data[, get(col)])){
      data[, (col) := as.character(get(col))]  
      data[get(col) == "NULL" | is.null(get(col)), (col) := NA]  
    }
  }
  return(data)
}
