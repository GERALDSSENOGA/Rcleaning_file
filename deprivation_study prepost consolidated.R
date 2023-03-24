
library(googlesheets4)
library(tidyverse)
library(janitor)
library(lubridate, warn.conflicts = FALSE)
library(plyr)
library(data.table)
library(plyr)
library(dplyr)
#######################

###################################################################################################################
b1_2018_pre <- read_sheet("https://docs.google.com/spreadsheets/d/11VbYGj8ZT4-oIPT3_TJoZt1kkQw0t9dwpCh0lmFnbz4/edit#gid=0", sheet = "b1_2018_pre")                  
#typeof(b1_2018_pre$status)
#b1_2018_pre$geotype <- as.character(b1_2018_pre$geotype)
#--B2 2018
b2_2018_prepost <- read_sheet("https://docs.google.com/spreadsheets/d/1peBQvJ95N-1eNh8Dvu73jP4x2Glxht3aR90DQZopLcU/edit#gid=0", sheet = "b2_2018_prepost")
b2_2018_prepost$b_q1 <- as.character(b2_2018_prepost$b_q1)
b2_2018_prepost$b_q2 <- as.character(b2_2018_prepost$b_q2)
b2_2018_prepost$b_q3 <- as.character(b2_2018_prepost$b_q3)
b2_2018_prepost$b_q4 <- as.character(b2_2018_prepost$b_q4)
b2_2018_prepost$b_q5 <- as.character(b2_2018_prepost$b_q5)
b2_2018_prepost$b_q6 <- as.character(b2_2018_prepost$b_q6)
b2_2018_prepost$b_q7 <- as.character(b2_2018_prepost$b_q7)
b2_2018_prepost$b_q8 <- as.character(b2_2018_prepost$b_q8)
b2_2018_prepost <- b2_2018_prepost %>% data.table()
b2_2018_prepost$e_q11_v2
names(b2_2018_prepost)[names(b2_2018_prepost) == "e_q11"] <- "e_q11_v2"
b2_2018_prepost[b2_2018_prepost$e_q11_v2 == "Yes",e_q11_v2 := "ICM-affiliated church or ICM pastor’s church"]

#--B3 2018
b3_2018_pre_post <- read_sheet("https://docs.google.com/spreadsheets/d/1bVUOo-XeW4IwHIpe_BTTvtK4uaT39SyiNSkxzxpnUzg/edit#gid=0", sheet = "b3_2018_pre_post")
b3_2018_pre_post$b_q1 <- as.character(b3_2018_pre_post$b_q1)
b3_2018_pre_post$b_q2 <- as.character(b3_2018_pre_post$b_q2)
b3_2018_pre_post$b_q3 <- as.character(b3_2018_pre_post$b_q3)
b3_2018_pre_post$b_q4 <- as.character(b3_2018_pre_post$b_q4)
b3_2018_pre_post$b_q5 <- as.character(b3_2018_pre_post$b_q5)
b3_2018_pre_post$b_q6 <- as.character(b3_2018_pre_post$b_q6)
b3_2018_pre_post$b_q7 <- as.character(b3_2018_pre_post$b_q7)
b3_2018_pre_post$b_q8 <- as.character(b3_2018_pre_post$b_q8)
b3_2018_pre_post <- b3_2018_pre_post %>% data.table()
b3_2018_pre_post$e_q11_v2
names(b3_2018_pre_post)[names(b3_2018_pre_post) == "e_q11"] <- "e_q11_v2"
b3_2018_pre_post[b3_2018_pre_post$e_q11_v2 == "Yes",e_q11_v2 := "ICM-affiliated church or ICM pastor’s church"]

#--B1 2019
b1_2019_pre_post <- read_sheet("https://docs.google.com/spreadsheets/d/1QOi_M_2M15Wjjf1oTLclDObeH1V8rppgZz4il4MNyZI/edit#gid=0", sheet = "b1_2019_pre_post")
b1_2019_pre_post$b_q1 <- as.character(b1_2019_pre_post$b_q1)
b1_2019_pre_post$b_q2 <- as.character(b1_2019_pre_post$b_q2)
b1_2019_pre_post$b_q3 <- as.character(b1_2019_pre_post$b_q3)
b1_2019_pre_post$b_q4 <- as.character(b1_2019_pre_post$b_q4)
b1_2019_pre_post$b_q5 <- as.character(b1_2019_pre_post$b_q5)
b1_2019_pre_post$b_q6 <- as.character(b1_2019_pre_post$b_q6)
b1_2019_pre_post$b_q7 <- as.character(b1_2019_pre_post$b_q7)
b1_2019_pre_post$b_q8 <- as.character(b1_2019_pre_post$b_q8)
b1_2019_pre_post <- b1_2019_pre_post %>% data.table()
b1_2019_pre_post$e_q11_v2
names(b1_2019_pre_post)[names(b1_2019_pre_post) == "e_q11"] <- "e_q11_v2"
b1_2019_pre_post[b1_2019_pre_post$e_q11_v2 == "Yes",e_q11_v2 := "ICM-affiliated church or ICM pastor’s church"]

#-- B2 2019
b2_2019_pre_post <- read_sheet("https://docs.google.com/spreadsheets/d/12mv0tQkOAQ4K2CbRIZlsWV1QjMX4rC-dwHh7cRAXsiY/edit#gid=0", sheet = "b2_2019_pre_post")
#b2_2019_pre_post$b_q1 <- as.character(b2_2019_pre_post$b_q1)
#b2_2019_pre_post$b_q2 <- as.character(b2_2019_pre_post$b_q2)
b2_2019_pre_post$b_q3 <- as.character(b2_2019_pre_post$b_q3)
b2_2019_pre_post$b_q4 <- as.character(b2_2019_pre_post$b_q4)
b2_2019_pre_post$b_q5 <- as.character(b2_2019_pre_post$b_q5)
b2_2019_pre_post$b_q6 <- as.character(b2_2019_pre_post$b_q6)
b2_2019_pre_post$b_q7 <- as.character(b2_2019_pre_post$b_q7)
b2_2019_pre_post$b_q8 <- as.character(b2_2019_pre_post$b_q8)
b2_2019_pre_post$e_q11_v2
b2_2019_pre_post <- b2_2019_pre_post %>% data.table()
names(b2_2019_pre_post)[names(b2_2019_pre_post) == "e_q11"] <- "e_q11_v2"
b2_2019_pre_post[b2_2019_pre_post$e_q11_v2 == "Yes",e_q11_v2 := "ICM-affiliated church or ICM pastor’s church"]
#-- B3 2019
b3_2019_pre_post <- read_sheet("https://docs.google.com/spreadsheets/d/1UfpthjL22PVWDstw5ImGrGe3MN19wAhcZO7ObsBRM7s/edit#gid=0", sheet = "b3_2019_pre_post")
b3_2019_pre_post$b_q1 <- as.character(b3_2019_pre_post$b_q1)
b3_2019_pre_post$b_q2 <- as.character(b3_2019_pre_post$b_q2)
b3_2019_pre_post$b_q3 <- as.character(b3_2019_pre_post$b_q3)
b3_2019_pre_post$b_q4 <- as.character(b3_2019_pre_post$b_q4)
b3_2019_pre_post$b_q5 <- as.character(b3_2019_pre_post$b_q5)
b3_2019_pre_post$b_q6 <- as.character(b3_2019_pre_post$b_q6)
b3_2019_pre_post$b_q7 <- as.character(b3_2019_pre_post$b_q7)
b3_2019_pre_post$b_q8 <- as.character(b3_2019_pre_post$b_q8)
b3_2019_pre_post <- b3_2019_pre_post %>% data.table()
b3_2019_pre_post$e_q11_v2
names(b3_2019_pre_post)[names(b3_2019_pre_post) == "e_q11"] <- "e_q11_v2"
b3_2019_pre_post[b3_2019_pre_post$e_q11_v2 == "Yes",e_q11_v2 := "ICM-affiliated church or ICM pastor’s church"]
#-- B1 2020
b_2020_pre_post <- read_sheet("https://docs.google.com/spreadsheets/d/1LtKyk0_bv9qx6HnYql8DatynsDMArxCV7xQS3kWIf2U/edit#gid=1921658300", sheet = "b_2020_pre_post")
b_2020_pre_post$b_q1 <- as.character(b_2020_pre_post$b_q1)
b_2020_pre_post$b_q2 <- as.character(b_2020_pre_post$b_q2)
b_2020_pre_post$b_q3 <- as.character(b_2020_pre_post$b_q3)
b_2020_pre_post$b_q4 <- as.character(b_2020_pre_post$b_q4)
b_2020_pre_post$b_q5 <- as.character(b_2020_pre_post$b_q5)
b_2020_pre_post$b_q6 <- as.character(b_2020_pre_post$b_q6)
b_2020_pre_post$b_q7 <- as.character(b_2020_pre_post$b_q7)
b_2020_pre_post$b_q8 <- as.character(b_2020_pre_post$b_q8)
b_2020_pre_post$e_q11_v2
b_2020_pre_post <- b_2020_pre_post %>% data.table()
names(b_2020_pre_post)[names(b_2020_pre_post) == "e_q11"] <- "e_q11_v2"
b_2020_pre_post[b_2020_pre_post$e_q11_v2 == "Yes",e_q11_v2 := "ICM-affiliated church or ICM pastor’s church"]

#-- B1 2021
strech_v1_pre_post <- read_sheet("https://docs.google.com/spreadsheets/d/1HLsaQflnf4jOpTMrGJKKCETQyiE3C_u1tmtJlkc-aZ0/edit#gid=0", sheet = "strech_v1_pre_post")
strech_v1_pre_post$b_q1 <- as.character(strech_v1_pre_post$b_q1)
strech_v1_pre_post$b_q2 <- as.character(strech_v1_pre_post$b_q2)
strech_v1_pre_post$b_q3 <- as.character(strech_v1_pre_post$b_q3)
strech_v1_pre_post$b_q4 <- as.character(strech_v1_pre_post$b_q4)
strech_v1_pre_post$b_q5 <- as.character(strech_v1_pre_post$b_q5)
strech_v1_pre_post$b_q6 <- as.character(strech_v1_pre_post$b_q6)
strech_v1_pre_post$b_q7 <- as.character(strech_v1_pre_post$b_q7)
strech_v1_pre_post$b_q8 <- as.character(strech_v1_pre_post$b_q8)
strech_v1_pre_post <- strech_v1_pre_post %>% data.table()
strech_v1_pre_post$e_q11_v2
names(strech_v1_pre_post)[names(strech_v1_pre_post) == "e_q11"] <- "e_q11_v2"
strech_v1_pre_post[strech_v1_pre_post$e_q11_v2 == "Yes",e_q11_v2 := "ICM-affiliated church or ICM pastor’s church"]

#-- B2 2021
b2_2021_drct_pre_post <- read_sheet("https://docs.google.com/spreadsheets/d/1YFMcmftW--0Qrg1KQc_bx2Ox1uu5Oevls7MKi_ldGMI/edit#gid=0", sheet = "b2_2021_drct_pre_post")                                                   
b2_2021_drct_pre_post$b_q1 <- as.character(b2_2021_drct_pre_post$b_q1)
b2_2021_drct_pre_post$b_q2 <- as.character(b2_2021_drct_pre_post$b_q2)
b2_2021_drct_pre_post$b_q3 <- as.character(b2_2021_drct_pre_post$b_q3)
b2_2021_drct_pre_post$b_q4 <- as.character(b2_2021_drct_pre_post$b_q4)
b2_2021_drct_pre_post$b_q5 <- as.character(b2_2021_drct_pre_post$b_q5)
b2_2021_drct_pre_post$b_q6 <- as.character(b2_2021_drct_pre_post$b_q6)
b2_2021_drct_pre_post$b_q7 <- as.character(b2_2021_drct_pre_post$b_q7)
b2_2021_drct_pre_post$b_q8 <- as.character(b2_2021_drct_pre_post$b_q8)


#-- B2 2021
b2_2021_pre_stretch_v2 <- read_sheet("https://docs.google.com/spreadsheets/d/19RINXJDc3QANKgl8pdrob8DB_Z48pEJlLFg86OyKG3g/edit#gid=0", sheet = "b2_2021_pre_stretch_v2")
b2_2021_pre_stretch_v2$b_q1 <- as.character(b2_2021_pre_stretch_v2$b_q1)
b2_2021_pre_stretch_v2$b_q2 <- as.character(b2_2021_pre_stretch_v2$b_q2)
b2_2021_pre_stretch_v2$b_q3 <- as.character(b2_2021_pre_stretch_v2$b_q3)
b2_2021_pre_stretch_v2$b_q4 <- as.character(b2_2021_pre_stretch_v2$b_q4)
b2_2021_pre_stretch_v2$b_q5 <- as.character(b2_2021_pre_stretch_v2$b_q5)
b2_2021_pre_stretch_v2$b_q6 <- as.character(b2_2021_pre_stretch_v2$b_q6)
b2_2021_pre_stretch_v2$b_q7 <- as.character(b2_2021_pre_stretch_v2$b_q7)
b2_2021_pre_stretch_v2$b_q8 <- as.character(b2_2021_pre_stretch_v2$b_q8)

b2_2021_pre_stretch_v2$geotype <- as.character(b2_2021_pre_stretch_v2$geotype)

b2_2021_pre_stretch_v2 <- b2_2021_pre_stretch_v2 %>% data.table()
names(b2_2021_pre_stretch_v2)[names(b2_2021_pre_stretch_v2) == "e_q11"] <- "e_q11_v2"
b2_2021_pre_stretch_v2[b2_2021_pre_stretch_v2$e_q11_v2 == "Yes",e_q11_v2 := "ICM-affiliated church or ICM pastor’s church"]
names(b2_2021_pre_stretch_v2)[names(b2_2021_pre_stretch_v2) == "e_q12_icm"] <- "e_q12_icm_v2"
b2_2021_pre_stretch_v2[b2_2021_pre_stretch_v2$e_q12_icm_v2 == "Yes",e_q12_icm_v2 := "ICM-affiliated church or ICM pastor’s church"]

#-- B3 2021
b3_2021_pre_chc_t <- read_sheet("https://docs.google.com/spreadsheets/d/1EANSoRZiCSp7JvRd1fhbRKiI92cZp_fcKMz_-oH_TFc/edit#gid=0", sheet = "b3_2021_pre_chc_t")

#-- B3 2021
B3_2021_pre_trct <- read_sheet("https://docs.google.com/spreadsheets/d/1DXfDTqo3hkx0Pj1nl46_XIuB2ovm98Bcs4_VoFKvwl4/edit#gid=0", sheet = "B3_2021_pre_trct")
B3_2021_pre_trct$b_q1 <- as.character(B3_2021_pre_trct$b_q1)
B3_2021_pre_trct$b_q2 <- as.character(B3_2021_pre_trct$b_q2)
B3_2021_pre_trct$b_q3 <- as.character(B3_2021_pre_trct$b_q3)
B3_2021_pre_trct$b_q4 <- as.character(B3_2021_pre_trct$b_q4)
B3_2021_pre_trct$b_q5 <- as.character(B3_2021_pre_trct$b_q5)
B3_2021_pre_trct$b_q6 <- as.character(B3_2021_pre_trct$b_q6)
B3_2021_pre_trct$b_q7 <- as.character(B3_2021_pre_trct$b_q7)
B3_2021_pre_trct$b_q8 <- as.character(B3_2021_pre_trct$b_q8)
B3_2021_pre_trct$geotype <- as.character(B3_2021_pre_trct$geotype)

#-- B1 2022
b1_2022_pre_irct <- read_sheet("https://docs.google.com/spreadsheets/d/1iJgA3q_xEAXbu7tNHv3cFx3tObb5RE6c5IkQbewa_0M/edit#gid=0", sheet = "b1_2022_pre_irct")
b1_2022_pre_irct <- b1_2022_pre_irct %>% data.table()
b1_2022_pre_irct$b_q1 <- as.character(b1_2022_pre_irct$b_q1)
b1_2022_pre_irct$b_q2 <- as.character(b1_2022_pre_irct$b_q2)
b1_2022_pre_irct$b_q3 <- as.character(b1_2022_pre_irct$b_q3)
b1_2022_pre_irct$b_q4 <- as.character(b1_2022_pre_irct$b_q4)
b1_2022_pre_irct$b_q3 <- as.character(b1_2022_pre_irct$b_q3)
b1_2022_pre_irct$b_q5 <- as.character(b1_2022_pre_irct$b_q5)
b1_2022_pre_irct$b_q6 <- as.character(b1_2022_pre_irct$b_q6)
b1_2022_pre_irct$b_q7 <- as.character(b1_2022_pre_irct$b_q7)
b1_2022_pre_irct$b_q8 <- as.character(b1_2022_pre_irct$b_q8)
b1_2022_pre_irct$geotype <- as.character(b1_2022_pre_irct$geotype)

########################################################################################################################################################################################################################################

all_deprivation_study_pre_post <- bind_rows(
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
  B3_2021_pre_trct,
  #2022
  b1_2022_pre_irct
)
names(all_deprivation_study_pre_post)[names(all_deprivation_study_pre_post) == "spiritual_eternal_salvation_grace_alone"] <- "spiritual.eternal.salvation.grace.alone"
all_deprivation_study_pre_post2 <- copy(all_deprivation_study_pre_post)
#write.csv(all_deprivation_study_pre_post,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/prepost-combined/all_pre_post_raw_cols.csv')
#a <- read.csv('G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/prepost-combined/all_pre_post_raw_cols.csv')
########################################################################################################################################################################################################################################

all_deprivation_study_pre_post <- all_deprivation_study_pre_post[,which(colnames(all_deprivation_study_pre_post)%in% myvars)]

all_deprivation_study_pre_post <- change_col_names(all_deprivation_study_pre_post)
###############################################################################################################
var_int <- c("hh.num.mem","povscore.phone.hh.total","inc.total.hh.last.mth.cln","con.last.seven.days.meat","con.last.seven.days.cereals","con.last.seven.days.fruits.veg",
             "con.last.seven.days.milk.eggs","con.last.seven.days.beverages","con.last.seven.days.alcohol","con.last.seven.days.cigarettes", "con.last.seven.days.snacks","con.last.thirty.days.load",
              "con.last.thirty.days.transport","con.last.thirty.days.business","con.last.thirty.days.clothing","con.last.thirty.days.cosmetics","con.last.thirty.days.gambling","con.last.thirty.days.utilities",
             "con.last.thirty.days.offering","con.last.six.mths.edu","con.last.six.mths.health","con.last.six.mths.wedding","con.last.six.mths.funeral","con.last.six.mths.celebration","spiritual.read.bible.last.seven.days","sav.hh.savings.total",       
              "health_adult_two_wks_diarrhea","health_child_two_wks_diarrhea","health_adult_two_wks_cough","health_child_two_wks_cough",
              "num_children_notov_schage","num_children_notov_schage_enr","pc_children_notov_schage_enr")

#write.csv(all_deprivation_study_pre_post,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/prepost-combined/all_deprivation_study_pre_post.csv')
################################################################################################################










b1_2018_pre_ops$mem.num.hh.head <- as.character(b1_2018_pre_ops$mem.num.hh.head)
b1_2018_pre_all$mem.num.hh.head <- as.character(b1_2018_pre_all$mem.num.hh.head)
b2_2018_prepost_ops$mem.num.hh.head <- as.character(b2_2018_prepost_ops$mem.num.hh.head)
b3_2018_pre_post_ops$mem.num.hh.head <- as.character(b3_2018_pre_post_ops$mem.num.hh.head)
b2_2019_pre_post_ops$mem.num.hh.head <- as.character(b2_2019_pre_post_ops$mem.num.hh.head)
b3_2019_pre_post_ops$presurvey.comm.total <- as.character(b3_2019_pre_post_ops$presurvey.comm.total)
b1_2020_pre_post_clean$mem.age.1 <- as.double(b1_2020_pre_post_clean$mem.age.1)
b1_2021_strech_v1_prepost_ops$hh.num.mem.local <- as.character(b1_2021_strech_v1_prepost_ops$hh.num.mem.local)
b1_2021_strech_v1_prepost_ops$presurvey.comm.total <- as.character(b1_2021_strech_v1_prepost_ops$presurvey.comm.total)
b1_2021_strech_v1_prepost_ops$mem.list.stillmember.it.count <- as.character(b1_2021_strech_v1_prepost_ops$mem.list.stillmember.it.count)
b1_2021_strech_v1_prepost_ops$health.total.child.die <- as.character(b1_2021_strech_v1_prepost_ops$health.total.child.die)
b2_2021_drct_prepost_ops$mem.age.1 <- as.double(b2_2021_drct_prepost_ops$mem.age.1)
b2_2021_pre_stretch_v2_ops$hh.num.mem.local <- as.character(b2_2021_pre_stretch_v2_ops$hh.num.mem.local)
b2_2021_pre_stretch_v2_ops$presurvey.comm.total <- as.character(b2_2021_pre_stretch_v2_ops$presurvey.comm.total)
b2_2021_pre_stretch_v2_ops$mem.list.stillmember.it.count <- as.character(b2_2021_pre_stretch_v2_ops$mem.list.stillmember.it.count)
b2_2021_pre_stretch_v2_ops$health.total.child.die <- as.character(b2_2021_pre_stretch_v2_ops$health.total.child.die)
b3_2021_pre_chc_t_ops$inc.total.hh.last.mth.earned <- as.character(b3_2021_pre_chc_t_ops$inc.total.hh.last.mth.earned)
b3_2021_pre_chc_t_ops$inc.total.hh.last.wk <- as.character(b3_2021_pre_chc_t_ops$inc.total.hh.last.wk)
b3_2021_pre_chc_t_ops$inc.total.hh.last.mth.cln <- as.character(b3_2021_pre_chc_t_ops$inc.total.hh.last.mth.cln)
b3_2021_pre_chc_t_ops$presurvey.comm.total <- as.character(b3_2021_pre_chc_t_ops$presurvey.comm.total)
b3_2021_pre_chc_t_ops$mem.list.stillmember.it <- as.character(b3_2021_pre_chc_t_ops$mem.list.stillmember.it)
b3_2021_pre_chc_t_ops$mem.list.stillmember.it.count <- as.character(b3_2021_pre_chc_t_ops$mem.list.stillmember.it.count)
b3_2021_pre_chc_t_ops$health.total.child.die <- as.character(b3_2021_pre_chc_t_ops$health.total.child.die)
b3_2021_pre_chc_t_ops$preload_date <- as.character(b3_2021_pre_chc_t_ops$preload_date)
b3_2021_pre_trct_ops$mem.age.1 <- as.double(b3_2021_pre_trct_ops$mem.age.1)
b3_2021_pre_trct_ops$presurvey.comm.total <- as.character(b3_2021_pre_trct_ops$presurvey.comm.total)
b3_2021_pre_trct_ops$mem.list.stillmember.it <- as.character(b3_2021_pre_trct_ops$mem.list.stillmember.it)
b3_2021_pre_trct_ops$preload_date <- as.character(b3_2021_pre_trct_ops$preload_date)
b1_2022_pre_irct__ops$mem.age.1 <- as.double(b1_2022_pre_irct__ops$mem.age.1)
b1_2022_pre_irct__ops$presurvey.comm.total <- as.character(b1_2022_pre_irct__ops$presurvey.comm.total)
b1_2022_pre_irct__ops$mem.list.stillmember.it <- as.character(b1_2022_pre_irct__ops$mem.list.stillmember.it)
b1_2022_pre_irct__ops$health.total.child.die <- as.character(b1_2022_pre_irct__ops$health.total.child.die)
b1_2022_pre_irct__ops$preload_date <- as.character(b1_2022_pre_irct__ops$preload_date)
b2_2022_pre_irct__ops$mem.age.1 <- as.double(b2_2022_pre_irct__ops$mem.age.1)
b2_2022_pre_irct__ops$presurvey.comm.total <- as.character(b2_2022_pre_irct__ops$presurvey.comm.total)
b2_2022_pre_irct__ops$mem.list.stillmember.it <- as.character(b2_2022_pre_irct__ops$mem.list.stillmember.it)
b2_2022_pre_irct__ops$preload_date <- as.character(b2_2022_pre_irct__ops$preload_date)
b3_2022_pre_rrct__ops$mem.age.1 <- as.double(b3_2022_pre_rrct__ops$mem.age.1)
b3_2022_pre_rrct__ops$inc.total.hh.last.mth.earned <- as.character(b3_2022_pre_rrct__ops$inc.total.hh.last.mth.earned)

b3_2022_pre_rrct__ops$presurvey.comm.total <- as.character(b3_2022_pre_rrct__ops$presurvey.comm.total)
b3_2022_pre_rrct__ops$mem.list.stillmember.it <- as.character(b3_2022_pre_rrct__ops$mem.list.stillmember.it)
b3_2022_pre_rrct__ops$preload_date <- as.character(b3_2022_pre_rrct__ops$preload_date)
#b3_2022_pre_rrct__ops$inc.total.hh.last.mth.earned <- as.character(b3_2022_pre_rrct__ops$inc.total.hh.last.mth.earned)

all_pre_post_cleaned <- bind_rows(
    #2018
  b1_2018_pre_ops,
  b2_2018_prepost_ops,
  b3_2018_pre_post_ops,
    #2019
  b1_2019_pre_post_ops,
  b2_2019_pre_post_ops,
  b3_2019_pre_post_ops,
    #2020
  b1_2020_pre_post_clean,
    #2021
  b1_2021_strech_v1_prepost_ops,
  b2_2021_drct_prepost_ops,
  b2_2021_pre_stretch_v2_ops,
  b3_2021_pre_chc_t_ops,
  b3_2021_pre_trct_ops,
    #2022
  b1_2022_pre_irct__ops,
  b2_2022_pre_irct__ops,
  b3_2022_pre_rrct__ops,
    ) 


write.csv(all_pre_post_cleaned,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/prepost-combined/all_pre_post_cleaned.csv')
########################################################################################################################


#merged
b1_2019_prepost_merged$mem.num.hh.head.x <- as.double(b1_2019_prepost_merged$mem.num.hh.head.x)
b1_2019_prepost_merged$mem.num.hh.head.y <- as.double(b1_2019_prepost_merged$mem.num.hh.head.y)
b3_2019_prepost_merged$presurvey.comm.total.x <- as.character(b3_2019_prepost_merged$presurvey.comm.total.x)
b3_2019_prepost_merged$presurvey.comm.total.y <- as.character(b3_2019_prepost_merged$presurvey.comm.total.y)
b2_2018_prepost_merged$mem.age.1.x <- as.double(b2_2018_prepost_merged$mem.age.1.x)
b1_2020_prepost_merged$mem.age.1.x <- as.double(b1_2020_prepost_merged$mem.age.1.x)
b1_2020_prepost_merged$mem.age.1.y <- as.double(b1_2020_prepost_merged$mem.age.1.y)
b1_2021_prepost_tfsv1_merged$hh.num.mem.local.x <- as.character(b1_2021_prepost_tfsv1_merged$hh.num.mem.local.x)
b1_2021_prepost_tfsv1_merged$hh.num.mem.local.y <- as.character(b1_2021_prepost_tfsv1_merged$hh.num.mem.local.y)
b1_2021_prepost_tfsv1_merged$presurvey.comm.total.x <- as.character(b1_2021_prepost_tfsv1_merged$presurvey.comm.total.x)
b1_2021_prepost_tfsv1_merged$presurvey.comm.total.y <- as.character(b1_2021_prepost_tfsv1_merged$presurvey.comm.total.y)
b1_2021_prepost_tfsv1_merged$mem.list.stillmember.it.count.x <- as.character(b1_2021_prepost_tfsv1_merged$mem.list.stillmember.it.count.x)
b1_2021_prepost_tfsv1_merged$mem.list.stillmember.it.count.y <- as.character(b1_2021_prepost_tfsv1_merged$mem.list.stillmember.it.count.y)
b1_2021_prepost_tfsv1_merged$health.total.child.die.x <- as.character(b1_2021_prepost_tfsv1_merged$health.total.child.die.x)
b1_2021_prepost_tfsv1_merged$health.total.child.die.y <- as.character(b1_2021_prepost_tfsv1_merged$health.total.child.die.y)
b2_2021_drct_prepost_merged$mem.age.1.x <- as.double(b2_2021_drct_prepost_merged$mem.age.1.x)
b2_2021_drct_prepost_merged$mem.age.1.y <- as.double(b2_2021_drct_prepost_merged$mem.age.1.y)
b2_2021_prepost_tfsv2_merged$hh.num.mem.local.x <- as.character(b2_2021_prepost_tfsv2_merged$hh.num.mem.local.x)
b2_2021_prepost_tfsv2_merged$hh.num.mem.local.y <- as.character(b2_2021_prepost_tfsv2_merged$hh.num.mem.local.y)
b2_2021_prepost_tfsv2_merged$presurvey.comm.total.x <- as.character(b2_2021_prepost_tfsv2_merged$presurvey.comm.total.x)
b2_2021_prepost_tfsv2_merged$presurvey.comm.total.y <- as.character(b2_2021_prepost_tfsv2_merged$presurvey.comm.total.y)
b2_2021_prepost_tfsv2_merged$mem.list.stillmember.it.count.x <- as.character(b2_2021_prepost_tfsv2_merged$mem.list.stillmember.it.count.x)
b2_2021_prepost_tfsv2_merged$mem.list.stillmember.it.count.y <- as.character(b2_2021_prepost_tfsv2_merged$mem.list.stillmember.it.count.y)
b2_2021_prepost_tfsv2_merged$health.total.child.die.x <- as.character(b2_2021_prepost_tfsv2_merged$health.total.child.die.x)
b2_2021_prepost_tfsv2_merged$health.total.child.die.y <- as.character(b2_2021_prepost_tfsv2_merged$health.total.child.die.y)
b3_2021_prepost_chc_t_merged$inc.total.hh.last.mth.earned.x <- as.character(b3_2021_prepost_chc_t_merged$inc.total.hh.last.mth.earned.x)
b3_2021_prepost_chc_t_merged$inc.total.hh.last.wk.x <- as.character(b3_2021_prepost_chc_t_merged$inc.total.hh.last.wk.x)
b3_2021_prepost_chc_t_merged$inc.total.hh.last.mth.cln.x <- as.character(b3_2021_prepost_chc_t_merged$inc.total.hh.last.mth.cln.x)
b3_2021_prepost_chc_t_merged$inc.total.hh.last.mth.earned.y <- as.character(b3_2021_prepost_chc_t_merged$inc.total.hh.last.mth.earned.y)
b3_2021_prepost_chc_t_merged$inc.total.hh.last.wk.y <- as.character(b3_2021_prepost_chc_t_merged$inc.total.hh.last.wk.y)
b3_2021_prepost_chc_t_merged$inc.total.hh.last.mth.cln.y <- as.character(b3_2021_prepost_chc_t_merged$inc.total.hh.last.mth.cln.y)
b3_2021_prepost_chc_t_merged$presurvey.comm.total.x <- as.character(b3_2021_prepost_chc_t_merged$presurvey.comm.total.x)
b3_2021_prepost_chc_t_merged$presurvey.comm.total.y <- as.character(b3_2021_prepost_chc_t_merged$presurvey.comm.total.y)
b3_2021_prepost_chc_t_merged$mem.list.stillmember.it.x <- as.character(b3_2021_prepost_chc_t_merged$mem.list.stillmember.it.x)
b3_2021_prepost_chc_t_merged$mem.list.stillmember.it.count.x <- as.character(b3_2021_prepost_chc_t_merged$mem.list.stillmember.it.count.x)
b3_2021_prepost_chc_t_merged$health.total.child.die.x <- as.character(b3_2021_prepost_chc_t_merged$health.total.child.die.x)
b3_2021_prepost_chc_t_merged$mem.list.stillmember.it.y <- as.character(b3_2021_prepost_chc_t_merged$mem.list.stillmember.it.y)
b3_2021_prepost_chc_t_merged$mem.list.stillmember.it.count.y <- as.character(b3_2021_prepost_chc_t_merged$mem.list.stillmember.it.count.y)
b3_2021_prepost_chc_t_merged$health.total.child.die.y <- as.character(b3_2021_prepost_chc_t_merged$health.total.child.die.y)
b3_2021_prepost_chc_t_merged$preload_date.x <- as.character(b3_2021_prepost_chc_t_merged$preload_date.x)
b3_2021_prepost_chc_t_merged$preload_date.y <- as.character(b3_2021_prepost_chc_t_merged$preload_date.y)
B3_2021_prepost_trct_merged$mem.age.1.x <- as.double(B3_2021_prepost_trct_merged$mem.age.1.x)
B3_2021_prepost_trct_merged$mem.age.1.y <- as.double(B3_2021_prepost_trct_merged$mem.age.1.y)
B3_2021_prepost_trct_merged$presurvey.comm.total.x <- as.character(B3_2021_prepost_trct_merged$presurvey.comm.total.x)
B3_2021_prepost_trct_merged$presurvey.comm.total.y <- as.character(B3_2021_prepost_trct_merged$presurvey.comm.total.y)
B3_2021_prepost_trct_merged$mem.list.stillmember.it.x <- as.character(B3_2021_prepost_trct_merged$mem.list.stillmember.it.x)
B3_2021_prepost_trct_merged$mem.list.stillmember.it.y <- as.character(B3_2021_prepost_trct_merged$mem.list.stillmember.it.y)
B3_2021_prepost_trct_merged$preload_date.y <- as.character(B3_2021_prepost_trct_merged$preload_date.y)
B3_2021_prepost_trct_merged$preload_date.x <- as.character(B3_2021_prepost_trct_merged$preload_date.x)
b1_2022_prepost_irct_merged$mem.age.1.x <- as.double(b1_2022_prepost_irct_merged$mem.age.1.x)
b1_2022_prepost_irct_merged$mem.age.1.y <- as.double(b1_2022_prepost_irct_merged$mem.age.1.y)
b1_2022_prepost_irct_merged$presurvey.comm.total.x <- as.character(b1_2022_prepost_irct_merged$presurvey.comm.total.x)
b1_2022_prepost_irct_merged$presurvey.comm.total.y <- as.character(b1_2022_prepost_irct_merged$presurvey.comm.total.y)
b1_2022_prepost_irct_merged$mem.list.stillmember.it.x <- as.character(b1_2022_prepost_irct_merged$mem.list.stillmember.it.x)
b1_2022_prepost_irct_merged$health.total.child.die.x <- as.character(b1_2022_prepost_irct_merged$health.total.child.die.x)
b1_2022_prepost_irct_merged$mem.list.stillmember.it.y <- as.character(b1_2022_prepost_irct_merged$mem.list.stillmember.it.y)
b1_2022_prepost_irct_merged$preload_date.x <- as.character(b1_2022_prepost_irct_merged$preload_date.x)
b1_2022_prepost_irct_merged$preload_date.y <- as.character(b1_2022_prepost_irct_merged$preload_date.y)
b2_2022_prepost_irct_merged$mem.age.1.x <- as.double(b2_2022_prepost_irct_merged$mem.age.1.x)
b2_2022_prepost_irct_merged$mem.age.1.y <- as.double(b2_2022_prepost_irct_merged$mem.age.1.y)
b2_2022_prepost_irct_merged$presurvey.comm.total.x <- as.character(b2_2022_prepost_irct_merged$presurvey.comm.total.x)
b2_2022_prepost_irct_merged$presurvey.comm.total.y <- as.character(b2_2022_prepost_irct_merged$presurvey.comm.total.y)
b2_2022_prepost_irct_merged$mem.list.stillmember.it.x <- as.character(b2_2022_prepost_irct_merged$mem.list.stillmember.it.x)
b2_2022_prepost_irct_merged$health.total.child.die.x <- as.character(b2_2022_prepost_irct_merged$health.total.child.die.x)
b2_2022_prepost_irct_merged$mem.list.stillmember.it.y <- as.character(b2_2022_prepost_irct_merged$mem.list.stillmember.it.y)
b2_2022_prepost_irct_merged$preload_date.x <- as.character(b2_2022_prepost_irct_merged$preload_date.x)
b2_2022_prepost_irct_merged$preload_date.y <- as.character(b2_2022_prepost_irct_merged$preload_date.y)

b3_2022_prepost_rrct_merged$mem.age.1.x <- as.double(b3_2022_prepost_rrct_merged$mem.age.1.x)
b3_2022_prepost_rrct_merged$mem.age.1.y <- as.double(b3_2022_prepost_rrct_merged$mem.age.1.y)
b3_2022_prepost_rrct_merged$presurvey.comm.total.x <- as.character(b3_2022_prepost_rrct_merged$presurvey.comm.total.x)
b3_2022_prepost_rrct_merged$presurvey.comm.total.y <- as.character(b3_2022_prepost_rrct_merged$presurvey.comm.total.y)
b3_2022_prepost_rrct_merged$mem.list.stillmember.it.x <- as.character(b3_2022_prepost_rrct_merged$mem.list.stillmember.it.x)
b3_2022_prepost_rrct_merged$health.total.child.die.x <- as.character(b3_2022_prepost_rrct_merged$health.total.child.die.x)
b3_2022_prepost_rrct_merged$mem.list.stillmember.it.y <- as.character(b3_2022_prepost_rrct_merged$mem.list.stillmember.it.y)
b3_2022_prepost_rrct_merged$preload_date.x <- as.character(b3_2022_prepost_rrct_merged$preload_date.x)
b3_2022_prepost_rrct_merged$preload_date.y <- as.character(b3_2022_prepost_rrct_merged$preload_date.y)

b3_2022_prepost_rrct_merged$inc.total.hh.last.mth.earned.x <- as.character(b3_2022_prepost_rrct_merged$inc.total.hh.last.mth.earned.x)
b3_2022_prepost_rrct_merged$inc.total.hh.last.mth.earned.y <- as.character(b3_2022_prepost_rrct_merged$inc.total.hh.last.mth.earned.y)



prepost_ops_all_merged <- bind_rows(
   #2018
  b1_2018_prepost_merged,
  b2_2018_prepost_merged,
  b3_2018_prepost_merged,
   #2019
  b1_2019_prepost_merged,
  b2_2019_prepost_merged,
  b3_2019_prepost_merged,
   #2020
  b1_2020_prepost_merged,
   #2021
  b1_2021_prepost_tfsv1_merged,
  b2_2021_drct_prepost_merged,
  b2_2021_prepost_tfsv2_merged,
   #2022
  b3_2021_prepost_chc_t_merged, # no post 
  B3_2021_prepost_trct_merged,# no post 
  b1_2022_prepost_irct_merged, # no post 
  b2_2022_prepost_irct_merged, # no post 
  b3_2022_prepost_rrct_merged # no post  + ops
 )

write.csv(prepost_ops_all_merged,file='G:/.shortcut-targets-by-id/0BwSiMwXYbhP-LUE0eW54TXl4RlE/ICM Research/0. Data/4.DataFusionProject/04 Working Drive/PrePost updated/prepost-combined/prepost_matched/prepost_ops_all_merged.csv')
###########################################################################################################################
