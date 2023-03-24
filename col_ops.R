# requires precb_modifs to have the same names for everything
# colname, options - options
# options lists - list_name
# colname new - new

# options | COLUMN NAME & ORDER REQUIREMENTS
# OPTIONS_NAME (name of options list)
# VALUE	(value in tf_stretch_preset)
# LABEL (new assigned label)

# precb_modif | COLUMN NAME REQUIREMENT
# OPTIONS_NAME (matches options-name from options list)	
# NEW (this must be the name of the column names)

# INPUT REQUIREMENTS
# tf_stretch_pre: base tf_stretch_preset required for updating
# precb_modif: colname file, that has old, new colnames and column options lists
# options: options list file
# label: string -> "LABEL" or "LABEL.NEW" (type of label that we want to change the tf_stretch_presets into) (default = LABEL)
col_options <- function(tf_stretch_mid, precb_modif, options, label="LABEL") {
  
  require(dplyr)
  # take all unique options, and match it with those in the precb_modif
  lst <- unique(precb_modif$OPTIONS_NAME)
  lst <- lst[lst!=""]
  lst <- lst[!is.na(lst)]
  
  oplst <- unique(options$OPTIONS_NAME)
  
  srch <- lst[lst %in% oplst] # only values we need to use
  opfin <- options[options$OPTIONS_NAME %in% srch, ] # final list of options
  
  # Rename all variables to labels 
  # go through all option lists
  for (i in (1:length(srch))){
    opt <- srch[i]  # options type to use
    cols <- precb_modif$New[precb_modif$OPTIONS_NAME==opt] # column name(s) we are modifying
    loc <- which(colnames(tf_stretch_mid) %in% cols) # which number column it is
    
    # created names character list of variables and new labels (cols 2 & 3 - since 1 is all the same)
    tbl <- opfin[opfin$OPTIONS_NAME == opt, c("VALUE", label)] 
    trtbl <- t(tbl) # transpose table to create name chr list
    colnames(trtbl) <- trtbl[1, ] # modify colnames to be the values (which is the first row)
    trtbl <- trtbl[-1, ] # remove first row which are now the column names, this is now a names chr list
    
    # nested for loop to modify all columns with that option list (ex: yes_no_only) [tf_stretch_mid]
    for (j in loc){
      if (!all(is.na(tf_stretch_mid[,j]))){
        tf_stretch_mid[,j] <- recode(tf_stretch_mid[,j], !!!trtbl, .default = NA_character_)
      }
    }
  }
  
  return(tf_stretch_mid)
}
