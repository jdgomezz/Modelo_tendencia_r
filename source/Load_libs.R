Load_libs <- function(root){
  source(paste0(root, '/source/td.R'))
  source(paste0(root,'/source/ownMath.R'))
  source(paste0(root,'/source/Data_transformation.R'))
  source(paste0(root,'/source/Load_data.R'))
  list.of.packages <- c("reshape2",
                        "dplyr", 
                        "moments",
                        "plotly",
                        "xlsx", 
                        "Emcdf",
                        "agrmt", 
                        "ks",
                        "h2o")
  
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
  if(length(new.packages)) install.packages(new.packages)
  
}