Load_libs <- function(){
  source(paste0(root, '/source/td.R'))
  source(paste0(root,'/source/onwMath.R'))
  source(paste0(root,'/source/Data_transformation.R'))
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