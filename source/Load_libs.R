Load_libs <- function(){
  source('~/source/td.R')
  source('~/source/onwMath.R')
  source('~/source/Data_transformation.R')
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