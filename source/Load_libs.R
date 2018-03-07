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
                        "h2o",
                        "RODBC")

  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
  if(length(new.packages)) install.packages(new.packages)
  
  lapply(list.of.packages, require, character.only = TRUE)
  
  # Paquetes de revolutionAnalytics (http://blog.revolutionanalytics.com/packages/page/2/)
  # install.packages(c("dplyr", "dbplyr", "odbc", "sparklyr"), repos = "https://cloud.r-project.org")
  # devtools::install_github("RevolutionAnalytics/dplyrXdf")
  # library(dplyrXdf)
  
}