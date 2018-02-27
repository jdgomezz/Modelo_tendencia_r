#  Función para conectar R (en la nube con Teradata Exito)
library(RODBC)
td <- function (server = 'TeradataODBC', 
                uid = 'jdgomezz', 
                pwd = 'jdgomezz01', 
                query = "select top 10 * from bd_ddpo.vtsublinea"){
  
  char <- paste("Driver=Teradata;DBCName=", server, ";UID=", uid, "PWD=", pwd);
  ch <- odbcConnect(dsn= server, uid = uid, pwd = pwd)
  data <- sqlQuery(ch, query);      # Ejecuta consulta
  odbcClose(ch);                    # Cierra conexión
  rm(char, ch);                     # Remueve variables
  return(data);                     # Devuelve resultados de la consulta
}