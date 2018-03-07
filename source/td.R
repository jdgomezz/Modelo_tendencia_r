#  Función para conectar R (en la nube con Teradata Exito)
td <- function (server = '10.2.113.66', 
                uid = 'jdgomezz', 
                pwd = 'jdgomezz01', 
                query = "select top 10 * from bd_ddpo.vtsublinea"){
  
  char <- paste0("DRIVER=Teradata; DBCNAME=", server, "; UID=", uid, "; PWD=", pwd, ';')
  ch <- odbcDriverConnect(char)
  data <- sqlQuery(ch, query);      # Ejecuta consulta
  odbcClose(ch);                    # Cierra conexión
  rm(char, ch);                     # Remueve variables
  return(data);                     # Devuelve resultados de la consulta
}

td_Xdf <- function(server = '10.2.113.66', 
                    uid = 'jdgomezz', 
                    pwd = 'jdgomezz01', 
                    query = "select top 10 * from bd_ddpo.vtsublinea",
                    name = "xdf/test.xdf"){
  char <- paste0("DRIVER=Teradata; DBCNAME=", server, "; UID=", uid, "; PWD=", pwd, ';')
  rxTeradataDS  <- RxTeradata(sqlQuery = query, connectionString = char)
  data  <- rxImport(rxTeradataDS, name, overwrite = TRUE)
  return(data)
}
