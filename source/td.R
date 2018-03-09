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
                    query = "select top 10 * from bd_ddpo.vwventashora",
                    name = "xdf/venta.xdf",
                    booleano = TRUE){
  
  char <- paste0("DRIVER=Teradata; DBCNAME=", server, "; UID=", uid, "; PWD=", pwd, ';')
  rxTeradataDS  <- RxOdbcData(sqlQuery = query, connectionString = char)
  #rxTeradataDS  <- RxTeradata(sqlQuery = query, connectionString = char)
  data <- rxImport( inData = rxTeradataDS, outFile = name, overwrite = booleano)
  rm(char, rxTeradataDS)
  return(data)
  
}

#  Extraccion desde teradata via conexion rjdbc
td_jdbc <- function (JAR_DIR =  "/home/centralrepo/TeraJDCB/", query = 'select top 10 * from bd_ddpo.vtsublinea'){
  .jaddClassPath(paste(JAR_DIR, "terajdbc4.jar", sep=""))
  .jaddClassPath(paste(JAR_DIR, "tdgssconfig.jar", sep=""))
  
  drv <- try(JDBC("com.teradata.jdbc.TeraDriver", paste0(JAR_DIR, 'terajdbc4.jar')))
  
  drv <- JDBC(driverClass="com.teradata.jdbc.TeraDriver")
  database <- dbConnect(drv,"jdbc:teradata://10.2.113.66/TMODE=ANSI,charset=UTF8,TYPE=FASTEXPORT,USER=jdgomezz,PASSWORD=jdgomezz01")
  dbGetQuery(database, query = query)
}


