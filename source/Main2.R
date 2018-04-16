rm(list = ls())

# Onlinux
setwd("~/")

# On Windows
#setwd("C:/Users/User/Desktop")

wd <- getwd()
root <- paste0(wd, '/Agotado_en_gondola')
inSource <- "xdf/"
landing <- "xdf/"
model_lib <- "xdf/"
output_lib <- "xdf/"
log_lib <- "xdf/"

libs <- list(inSource, landing, model_lib, output_lib, log_lib)
# ================== DECLARACIÓN DE LIBRERIAS EXTERNAS Y PROPIAS ===================

source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

RxComputeContext("RxLocalParallel") # Cambiar contexto de ejecución de la máquina a paralelo

# ================ INSTRUCCIONES DE CARGA DE DATOS ========================================

# Cargar datos de venta utilizanzo función de R Open a partir de una consulta en Teradata e imprimir
# una tabla de datos con formato xdf

fi <- "'2018-01-01'"                         # Fecha inicial
ff <- "'2018-03-31'"                         # Fecha final
cut_registros <- 0                          # Número m??nimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de última venta realizada
cut_tiempo_vida <- 30                         # Tiempo de vida m??nimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
cut_proporcion <- 1                          # Proporción de venta m??nima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra
booleano <- TRUE
query <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza_v2.txt'
pars <- list(fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles, booleano)
querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL and DependenciaCD in (41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569)"
#querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL"
connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
dep <- rxImport(odbcDS)$DependenciaCD
npart <- 40
delta <- length(dep)%/%npart
nodes <- 8

tiendas <- c(41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569)
nth <- 8
k_n <- 50
memo <- '16g'
nodes <- 8

h2o.removeAll() # Clean slate - just in case the cluster was already running

yo <- c("sesgoh", "asim_bowleyh", "q2h")    # Caracteristicas a visualizar
y <- c("asim_pearsonh", "curtosish", "q1h",  "q2h", "q3h", "sesgoh", "modah", "m1h", "asim_bowleyh")
cl <-makeCluster(nodes)
registerDoParallel(cl)
system.time( 
  result <- foreach (i = 1:length(tiendas), .combine='cbind', .export = c('LoadXdf', "RxCharacteristics", "cluster_model", "Mode"), .packages = c("moments", "data.table", "h2o", "plotly")) %dopar% {
                  patrones_dep <- MainByDep(tiendas[i], (i-1),  query, libs, pars, nth, k_n, memo, y, yo, fileConn, TRUE)
  }
)
stopCluster(cl)
rm(cl)  
# ==================== CONSOLIDACIÓN DEL ARCHIVO DE SALIDA ============================
outfile_patrones <- paste0(output_lib, "patrones_all_deps.xdf")

DEPS <- 1
outfile<- rxImport(paste0(output_lib, "patrones_all_", tiendas[DEPS] , ".xdf"))

for (DEPS in 2:length(tiendas)){
  aux <- rxImport(paste0(output_lib, "patrones_all_",  tiendas[DEPS] , ".xdf"))
  outfile <- rbind(outfile, aux)
  print(DEPS)
}

patrones_all <- rxImport(inData = outfile,
                        outFile = outfile_patrones,
                        overwrite = TRUE)
# ==================== ESCRITURA DE ARCHIVO DE SALIDA DE PATRÓN =====================

# Organizaci?n de formatro de archivo de salida

dias <- c(Sys.Date()- as.POSIXlt(Sys.Date())$wday,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 1,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 2,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 3,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 4,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 5,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 6)# Primer d?a de la semana
fecha_dia = data.frame(dia = 1:7, diafecha = dias)

outfile <- merge(x = outfile, y = fecha_dia, by = "dia", all.x = TRUE)
outfile <- subset(outfile, select = c(7, 2, 3, 5, 6))
colnames(outfile) <- c("Fecha", "Pluid", "storeid", "Hora", "Perfil")

# Escritura de archivo final

outfile$Fecha <- format(outfile$Fecha, "%Y-%m-%d")
outfile$storeid <- as.numeric(as.character(outfile$storeid))
outfile$Pluid <- as.numeric(as.character(outfile$Pluid))
outfile$Hora <- as.numeric(as.character(outfile$Hora))
outfile$Fecha <- as.Date(outfile$Fecha) 
write.table(outfile, file = paste0(output_lib, "tendencia.csv"), sep=",", row.names = FALSE)

system('sshpass -p "hadoop" scp ~/xdf/tendencia.csv hdp_agotadoln@10.2.113.138:/data/LZ/Agotados/Datos/tendencia.csv')

# ================= Evaluaci?n de los patrones =========================================


# ================ ENVIAR E-MAIL DE FINALIZACIÓN DE PROCESO =============================


send.mail(from = "jgomezz101@gmail.com",                                                     # Desde
          to = "jdgomezz@grupo-exito.com",                                                  # Para
          subject = "Reporte eficiencia modelo de tendencia",                                # Asunto
          body = "Buena tarde",                                                              # Cuerpo
          # html = T,  # recibe HTML encoding para el cuerpo
          encoding = "utf-8",                                                                # Codificacion
          #attach.files = c(pathLog),                                                        # rutas de los archivos a adjuntar
          #file.names = c("Log Ejecucion"),                                                  # nombres de los archivos a adjuntar
          smtp = list(host.name = "smtp.gmail.com",                                          # Host del mail
                      port = 465,                                                            # Puerto del host
                      user.name = "jgomezz101@gmail.com",                                    # Usuario que envia
                      passwd = "N1nt3nd0\""),                                                 # Contrase?a del que envia
          ssl = TRUE,
          authenticate = FALSE,                                                              # Autenticar
          send = TRUE,
          debug=TRUE)
