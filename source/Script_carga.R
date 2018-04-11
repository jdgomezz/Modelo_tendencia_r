rm(list = ls())
# Onlinux
setwd("~/")

# On Windows
#setwd("C:/Users/User/Desktop")

wd <- getwd()
root <- paste0(wd, '/Agotado_en_gondola')
inSource <- "xdf_2/"
landing <- "xdf_2/"
model_lib <- "xdf_2/"
output_lib <- "xdf_2/"

# ================== DECLARACIÓN DE LIBRERIAS EXTERNAS Y PROPIAS ===================

source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

RxComputeContext("RxLocalParallel") # Cambiar contexto de ejecución de la máquina a paralelo

# ======================== LIMPIEZA DE TODOS LOS DIRECTORIOS ==============================

#system(paste0("rm -r ", inSource, "*.xdf"))
#system(paste0("rm -r ", landing, "*.xdf"))
#system(paste0("rm -r ", model_lib, "*.xdf"))
#system(paste0("rm -r ", output_lib, "*.xdf"))

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
file <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza_v2.txt'
outfile_ventas <- paste0(inSource, 'ventas.xdf')

#querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL and DependenciaCD in (41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569)"
querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL"
connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
dep <- rxImport(odbcDS)$DependenciaCD
npart <- 8
delta <- length(dep)%/%npart
nodes <- 8

acum <- 0
for (i in 0:npart){
  deptemp <- dep[(1+i*delta):(1+(i+1)*delta)]
  deptemp <- deptemp[-(delta+1)]
  deptemp <- deptemp[!is.na(deptemp)]
  print(paste0(length(deptemp) + acum, " ", length(deptemp)))
  acum <- acum + length(deptemp)
}

cl <-makeCluster(nodes)
registerDoParallel(cl)
system.time( 
  result <- foreach (i = 0:npart, .combine='cbind', .export = c('LoadXdf')) %dopar% {
    deptemp <- dep[(1+i*delta):(1+(i+1)*delta)]
    deptemp <- deptemp[-(delta+1)]
    deptemp <- deptemp[!is.na(deptemp)]
    deptemp <- paste(deptemp, collapse = ', ')
    pars <- c(deptemp, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
    LoadXdf(file, paste0(file = inSource, filename ='ventas_',i,'.xdf'), booleano = booleano, pars = pars)
    print(paste0('Finalizado iteracion ',i))
    rm(deptemp, pars)
  })
stopCluster(cl)
rm(cl)

i <- 0
venta <- rxImport(inData = paste0(inSource,'ventas_',i,'.xdf'), 
                  outFile = outfile_ventas, overwrite = TRUE)
acum =  nrow(rxImport(paste0(inSource, 'ventas_',i,'.xdf')))
#file.remove(paste0(inSource, 'ventas_',i,'.xdf'))

for (i in 1:(npart-1)){
  venta <- rxImport(inData = paste0(inSource, 'ventas_',i,'.xdf'), 
                    outFile= outfile_ventas,
                    append = "rows", 
                    overwrite = TRUE)
  acum = acum + nrow(rxImport(paste0(inSource, 'ventas_',i,'.xdf')))
  print(i)
  # file.remove(paste0(inSource, 'ventas_',i,'.xdf'))
}

rxDataStep(inData = venta, 
           outFile = venta,
           overwrite = TRUE,
           transforms = 
             list(Hora_n = as.numeric(hora), concat = paste0(Pluid, "-", DependenciaCD, "-", dia)) )