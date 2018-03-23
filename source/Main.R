root <- '~/Agotado_en_gondola'
source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

RxComputeContext("RxLocalParallel")          # Cambiar contexto de ejecución de la máquina a paralelo

# =========================================== REGLAS DE NEGOCIO ========================================

fi <- "'2018-03-21'"                         # Fecha inicial
ff <- "'2018-03-21'"                         # Fecha final
cut_registros <- 0                           # Número mínimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de última venta realizada
cut_tiempo_vida <- 0                        # Tiempo de vida mínimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
cut_proporcion <- 0                          # Proporción de venta mínima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra

pars <- c(fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
booleano <- TRUE
query <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza.txt'

# ================================== PAR?METROS DE CONEXI?N Y PARALELIZACI?N ============================

npart = 20                                  # N?mero de particiones para la carga de informaci?n hist?rica
nodes = 8                                   # N?mero de procesadores a utilizar
querydep = "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL" # Query para extracci?n de dependencias activas
server = "10.2.113.66"                      # Direcci?n IP del servidor
user = "jdgomezz"                           # Usuario en servidor
pwd = "jdgomezz01"                          # Contrase?a de usuario en servidor
path = "xdf/ventas"                         # Ruta de los archivos de salida

# ======================================INSTRUCCIONES DE CARGA DE DATOS ===============================
# Cargar datos de venta utilizanzo función de R Open a partir de una consulta en Teradata e imprimir
# una tabla de datos con formato xdf

querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL"
connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
dep <- rxImport(odbcDS)$DependenciaCD
npart <- 20
delta <- round(length(dep)/npart)

cl <-makeCluster(nodes)
registerDoParallel(cl)
system.time( 
  result <- foreach (i = 0:(npart-1), .combine='cbind', .export = c('LoadXdf')) %dopar% {
    deptemp <- dep[(1+i*delta):(1+(i+1)*delta)]
    deptemp <- deptemp[!is.na(deptemp)]
    deptemp <- paste(deptemp, collapse = ', ')
    pars <- c(deptemp, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
    
    venta <- LoadXdf(server = server,
                     uid = user,
                     pwd = pwd,
                     file = query, 
                     filename = paste0('xdf/ventas_',i,'.xdf'),
                     booleano = booleano, 
                     pars = pars)
    print(paste0('Finalizado iteracion ',i))
  })
  stopCluster(cl)
  
rxDataStep(inData = venta, outFile = venta, overwrite = TRUE, transforms = list(Hora_n = as.numeric(Hora)) )

# ====================== INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ================================

chars_namefile1 <- "xdf/chs1.xdf"
chars_namefile2 <- "xdf/chs2.xdf"
chars_namefile <- "xdf/characteristics.xdf"
xs <- RxCharacteristics(z = venta, name = chars_namefile, name1 = chars_namefile1, name2 = chars_namefile2)

# ===================== INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ESPECTRALES ====================

# Falta por construir este procedimiento

# ======================= INSTRUCCIONES PARA LA IDENTIFICACIÓN DE PATRONES =============================

conn <- h2o.init(ip = "localhost", port=54321, nthreads = nth, max_mem_size = memo)
h2o.removeAll() # Clean slate - just in case the cluster was already running

model <- cluster_model(xs = xs, k_n = 100, nth = 2, memo ='4g', dep = 35)
