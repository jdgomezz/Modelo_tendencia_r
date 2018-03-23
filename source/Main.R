root <- '~/Agotado_en_gondola'
source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

RxComputeContext("RxLocalParallel")          # Cambiar contexto de ejecuciÃ³n de la mÃ¡quina a paralelo

# =========================================== REGLAS DE NEGOCIO ========================================

fi <- "'2018-01-01'"                         # Fecha inicial
ff <- "'2018-03-20'"                         # Fecha final
cut_registros <- 0                           # NÃºmero mÃ­nimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de Ãºltima venta realizada
cut_tiempo_vida <- 0                        # Tiempo de vida mÃ­nimo por plu-dep ABS(Fecha 1ra venta - Fecha Ãºltima venta)
cut_proporcion <- 0                          # ProporciÃ³n de venta mÃ­nima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra

pars <- c(fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
booleano <- TRUE
query <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza.txt'

# ================================== PARÁMETROS DE CONEXIÓN Y PARALELIZACIÓN ============================

npart = 20                                  # Número de particiones para la carga de información histórica
nodes = 8                                   # Número de procesadores a utilizar
querydep = "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL" # Query para extracción de dependencias activas
server = "10.2.113.66"                      # Dirección IP del servidor
user = "jdgomezz"                           # Usuario en servidor
pwd = "jdgomezz01"                          # Contraseña de usuario en servidor
path = "xdf/ventas"                         # Ruta de los archivos de salida

# ======================================INSTRUCCIONES DE CARGA DE DATOS ===============================
# Cargar datos de venta utilizanzo funciÃ³n de R Open a partir de una consulta en Teradata e imprimir
# una tabla de datos con formato xdf

venta <- Load_data_par(npart = npart,
                       nodes = nodes, 
                       params = pars,
                       querydep = querydep, 
                       query = query,
                       server = server, 
                       user = user,
                       pwd = pwd, 
                       path = path)
  
rxDataStep(inData = venta, outFile = venta, overwrite = TRUE, transforms = list(Hora_n = as.numeric(Hora)) )

# ====================== INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ================================

chars_namefile1 <- "xdf/chs1.xdf"
chars_namefile2 <- "xdf/chs2.xdf"
chars_namefile <- "xdf/characteristics.xdf"
xs <- RxCharacteristics(z = venta, name = chars_namefile, name1 = chars_namefile1, name2 = chars_namefile2)

# ===================== INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ESPECTRALES ====================

# Falta por construir este procedimiento

# ======================= INSTRUCCIONES PARA LA IDENTIFICACIÃ“N DE PATRONES =============================

conn <- h2o.init(ip = "localhost", port=54321, nthreads = nth, max_mem_size = memo)
h2o.removeAll() # Clean slate - just in case the cluster was already running

model <- cluster_model(xs = xs, k_n = 100, nth = 2, memo ='4g', dep = 35)
