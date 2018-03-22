root <- '~/Agotado_en_gondola'
source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

RxComputeContext("RxLocalParallel") # Cambiar contexto de ejecución de la máquina a paralelo

# ================ INSTRUCCIONES DE CARGA DE DATOS ======================
# Cargar datos de venta utilizanzo función de R Open a partir de una consulta en Teradata e imprimir
# una tabla de datos con formato xdf
dep <- '35'
#dep <- "41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569" # Dependencia(s)
fi <- "'2018-01-01'"                         # Fecha inicial
ff <- "'2018-03-20'"                         # Fecha final
cut_registros <- 0                           # Número mínimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de última venta realizada
cut_tiempo_vida <- 0                        # Tiempo de vida mínimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
cut_proporcion <- 0                          # Proporción de venta mínima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra

pars <- c(dep, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
booleano <- TRUE
file <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza.txt'
filename <- 'xdf/ventas.xdf'

venta <- LoadXdf(file, filename, booleano, pars)
rxDataStep(inData = venta, outFile = venta, overwrite = TRUE, transforms = list(Hora_n = as.numeric(Hora)) )

# ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================

chars_namefile1 <- "xdf/chs1.xdf"
chars_namefile2 <- "xdf/chs2.xdf"
chars_namefile <- "xdf/characteristics.xdf"
xs <- RxCharacteristics(z = venta, name = chars_namefile, name1 = chars_namefile1, name2 = chars_namefile2)

# =============== INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ESPECTRALES ============

xs2 <- characteristics_classic(ventas)

# ================ INSTRUCCIONES PARA LA IDENTIFICACIÓN DE PATRONES ======================

conn <- h2o.init(ip = "localhost", port=54321, nthreads = nth, max_mem_size = memo)
h2o.removeAll() # Clean slate - just in case the cluster was already running

model <- cluster_model(xs = xs, k_n = 100, nth = 2, memo ='4g', dep = 35)
