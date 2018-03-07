root <- '~/Agotado_en_gondola'
source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

# ================ INSTRUCCIONES DE CARGA DE DATOS ======================
# Cargar datos con conexión odbc

file <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza.txt'
ventas <- Load_data(file)

# Cargar datos con un read.csv típico
file <- '~/csv/PRUEBA.csv'
ventas <- Load_csv(file)

# Cargar datos de venta utilizanzo función de ROpen apartir de un csv y trabajar con una tbala de datos
# con formato xdf

file <- 'csv/PRUEBA.csv'
outfile <- 'xdf/ventas.xdf'
venta <- Load_Xdf(file, outfile)

# Cargar datos de venta utilizanzo función de R Open a partir de una consulta en Teradata e imprimir
# una tabla de datos con formato xdf

dep <- 35                                    # Dependencia(s)
fi <- "'2017-01-01'"                         # Fecha inicial
ff <- "'2018-02-20'"                         # Fecha final
cut_registros <- 0                           # Número mínimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de última venta realizada
cut_tiempo_vida <- 30                        # Tiempo de vida mínimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
cut_proporcion <- 1                          # Proporción de venta mínima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra

pars <- c(dep, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
booleano <- TRUE
file <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza.txt'
filename <- 'xdf/ventas.xdf'

venta <- LoadXdf(file, filename, booleano, pars)
  
# ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================
xs <- characteristics(ventas)
model <- cluster_model(xs[[1]], 100, 2, '4g')

