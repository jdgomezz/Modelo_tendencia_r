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
booleano <- FALSE
venta <- Load_xdf(file, outfile, booleano)

# Cargar datos de venta utilizanzo función de R Open a partir de una consulta en Teradata e imprimir una tabla de datos
# con formato xdf

connectionString <- "xxxxxx"
claimsXdfFileName <- "yyyyy"
file <- 'Agotado_en_gondola/querys/query_extraccion_limpieza.txt'
nombre <- "venta"
Load_xdf_Odbc(claimsXdfFileName, connectionString, file, nombre)

# ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================
xs <- characteristics(ventas)
model <- cluster_model(xs[[1]], 100, 2, '4g')

