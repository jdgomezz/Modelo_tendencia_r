source('~/source/Load_libs.R')
Load_libs()
#file <- '~/querys/query_extraccion_limpieza.txt'
#ventas <- Load_data(file)

file <- '~/cvs/PRUEBA.csv'
ventas <- Load_csv(file)

xs <- characteristics(ventas)
model <- cluster_model(xs[[1]], 100, 2, '4g')

