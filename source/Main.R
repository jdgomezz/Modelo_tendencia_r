root <- '~/Agotado_en_gondola'
source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)
#file <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza.txt'
#ventas <- Load_data(file)

file <- '~/cvs/PRUEBA.csv'
ventas <- Load_csv(file)

xs <- characteristics(ventas)
model <- cluster_model(xs[[1]], 100, 2, '4g')

