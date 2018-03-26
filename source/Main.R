rm(list = ls())

root <- '~/Agotado_en_gondola'
source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

RxComputeContext("RxLocalParallel") # Cambiar contexto de ejecución de la máquina a paralelo

# ================ INSTRUCCIONES DE CARGA DE DATOS ======================
# Cargar datos de venta utilizanzo función de R Open a partir de una consulta en Teradata e imprimir
# una tabla de datos con formato xdf

fi <- "'2018-03-19'"                         # Fecha inicial
ff <- "'2018-03-20'"                         # Fecha final
cut_registros <- 0                           # Número mínimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de última venta realizada
cut_tiempo_vida <- 0                        # Tiempo de vida mínimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
cut_proporcion <- 0                          # Proporción de venta mínima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra

booleano <- TRUE
file <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza.txt'
filename <- 'xdf/ventas.xdf'

querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL"
connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
dep <- rxImport(odbcDS)$DependenciaCD
npart <- 20
delta <- round(length(dep)/npart)
nodes <- 8

cl <-makeCluster(nodes)
registerDoParallel(cl)
system.time( 
  result <- foreach (i = 0:(npart-1), .combine='cbind', .export = c('LoadXdf')) %dopar% {
    deptemp <- dep[(1+i*delta):(1+(i+1)*delta)]
    deptemp <- deptemp[!is.na(deptemp)]
    deptemp <- paste(deptemp, collapse = ', ')
    pars <- c(deptemp, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
    venta <- LoadXdf(file, paste0('xdf/ventas_',i,'.xdf'), booleano, pars)
    print(paste0('Finalizado iteracion ',i))
  })
   on.exit(stopCluster(cl))
   rm(cl)

   i <- 0
   venta <- rxImport(inData = paste0('xdf/ventas_',i,'.xdf'), 
            outFile= "xdf/ventas.xdf", overwrite = TRUE)
   acum =  nrow(rxImport(paste0('xdf/ventas_',i,'.xdf')))
   file.remove(paste0('xdf/ventas_',i,'.xdf'))
   
   for (i in 1:(npart-1)){
       rxImport(inData = paste0('xdf/ventas_',i,'.xdf'), 
                outFile= "xdf/ventas.xdf", append = "rows", 
                overwrite = TRUE)
      acum = acum + nrow(rxImport(paste0('xdf/ventas_',i,'.xdf')))
      print(acum)
      file.remove(paste0('xdf/ventas_',i,'.xdf'))
   }
    
   rxDataStep(inData = venta, outFile = venta, overwrite = TRUE, transforms = list(Hora_n = as.numeric(Hora)) )

# ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================

chars_namefile1 <- "xdf/chs1.xdf"
chars_namefile2 <- "xdf/chs2.xdf"
chars_namefile <- "xdf/characteristics.xdf"
xs <- RxCharacteristics(z = venta, name = chars_namefile, name1 = chars_namefile1, name2 = chars_namefile2)

# ================ INSTRUCCIONES PARA LA IDENTIFICACIÓN DE PATRONES ======================
nth <- 8
memo <- '32g'

conn <- h2o.init(ip = "localhost", port=54321, nthreads = nth, max_mem_size = memo)
h2o.removeAll() # Clean slate - just in case the cluster was already running

model <- cluster_model(xs = xs, k_n = 60, nth = nth, memo = memo, dep = 35)

# ==================== ESCRITURA DE ARCHIVO DE SALIDA DE PATRÓN =====================

# centroides

centros <- as.matrix(model[[1]]@model$centers)
storage.mode(centros) <- "numeric"
centros <- data.frame(centros[, 2:ncol(centros)])

chars <- model[[1]]@parameters$x

i <- 1
j <- 2
k <- 4

i <- 15
j <- 19
k <- 21

i <- 1
j <- 5
k <- 7

tamano <- as.matrix(model[[1]]@model$cross_validation_metrics@metrics$centroid_stats)
storage.mode(tamano)

centros <- centros[tamano[, 2] != 0, ]
tamano <- tamano[tamano[,2] != 0, ]

plot_ly(data = centros, 
        x = ~centros[, i], 
        y = ~centros[, j], 
        z = ~centros[, k],
        color = ~tamano[, 2],
        size = ~tamano[, 2],
        marker =  list(symbol = 'circle', sizemode = 'diameter'),
        sizes = c(5, 150)) %>% add_markers() %>% layout(scene = list(xaxis = list(title = chars[i], range = c(0, 10)),
                                                                    yaxis = list(title = chars[j], range = c(0, 50)),
                                                               zaxis = list(title = chars[k], range = c(0, 20))))

library(mailR)


send.mail(from = "jgomezz101@gmail.com",                                                     # Desde
          to = "juan.fernandez@idata.com.co",                                                          # Para
          subject = "Cualquier maricada",                                                         # Asunto
          body = "Buena tarde",                                                            # Cuerpo
          # html = T,  # recibe HTML encoding para el cuerpo
          encoding = "utf-8",                                                            # Codificacion
          #attach.files = c(pathLog),                                                     # rutas de los archivos a adjuntar
          #file.names = c("Log Ejecucion"),                                                       # nombres de los archivos a adjuntar
          smtp = list(host.name = "smtp.gmail.com",                                              # Host del mail
                      port = 465,                                                   # Puerto del host
                      user.name = "jgomezz101@gmail.com",                                    # Usuario que envia
                      passwd = "n1nt3nd0!"),                                                 # Contrase?a del que envia
          authenticate = FALSE,                                                           # Autenticar
          send = TRUE,
          debug=TRUE)