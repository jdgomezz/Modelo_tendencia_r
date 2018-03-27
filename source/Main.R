rm(list = ls())
setwd("~/")
root <- '~/Agotado_en_gondola'
inSource <- "xdf_test/"
landing <- "xdf_test/"
model_lib <- "xdf_test/"
output_lib <- "xdf_test/"

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
outfile_ventas <- paste0(inSource, 'ventas.xdf')

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
    LoadXdf(file, paste0(inSource, 'ventas_',i,'.xdf'), booleano, pars)
    print(paste0('Finalizado iteracion ',i))
  })
   on.exit(stopCluster(cl))
   rm(cl)

   i <- 0
   venta <- rxImport(inData = paste0(inSource,'ventas_',i,'.xdf'), 
            outFile = outfile_ventas, overwrite = TRUE)
   acum =  nrow(rxImport(paste0(inSource, 'ventas_',i,'.xdf')))
   file.remove(paste0(inSource, 'ventas_',i,'.xdf'))
   
   xs <- rx
   for (i in 1:(npart-1)){
       venta <- rxImport(inData = paste0(inSource, 'ventas_',i,'.xdf'), 
                         outFile= outfile_ventas,
                         append = "rows", 
                         overwrite = TRUE)
      acum = acum + nrow(rxImport(paste0(inSource, 'ventas_',i,'.xdf')))
      print(acum)
      file.remove(paste0(inSource, 'ventas_',i,'.xdf'))
   }
    
   rxDataStep(inData = venta, 
              outFile = venta,
              overwrite = TRUE,
              transforms = 
              list(Hora_n = as.numeric(Hora)) )

# ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================

chars_namefile1 <- paste0(landing, "chs1.xdf")
chars_namefile2 <-  paste0(landing, "chs2.xdf")
chars_namefile <-  paste0(landing, "characteristics.xdf")

xs <- RxCharacteristics(z = venta, 
                        name = chars_namefile,
                        name1 = chars_namefile1,
                        name2 = chars_namefile2)

# ================ INSTRUCCIONES PARA LA IDENTIFICACIÓN DE PATRONES ======================
nth <- 8      # Número de procesadores a utilizar
memo <- '32g' # Memoria RAM a utilizar
k_n <- 60     # Número de clusters deseados

conn <- h2o.init(ip = "localhost",
                 port=54321, 
                 nthreads = nth, 
                 max_mem_size = memo)

h2o.removeAll() # Clean slate - just in case the cluster was already running

y <- c("m1", "moda", "q1", "q2", "q3", "asim_pearson", "m1h", "modah", "q1h", "q2h", "q3h", "asim_pearsonh") # Lista de características a considerar en el proceso de clusterización

model <- cluster_model(xs = xs,
                       k_n = k_n, 
                       nth = nth, 
                       memo = memo,
                       y = y)

# Visualización/impresión de métricas de desempeño de los centroides

# Extraer los centros

centros <- as.matrix(model[[1]]@model$centers)
storage.mode(centros) <- "numeric"
centros <- data.frame(centros[, 2:ncol(centros)])

chars <- model[[1]]@parameters$x

# Escoger las características a visualizar

i <- 1
j <- 2
k <- 4

tamano <- as.matrix(model[[1]]@model$cross_validation_metrics@metrics$centroid_stats)
storage.mode(tamano)

# Omitir los grupos que no tengan muestras

centros <- centros[tamano[, 2] != 0, ]
tamano <- tamano[tamano[,2] != 0, ]

# Graficación de los centroides del los grupos

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

# ===== CONSTRUCCIÓN DE PATRONES A TRAVÉS DE LA ESTIMACIÓN DE FUNCIONES DE DENSIDAD CON KERNEL GAUSSIANO ===================================

# Estimación de densidad de probabilidad con el kernel density estimation package 
# para hallar las distribuciones de los patrones

rxDataStep(inData = outfile_ventas,
           outFile = outfile_ventas, 
           overwrite = TRUE, 
           transforms = list(concat = paste0(Pluid,"-", DependenciaCD, "-", dia) ))

patrones$concat <- paste0(patrones$Pluid,"-", patrones$DependenciaCD, "-", patrones$dia);
rxImport(inData = patrones, outFile = paste0(model_lib, "patrones.xdf"));

xs <- rxMerge(inData1 = outfile_ventas,
              inData2 = paste0(model_lib, "patrones.xdf"),
              outFile = paste0(model_lib, "venta_f.xdf"),
              matchVars = c("concat"), 
              type = "inner",
              overwrite = TRUE)

data <- rxImport(xs)

dat <- data.frame(concat = data$concat,
                  Pluid = data$Pluid.ventas,
                  DependenciaCD = data$DependenciaCD.ventas, 
                  dia = data$dia.ventas, 
                  Hora = data$Hora_n,
                  UnidadesVendidas = data$UnidadesVendidas);

llaves_patrones <- unique(dat$concat)
nc <- length(llaves_patrones)

pattern <- data.frame(patron = 1,
                      Hora = 1,
                      unds = 1,
                      weight = 1)
pattern[-1, ]
for (i in 1:nc){
  auxdata <- dat[dat$concat == llaves_patrones[i], ]
  auxdata <- auxdata[, !(names(auxdata) %in% c("concat", "DependenciaCD", "Pluid", "dia"))]
  
  # Estimación de función de densidad con kernel gaussiano
  
  Hpi1 <- Hpi(x=auxdata);
  fhat.pi1 <- kde(x=auxdata, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  
  # Adimensionalización de la distribición [a, b] -> [0, 1]
  
  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  xy <- expand.grid(u, v)
  est_dist <- as.vector(est_dist)
  
  aux <- data.frame(patron = i,
                        Hora = xy[, 1],
                        unds = xy[, 2],
                        weight = est_dist)
  
  pattern <- cbind(aux, pattern)
  # convertir la matriz en un vector
  #plot_ly(x = v, y = u, z= est_dist) %>% add_surface() 
}
write.csv(pattern, file = paste0(output_lib, "patterns.csv"))

# ==================== ESCRITURA DE ARCHIVO DE SALIDA DE PATRÓN =====================

system('sshpass -p "hadoop" scp ~/xdf_test/patterns.csv hdp_agotadoln@10.2.113.138/data/LZ/Agotados/Datos/patterns.csv')

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