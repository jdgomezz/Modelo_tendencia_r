rm(list = ls())
# Onlinux
#setwd("~/")

# On Windows
setwd("C:/Users/User/Desktop")

wd <- getwd()
root <- paste0(wd, '/modelo_tendencia_r')
inSource <- "xdf/"
landing <- "xdf/"
model_lib <- "xdf/"
output_lib <- "xdf/"

# ================== DECLARACIÓN DE LIBRERIAS EXTERNAS Y PROPIAS ===================

source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

RxComputeContext("RxLocalParallel") # Cambiar contexto de ejecución de la máquina a paralelo

# ======================== LIMPIEZA DE TODOS LOS DIRECTORIOS ==============================

system(paste0("rm -r ", inSource, "*.xdf"))
system(paste0("rm -r ", landing, "*.xdf"))
system(paste0("rm -r ", model_lib, "*.xdf"))
system(paste0("rm -r ", output_lib, "*.xdf"))
                            
# ================ INSTRUCCIONES DE CARGA DE DATOS ========================================

# Cargar datos de venta utilizanzo función de R Open a partir de una consulta en Teradata e imprimir
# una tabla de datos con formato xdf

fi <- "'2018-01-01'"                         # Fecha inicial
ff <- "'2018-03-27'"                         # Fecha final
cut_registros <- 30                          # Número m�?nimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de última venta realizada
cut_tiempo_vida <- 30                         # Tiempo de vida m�?nimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
cut_proporcion <- 1                          # Proporción de venta m�?nima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra

booleano <- TRUE
file <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza.txt'
outfile_ventas <- paste0(inSource, 'ventas.xdf')

querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL and DependenciaCD in (41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569)"
connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
dep <- rxImport(odbcDS)$DependenciaCD
npart <- 2
delta <- round(length(dep)/npart)
nodes <- 2

cl <-makeCluster(nodes)
registerDoParallel(cl)
system.time( 
  result <- foreach (i = 0:(npart-1), .combine='cbind', .export = c('LoadXdf')) %dopar% {
    deptemp <- dep[(1+i*delta):(1+(i+1)*delta)]
    deptemp <- deptemp[!is.na(deptemp)]
    deptemp <- paste(deptemp, collapse = ', ')
    pars <- c(deptemp, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
    LoadXdf(file, paste0(file = inSource, filename ='ventas_',i,'.xdf'), booleano = booleano, pars = pars)
    print(paste0('Finalizado iteracion ',i))
    rm(deptemp, pars)
  })
   stopCluster(cl)
   rm(cl)

   i <- 1
   venta <- rxImport(inData = paste0(inSource,'ventas_',i,'.xdf'), 
            outFile = outfile_ventas, overwrite = TRUE)
   acum =  nrow(rxImport(paste0(inSource, 'ventas_',i,'.xdf')))
   #file.remove(paste0(inSource, 'ventas_',i,'.xdf'))
   
   for (i in 2:npart){
       venta <- rxImport(inData = paste0(inSource, 'ventas_',i,'.xdf'), 
                         outFile= outfile_ventas,
                         append = "rows", 
                         overwrite = TRUE)
      acum = acum + nrow(rxImport(paste0(inSource, 'ventas_',i,'.xdf')))
      print(i)
     # file.remove(paste0(inSource, 'ventas_',i,'.xdf'))
   }
   
venta0 = "xdf/ventas_3.xdf"
venta = "xdf/ventas_30.xdf"

rxDataStep(inData = venta0, 
              outFile = venta,
              overwrite = TRUE,
              transforms = 
              list(Hora_n = as.numeric(Hora), concat = paste0(Pluid, "-", DependenciaCD, "-", dia)) )

# ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================
chars_namefile1 <- paste0(landing, "chs1.xdf")
chars_namefile2 <-  paste0(landing, "chs2.xdf")
chars_namefile <-  paste0(landing, "characteristics_3.xdf")

xs <- RxCharacteristics(z = venta, 
                        name = chars_namefile,
                        name1 = chars_namefile1,
                        name2 = chars_namefile2)

# ================ INSTRUCCIONES PARA LA IDENTIFICACIÓN DE PATRONES ======================
nth <- 2      # Número de procesadores a utilizar
memo <- '8g' # Memoria RAM a utilizar
k_n <- 3     # Número de clusters deseados

conn <- h2o.init(ip = "localhost",
                 port=54321, 
                 nthreads = nth, 
                 max_mem_size = memo)


# Todas las caracter�sticas disponibles

y <- c("m1", "m2", "m3", "sd", "n", "moda", "q1", "q2", "q3", "sesgo", "curtosis", "asim_fisher", "asim_pearson0", "asim_pearson", "asim_bowley", "m1h", "m2h", "m3h", "sdh", "nh", "modah", "q1h", "q2h", "q3h", "sesgoh", "curtosish", "asim_fisherh", "asim_pearson0h", "asim_pearsonh", "asim_bowleyh")

xs <- paste0(landing, 'characteristics_3.xdf') # Archivo de caracter�sticas de insumo para la clusterizaci�n

yo <- c("curtosish", "asim_bowleyh", "q2h")    # Caracteristicas a visualizar
y <- c("asim_pearsonh", "curtosish", "q1h",  "q2h", "q3h", "sesgoh", "modah", "m1h", "asim_bowleyh")

for (Master_I in 1:7){
  h2o.removeAll() # Clean slate - just in case the cluster was already running
  model <- cluster_model(xs = xs,
                         k_n = 10, 
                         nth = nth, 
                         memo = memo,
                         y = y,
                         yo = yo,
                         dep = 33,
                         dia = Master_I,
                         estimate_k = FALSE)
  
  # model[[3]]
  
  # ===== CONSTRUCCIÓN DE PATRONES A TRAVÉS DE LA ESTIMACIÓN DE FUNCIONES DE DENSIDAD CON KERNEL GAUSSIANO ===================================
  
  # Estimación de densidad de probabilidad con el kernel density estimation package 
  # para hallar las distribuciones de los patrones
  
  patterns <- rxImport(inData = model[[2]], outFile = paste0(model_lib, "patrones.xdf"), overwrite = TRUE)
  
  venta_f <- rxMerge(inData1 = outfile_ventas,
                inData2 = patterns,
                outFile = paste0(model_lib, "venta_f.xdf"),
                matchVars = c("concat"), 
                type = "inner",
                overwrite = TRUE)
  
  data <- rxImport(venta_f)
  
  dat <- data.frame(cluster_id = data$cluster_id,
                    concat = data$concat,
                    Pluid = data$Pluid.ventas,
                    DependenciaCD = data$DependenciaCD.ventas, 
                    dia = data$dia.ventas, 
                    Hora = data$Hora_n,
                    UnidadesVendidas = data$UnidadesVendidas)
  
  llaves_patrones <- unique(dat$concat)
  nc <- length(llaves_patrones)
  
  ## Distribuciones Hora
  patron <- data.frame(x = 1,
                       y = 1,
                       cluster_id = 1)
  for (i in 1:nc){
    auxdata <- dat[dat$concat == llaves_patrones[i], ]
    cluster_id <- unique(auxdata$cluster_id)
  
    auxdata <- auxdata[, !(names(auxdata) %in% c("cluster_id", "UnidadesVendidas", "concat", "DependenciaCD", "Pluid", "dia"))]
    den <- density(x = auxdata, kernel = "gaussian", from = 8, to = 21);
    den$y <- den$y/sum(den$y)
    den$x <- floor(den$x)
    data <- data.table(x = den$x, y = den$y)
    # Agrupar por horas enteras
    new <- data.frame(data %>% group_by(x) %>% summarise(y = sum(y)))
    new$cluster_id <- rep(cluster_id, nrow(new))
    rm(data, auxdata, den)
    patron <- rbind(patron, new)
  }
  patron <- patron[-1, ]
  
  patron.xdf <- rxImport(inData = patron, outFile = paste0(output_lib, "patrones.xdf"), overwrite = TRUE)
  llaves.xdf <- rxImport(inData = model[[4]], outFile = paste0(output_lib, "geoprodtime.xdf"), overwrite = TRUE)
  
  patrones_expandidos.xdf <- rxMerge(inData1 = llaves.xdf,
                                     inData2 = patron.xdf,
                                     outFile = paste0(output_lib, "patrones_geoprodtime_", Master_I,".xdf"),
                                     matchVars = c("cluster_id"),
                                     varsToKeep1 =c("Pluid", "DependenciaCD", "dia", "cluster_id"),
                                     varsToKeep2 =c("cluster_id", "x", "y"),
                                     type = "inner",
                                     overwrite = TRUE)
}

I <- 1
patrones_all <- rxImport(paste0(output_lib,'patrones_geoprodtime_',I,'.xdf'))

for (I in 2:7){
  aux <- rxImport(paste0(output_lib,'patrones_geoprodtime_',I,'.xdf'))
  patrones_all <- rbind(patrones_all, aux)
}


# ==================== ESCRITURA DE ARCHIVO DE SALIDA DE PATRÓN =====================

# Organizaci�n de formatro de archivo de salida

dias <- c(Sys.Date()- as.POSIXlt(Sys.Date())$wday,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 1,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 2,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 3,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 4,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 5,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 6)# Primer d�a de la semana
fecha_dia = data.frame(dia = 1:7, diafecha = dias)

patrones_all <- merge(x = patrones_all, y = fecha_dia, by = "dia", all.x = TRUE)
patrones_all <- subset(patrones_all, select = c(3, 2, 7, 5, 6))
colnames(patrones_all) <- c("DependenciaCD", "Pluid", "Fecha", "Hora", "Perfil")

# Escritura de archivo final

write.csv(patrones_all, file = paste0(output_lib, "patterns.csv"))
system('sshpass -p "hadoop" scp ~/xdf_test/patterns.csv hdp_agotadoln@10.2.113.138:/data/LZ/Agotados/Datos/patterns.csv')




# ================= OTRA ALTERNATIVA DE CONSTRUCCI�N DE PATRONES =======================
## Distribuciones Hora-Unidades
pattern <- data.frame(patron = 1,
                      Hora = 1,
                      unds = 1,
                      weight = 1)
pattern[-1, ]

for (i in 1:nc){
  auxdata <- dat[dat$concat == llaves_patrones[i], ]
  auxdata <- auxdata[, !(names(auxdata) %in% c("concat", "DependenciaCD", "Pluid", "dia"))]
  
  Hpi1 <- Hpi(x=auxdata);
  fhat.pi1 <- kde(x=auxdata, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  
  # Adimensionalización de la distribición [a, b] -> [0, 1]
  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  # 
  plot_ly(x = v, y = u, z= est_dist) %>% add_surface() 
  
  # Estimación de función de densidad con kernel gaussiano
  
  Hpi1 <- Hpi(x=auxdata);
  fhat.pi1 <- kde(x=auxdata, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  
  # Adimensionalización de la distribición [a, b] -> [0, 1]
  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  # 
  plot_ly(x = v, y = u, z= est_dist) %>% add_surface() 
  
  xy <- expand.grid(u, v)
  est_dist <- as.vector(est_dist)
  
  aux <- data.frame(patron = i,
                        Hora = xy[, 1],
                        unds = xy[, 2],
                        weight = est_dist)
  
  pattern <- cbind(aux, pattern)
  # convertir la matriz en un vector
}

# =================== VALIDACI?N DE PATRONES (verificar si los miembros de los grupos si son similares a sus centroides) ==============



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