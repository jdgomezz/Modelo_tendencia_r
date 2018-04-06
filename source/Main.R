rm(list = ls())
# Onlinux
setwd("~/")

# On Windows
#setwd("C:/Users/User/Desktop")

wd <- getwd()
root <- paste0(wd, '/Agotado_en_gondola')
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
ff <- "'2018-03-31'"                         # Fecha final
cut_registros <- 0                          # Número m??nimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de última venta realizada
cut_tiempo_vida <- 30                         # Tiempo de vida m??nimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
cut_proporcion <- 1                          # Proporción de venta m??nima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra

booleano <- TRUE
file <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza_v2.txt'
outfile_ventas <- paste0(inSource, 'ventas.xdf')

querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL and DependenciaCD in (41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569)"
#querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL and DependenciaCD in (35, 33)"
connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
dep <- rxImport(odbcDS)$DependenciaCD
npart <- 20
delta <- length(dep)%/%npart
nodes <- 8

cl <-makeCluster(nodes)
registerDoParallel(cl)
system.time( 
  result <- foreach (i = 0:(npart+1), .combine='cbind', .export = c('LoadXdf')) %dopar% {
    deptemp <- dep[(1+i*delta):(1+(i+1)*delta)]
    deptemp <- deptemp[-(delta+1)]
    deptemp <- deptemp[!is.na(deptemp)]
    deptemp <- paste(deptemp, collapse = ', ')
    pars <- c(deptemp, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
    LoadXdf(file, paste0(file = inSource, filename ='ventas_',i,'.xdf'), booleano = booleano, pars = pars)
    print(paste0('Finalizado iteracion ',i))
    rm(deptemp, pars)
  })
   stopCluster(cl)
   rm(cl)

   i <- 0
   venta <- rxImport(inData = paste0(inSource,'ventas_',i,'.xdf'), 
            outFile = outfile_ventas, overwrite = TRUE)
   acum =  nrow(rxImport(paste0(inSource, 'ventas_',i,'.xdf')))
   #file.remove(paste0(inSource, 'ventas_',i,'.xdf'))
   
   for (i in 1:(npart-1)){
       venta <- rxImport(inData = paste0(inSource, 'ventas_',i,'.xdf'), 
                         outFile= outfile_ventas,
                         append = "rows", 
                         overwrite = TRUE)
      acum = acum + nrow(rxImport(paste0(inSource, 'ventas_',i,'.xdf')))
      print(i)
     # file.remove(paste0(inSource, 'ventas_',i,'.xdf'))
   }
   
rxDataStep(inData = venta, 
              outFile = venta,
              overwrite = TRUE,
              transforms = 
              list(Hora_n = as.numeric(hora), concat = paste0(Pluid, "-", DependenciaCD, "-", dia)) )

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
memo <- '8g' # Memoria RAM a utilizar
k_n <- 50     # Número de clusters deseados

conn <- h2o.init(ip = "localhost",
                 port=54321, 
                 nthreads = nth, 
                 max_mem_size = memo)

for (DEPS in 1:length(dep)){
  # Todas las caracter?sticas disponibles
  y <- c("m1", "m2", "m3", "sd", "n", "moda", "q1", "q2", "q3", "sesgo", "curtosis", "asim_fisher", "asim_pearson0", "asim_pearson", "asim_bowley", "m1h", "m2h", "m3h", "sdh", "nh", "modah", "q1h", "q2h", "q3h", "sesgoh", "curtosish", "asim_fisherh", "asim_pearson0h", "asim_pearsonh", "asim_bowleyh")
  yo <- c("sesgoh", "asim_bowleyh", "q2h")    # Caracteristicas a visualizar
  y <- c("asim_pearsonh", "curtosish", "q1h",  "q2h", "q3h", "sesgoh", "modah", "m1h", "asim_bowleyh")
  plot_list <- list()
  colors <- c('#e6194b', '#3cb44b', '#ffe119', '#0082c8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#008080', '#800000', '#000080', '#000000', '#aaffc3', '#e6beff')
  for (Master_I in 1:7){
    h2o.removeAll() # Clean slate - just in case the cluster was already running
    model <- cluster_model(xs = xs,
                           k_n = k_n, 
                           nth = nth, 
                           memo = memo,
                           y = y,
                           yo = yo,
                           dep = dep[DEPS],
                           dia = Master_I,
                           estimate_k = FALSE)
    
    # model[[3]]
    
    # ===== CONSTRUCCIÓN DE PATRONES A TRAVÉS DE LA ESTIMACIÓN DE FUNCIONES DE DENSIDAD CON KERNEL GAUSSIANO ===================================
    
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
    
    llaves_patrones <- unique(dat$cluster_id)
    nc <- length(llaves_patrones)
    
    ## Distribuciones Hora
    patron <- data.frame(x = 1,
                         y = 1,
                         cluster_id = 1)
    for (i in 1:nc){
      auxdata <- dat[dat$cluster_id == llaves_patrones[i], ]
      cluster_id <- unique(auxdata$cluster_id)
      auxdata <- auxdata[, !(names(auxdata) %in% c("cluster_id", "UnidadesVendidas", "concat", "DependenciaCD", "Pluid", "dia"))]
      new <- construir_patrones(datos = auxdata, from = 7, to = 21)
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
    # =================== GRAFICACI?N DE LOS PATRONES ======================================
      p  <- plot_ly()
      for (II in 0:(k_n-1)){
        p <- add_lines(p,
                       x = c(6, patron[patron$cluster_id == II, 1]),
                       y = c(0, patron[patron$cluster_id == II, 2]),
                      mode = 'lines',
                      type = 'scatter',
                      line = list(shape = "spline"),
                      colors = colors[II + 1])
      }
      layout(p, xaxis = list(autotick = FALSE, ticks = "outside", tick0 = 6, dtick = 1, title = "Hora"),
                yaxis = list(autotick = FALSE, ticks = "outside", tick0 = 0, dtick = 0.01, title = "Perfil" ),
                title = paste0("Patrones del d?a ", Master_I), showlegend = FALSE) 
      
      plot_list <- rbind(plot_list, p)
  }
  
  # Consolidaci?n de los patrones en un solo .xdf
  I <- 1
  patrones_all <- rxImport(paste0(output_lib,'patrones_geoprodtime_',I,'.xdf'), overwrite = TRUE)
  
  for (I in 2:7){
    aux <- rxImport(paste0(output_lib,'patrones_geoprodtime_',I,'.xdf'), overwrite = TRUE)
    patrones_all <- rbind(patrones_all, aux)
  }
  
  clusters_geoprod <- unique(data.frame(dia = as.numeric(patrones_all$dia), x = patrones_all$Pluid, y = patrones_all$cluster_id))
  clusters_geoprod <- data.frame(clusters_geoprod %>% group_by(dia, y) %>% summarise(n_elems = n()))
  
  o <- plot_ly(clusters_geoprod, y = ~dia, x = ~y, color = ~y, size = ~n_elems, type = "scatter", colors = colors, 
          marker = list(symbol = 'circle', sizemode = 'diameter'), sizes = c(5, 80)) %>% hide_colorbar()
  
  p <- subplot(plotly_build(plot_list[[1]]), 
               plotly_build(plot_list[[2]]),
               plotly_build(plot_list[[3]]),
               plotly_build(plot_list[[4]]),
               nrows = 4, margin = 0.02, heights = rep(.25, 4), shareX = TRUE, shareY = TRUE)
  
  q <- subplot(
               plotly_build(plot_list[[5]]),
               plotly_build(plot_list[[6]]),
               plotly_build(plot_list[[7]]),
               nrows = 3, margin = 0.02, heights = c(1/3, 1/3, 1/3), shareY = TRUE)
  
  r <- subplot(o, p, q)
  rxImport(inData = patrones_all, outFile = paste0(output_lib, "patrones_all_", DEPS , ".xdf"), overwrite = TRUE)
}

# ==================== CONSOLIDACIÓN DEL ARCHIVO DE SALIDA ============================
DEPS <- 1
outfile_patrones <- paste0(output_lib, "patrones_all_deps.xdf")
agotado <- rxImport(inData = paste0(output_lib, "patrones_all_", DEPS , ".xdf"), 
                  outFile = outfile_patrones, overwrite = TRUE)

DEPS <- 1
patrones_all <- rxImport(paste0(output_lib, "patrones_all_", DEPS , ".xdf"))

for (DEPS in 2:15){
  agotado <- rxImport(paste0(output_lib, "patrones_all_", DEPS , ".xdf"))
  patrones_all <- rbind(patrones_all, agotado)                  
  print(DEPS)
}

for (DEPS in 2:15){
  agotado <- rxImport(inData = paste0(output_lib, "patrones_all_", DEPS , ".xdf"), 
                    outFile= outfile_patrones,
                    append = "rows", 
                    overwrite = TRUE)
  print(DEPS)
}

patrones_all <- rxImport(outfile_patrones, overwrite = TRUE)
# ==================== ESCRITURA DE ARCHIVO DE SALIDA DE PATRÓN =====================

# Organizaci?n de formatro de archivo de salida

dias <- c(Sys.Date()- as.POSIXlt(Sys.Date())$wday,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 1,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 2,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 3,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 4,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 5,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 6)# Primer d?a de la semana
fecha_dia = data.frame(dia = 1:7, diafecha = dias)

patrones_all <- merge(x = patrones_all, y = fecha_dia, by = "dia", all.x = TRUE)
patrones_all <- subset(patrones_all, select = c(7, 2, 3, 5, 6))
colnames(patrones_all) <- c("Fecha", "Pluid", "storeid", "Hora", "Perfil")

# Escritura de archivo final

#patrones_all$Fecha <- format(as.Date(format(as.Date(patrones_all$Fecha), "%Y-%m-%d")), "%Y-%m-%d")
patrones_all$Fecha <- format(patrones_all$Fecha, "%Y-%m-%d")

patrones_all$storeid <- as.numeric(as.character(patrones_all$storeid))
patrones_all$Pluid <- as.numeric(as.character(patrones_all$Pluid))
patrones_all$Hora <- as.numeric(as.character(patrones_all$Hora))
patrones_all$Fecha <- as.Date(patrones_all$Fecha) 
write.table(patrones_all, file = paste0(output_lib, "tendencia.csv"), sep=",", row.names = FALSE)

system('sshpass -p "hadoop" scp ~/xdf/tendencia.csv hdp_agotadoln@10.2.113.138:/data/LZ/Agotados/Datos/tendencia.csv')

# ================= Evaluaci?n de los patrones =========================================


# ================ ENVIAR E-MAIL DE FINALIZACIÓN DE PROCESO =============================

send.mail(from = "jgomezz101@gmail.com",                                                     # Desde
          to = "afbedoyag@grupo-exito.com",                                                  # Para
          subject = "Reporte eficiencia modelo de tendencia",                                # Asunto
          body = "Buena tarde",                                                              # Cuerpo
          # html = T,  # recibe HTML encoding para el cuerpo
          encoding = "utf-8",                                                                # Codificacion
          #attach.files = c(pathLog),                                                        # rutas de los archivos a adjuntar
          #file.names = c("Log Ejecucion"),                                                  # nombres de los archivos a adjuntar
          smtp = list(host.name = "smtp.gmail.com",                                          # Host del mail
                      port = 465,                                                            # Puerto del host
                      user.name = "jgomezz101@gmail.com",                                    # Usuario que envia
                      passwd = "n1nt3nd0!"),                                                 # Contrase?a del que envia
          authenticate = FALSE,                                                              # Autenticar
          send = TRUE,
          debug=TRUE)
