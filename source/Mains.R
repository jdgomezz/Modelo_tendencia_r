MainByDep <- function (tienda, Index, query, libs, pars, nth, k_n, memo, y, yo, fileConn, loadbool){
  inSource <- libs[[1]]
  landing <- libs[[2]]
  model_lib <- libs[[3]]
  output_lib <- libs[[4]]
  log_lib <- libs[[5]]
  
  fi <- pars[[1]]  
  ff <- pars[[2]]
  cut_registros <- pars[[3]]
  cut_ultima_venta <-pars[[4]] 
  cut_tiempo_vida <- pars[[5]] 
  cut_proporcion <- pars[[6]] 
  n_deciles <- pars[[7]] 
  booleano <- pars[[8]] 

  parametros <- c(tienda, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
  
  fileConn<-file(paste0(log_lib, "logs_", tienda, ".txt"), "w")
  
   # Log para confirmar descarga de histÃ³rico de ventas hora
  write(paste0("Inicia descarga de histÃ³ricos de venta de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)

  outfile_ventas <- paste0(inSource, 'ventas_', tienda, '.xdf')
   if (loadbool == TRUE){
      venta <- LoadXdf(query, outfile_ventas, booleano = booleano, pars = parametros)
      rxDataStep(inData = venta, 
                 outFile = venta,
                 overwrite = TRUE,
                 transforms = list(Hora_n = as.numeric(hora), 
                 concat = paste0(Pluid, "-", DependenciaCD, "-", dia)))
   }
  # Log para confirmar descarga de histÃ³rico de ventas hora
  write(paste0("Termina de descargar histÃ³ricos de venta de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)

  # ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================
  chars_namefile1 <- paste0(landing, "chs1_", tienda, ".xdf")
  chars_namefile2 <-  paste0(landing, "chs2_", tienda,".xdf")
  chars_namefile <-  paste0(landing, "characteristics_", tienda, ".xdf")
  
  write(paste0("Inicia computaciÃ³n de caracterÃ?sticas de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)

  xs <- RxCharacteristics(z = outfile_ventas, 
                          name = chars_namefile,
                          name1 = chars_namefile1,
                          name2 = chars_namefile2)
  
  # Log para confirmar computaciÃ³n de caracterÃ?sticas 
  write(paste0("Termina de computar caracterÃ?sticas de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
  
  # ================ INSTRUCCIONES PARA LA IDENTIFICACIÃ“N DE PATRONES ======================
  xs <- paste0("xdf/characteristics_", tienda, ".xdf")
  write(paste0("Inicia computar modelo de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)

  for (Master_I in 1:7){
     
     write(paste0("Inicia computar modelo de tienda ", tienda, " para el dÃ?a ", Master_I), file = fileConn, sep = "\n", append = TRUE)
     port <- 54321 + 3*Index
     conn <- h2o.init(ip = "localhost",
                      port  =port, 
                      nthreads = 1, 
                      max_mem_size = memo)
     
     model <- cluster_model(xs = xs,
                            k_n = k_n, 
                             y = y,
                             yo = yo,
                             dep = tienda,
                             dia = Master_I,
                             estimate_k = FALSE)
      
      # Log para confirmar computaciÃ³n de modelo de clasificaciÃ³n para el dÃ?a
      write(paste0("Termina de computar modelo de tienda ", tienda, " para el dÃ?a ", Master_I), file = fileConn, sep = "\n", append = TRUE)

     # ===== CONSTRUCCIÃ“N DE PATRONES A TRAVÃ‰S DE LA ESTIMACIÃ“N DE FUNCIONES DE DENSIDAD CON KERNEL GAUSSIANO ===================================
      
      patterns <- rxImport(inData = model[[2]], outFile = paste0(model_lib, "patrones", tienda, ".xdf"), overwrite = TRUE)
      
      venta_f <- rxMerge(inData1 = outfile_ventas,
                         inData2 = patterns,
                         outFile = paste0(model_lib, "venta_", tienda, "f.xdf"),
                         matchVars = c("concat"), 
                         type = "inner",
                         overwrite = TRUE)
      
      datos <- rxImport(venta_f)
      dat <- data.frame(cluster_id = datos$cluster_id,
                        concat = datos$concat,
                        Pluid = datos$Pluid.ventas,
                        DependenciaCD = datos$DependenciaCD.ventas, 
                        dia = datos$dia.ventas, 
                        Hora = datos$Hora_n,
                        UnidadesVendidas = datos$UnidadesVendidas)

      llaves_patrones <- unique(dat$cluster_id)
      nc <- length(llaves_patrones)
      
      # Distribuciones Hora
      patron <- data.frame(x = 1,
                           y = 1,
                           cluster_id = 1)

      for (i in 1:nc){
        auxdata <- dat[dat$cluster_id == llaves_patrones[i], ]
        cluster_id <- unique(auxdata$cluster_id)
        auxdata <- auxdata[, !(names(auxdata) %in% c("cluster_id", "UnidadesVendidas", "concat", "DependenciaCD", "Pluid", "dia"))]
        new <- construir_patrones(datos = auxdata, from = 7, to = 21, cluster_id = cluster_id)
        rm(auxdata)
        patron <- rbind(patron, new)
      }
      
      # Log para confirmar el cÃ³mputo de los patrones para la tienda.

      write(paste0("Termina de computar patrones de tienda ", tienda, " para el dÃ?a ", Master_I), file = fileConn, sep = "\n", append = TRUE)
      
      patron <- patron[-1, ]
      
      patron.xdf <- rxImport(inData = patron, outFile = paste0(output_lib, "patrones_", tienda, ".xdf"), overwrite = TRUE)
      llaves.xdf <- rxImport(inData = model[[4]], outFile = paste0(output_lib, "geoprodtime_", tienda, ".xdf"), overwrite = TRUE)
      
      patrones_expandidos.xdf <- rxMerge(inData1 = llaves.xdf,
                                         inData2 = patron.xdf,
                                         outFile = paste0(output_lib, "patrones_geoprodtime_", Master_I, "_", tienda, ".xdf"),
                                         matchVars = c("cluster_id"),
                                         varsToKeep1 =c("Pluid", "DependenciaCD", "dia", "cluster_id"),
                                         varsToKeep2 =c("cluster_id", "x", "y"),
                                         type = "inner",
                                         overwrite = TRUE)
      
      # Log para confirmar el cÃ³mputo de los patrones expandidos a los plus para la tienda.
      
      write(paste0("Termina de computar patrones expandidos de tienda ", tienda, " para el dÃ?a ", Master_I), file = fileConn, sep = "\n", append = TRUE)
    }
    # Consolidaci?n de los patrones en un solo .xdf
    I <- 1
    patrones_all <- rxImport(paste0(output_lib,'patrones_geoprodtime_', I, '_', tienda, '.xdf'), overwrite = TRUE)
    
    for (I in 2:7){
      aux <- rxImport(paste0(output_lib,'patrones_geoprodtime_', I, '_', tienda, '.xdf'), overwrite = TRUE)
      patrones_all <- rbind(patrones_all, aux)
    }
    
    clusters_geoprod <- unique(data.frame(dia = as.numeric(patrones_all$dia), x = patrones_all$Pluid, y = patrones_all$cluster_id))
    clusters_geoprod <- data.frame(clusters_geoprod %>% group_by(dia, y) %>% summarise(n_elems = n()))
    
    patron <- rxImport(inData = patrones_all, outFile = paste0(output_lib, "patrones_all_", tienda , ".xdf"), overwrite = TRUE)
    
    # Log para confirmar el cÃ³mputo de los patrones para todos los dÃ?as de la tienda.
    
    write(paste0("Termina de escribir patrones definitivos de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
    close(fileConn)
    
    return(patron)
}


CharsByDep <- function (tienda, query, libs, pars, fileConn, loadbool){
  inSource <- libs[[1]]
  landing <- libs[[2]]
  model_lib <- libs[[3]]
  output_lib <- libs[[4]]
  log_lib <- libs[[5]]
  
  fi <- pars[[1]]  
  ff <- pars[[2]]
  cut_registros <- pars[[3]]
  cut_ultima_venta <-pars[[4]] 
  cut_tiempo_vida <- pars[[5]] 
  cut_proporcion <- pars[[6]] 
  n_deciles <- pars[[7]] 
  booleano <- pars[[8]] 
  
  parametros <- c(tienda, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
  
  fileConn<-file(paste0(log_lib, "logs_", tienda, ".txt"), "w")
  
  # Log para confirmar descarga de histÃ³rico de ventas hora
  write(paste0("Inicia descarga de histÃ³ricos de venta de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
  
  outfile_ventas <- paste0(inSource, 'ventas_', tienda, '.xdf')
  if (loadbool == TRUE){
    venta <- LoadXdf(query, outfile_ventas, booleano = booleano, pars = parametros)
    rxDataStep(inData = venta, 
               outFile = venta,
               overwrite = TRUE,
               transforms = list(Hora_n = as.numeric(hora), 
                                 concat = paste0(Pluid, "-", DependenciaCD, "-", dia)))
  }
  # Log para confirmar descarga de histÃ³rico de ventas hora
  write(paste0("Termina de descargar histÃ³ricos de venta de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
  
  # ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================
  chars_namefile <-  paste0(landing, "CentralChars_", tienda, ".xdf")
  
  write(paste0("Inicia computaciÃ³n de características de tendencia central para ", tienda), file = fileConn, sep = "\n", append = TRUE)
  
  xs <- RxCharacteristicsPattern(z = outfile_ventas,  name = chars_namefile)
  # Log para confirmar computaciÃ³n de caracterÃ?sticas 
  write(paste0("Termina computaciÃ³n de características de tendencia central para ", tienda), file = fileConn, sep = "\n", append = TRUE)

  close(fileConn)
  
  return(xs)
}
