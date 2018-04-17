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
  
   # Log para confirmar descarga de histórico de ventas hora
  write(paste0("Inicia descarga de históricos de venta de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)

  outfile_ventas <- paste0(inSource, 'ventas_', tienda, '.xdf')
   if (loadbool == TRUE){
      venta <- LoadXdf(query, outfile_ventas, booleano = booleano, pars = parametros)
      rxDataStep(inData = venta, 
                 outFile = venta,
                 overwrite = TRUE,
                 transforms = list(Hora_n = as.numeric(hora), 
                 concat = paste0(Pluid, "-", DependenciaCD, "-", dia)))
   }
  # Log para confirmar descarga de histórico de ventas hora
  write(paste0("Termina de descargar históricos de venta de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)

  # ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================
  chars_namefile1 <- paste0(landing, "chs1_", tienda, ".xdf")
  chars_namefile2 <-  paste0(landing, "chs2_", tienda,".xdf")
  chars_namefile <-  paste0(landing, "characteristics_", tienda, ".xdf")
  
  write(paste0("Inicia computación de caracter??sticas de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)

  xs <- RxCharacteristics(z = outfile_ventas, 
                          name = chars_namefile,
                          name1 = chars_namefile1,
                          name2 = chars_namefile2)
  
  # Log para confirmar computación de caracter??sticas 
  write(paste0("Termina de computar caracter??sticas de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
  
  # ================ INSTRUCCIONES PARA LA IDENTIFICACIÓN DE PATRONES ======================
  xs <- paste0("xdf/characteristics_", tienda, ".xdf")
  write(paste0("Inicia computar modelo de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
  plot_list <- list()
  colors <- c('#e6194b', '#3cb44b', '#ffe119', '#0082c8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#008080', '#800000', '#000080', '#000000', '#aaffc3', '#e6beff')
  
  for (Master_I in 1:7){
     
     write(paste0("Inicia computar modelo de tienda ", tienda, " para el d??a ", Master_I), file = fileConn, sep = "\n", append = TRUE)
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
      
      # Log para confirmar computación de modelo de clasificación para el d??a
      write(paste0("Termina de computar modelo de tienda ", tienda, " para el d??a ", Master_I), file = fileConn, sep = "\n", append = TRUE)

     # ===== CONSTRUCCIÓN DE PATRONES A TRAVÉS DE LA ESTIMACIÓN DE FUNCIONES DE DENSIDAD CON KERNEL GAUSSIANO ===================================
      
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
      
      # Log para confirmar el cómputo de los patrones para la tienda.

      write(paste0("Termina de computar patrones de tienda ", tienda, " para el d??a ", Master_I), file = fileConn, sep = "\n", append = TRUE)
      
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
      
      # =================== GRAFICACI?N DE LOS PATRONES ======================================
      p  <- plot_ly()
      for (II in 0:5){
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
             title = paste0("Patrones del dia ", Master_I), showlegend = FALSE) 
      
      plot_list <- rbind(plot_list, p)
      
      # Log para confirmar el cómputo de los patrones expandidos a los plus para la tienda.
      
      write(paste0("Termina de computar patrones expandidos de tienda ", tienda, " para el d??a ", Master_I), file = fileConn, sep = "\n", append = TRUE)
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
    
    # ================= GRAFICACIÓN DE LOS PATRONES ================================
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
    
    patron <- rxImport(inData = patrones_all, outFile = paste0(output_lib, "patrones_all_", tienda , ".xdf"), overwrite = TRUE)
    
    # Log para confirmar el cómputo de los patrones para todos los d??as de la tienda.
    
    write(paste0("Termina de escribir patrones definitivos de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
    close(fileConn)
    
    outi <- list(patron, o, p, q, r)
    return(outi)
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
  
  # Log para confirmar descarga de histórico de ventas hora
  write(paste0("Inicia descarga de históricos de venta de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
  
  outfile_ventas <- paste0(inSource, 'ventas_', tienda, '.xdf')
  if (loadbool == TRUE){
    venta <- LoadXdf(query, outfile_ventas, booleano = booleano, pars = parametros)
    rxDataStep(inData = venta, 
               outFile = venta,
               overwrite = TRUE,
               transforms = list(Hora_n = as.numeric(hora), 
                                 concat = paste0(Pluid, "-", DependenciaCD, "-", dia)))
  }
  # Log para confirmar descarga de histórico de ventas hora
  write(paste0("Termina de descargar históricos de venta de tienda ", tienda), file = fileConn, sep = "\n", append = TRUE)
  
  # ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================
  chars_namefile <-  paste0(landing, "CentralChars_", tienda, ".xdf")
  
  write(paste0("Inicia computación de caracter?sticas de tendencia central para ", tienda), file = fileConn, sep = "\n", append = TRUE)
  
  xs <- RxCharacteristicsPattern(z = outfile_ventas,  name = chars_namefile)
  # Log para confirmar computación de caracter??sticas 
  write(paste0("Termina computación de caracter?sticas de tendencia central para ", tienda), file = fileConn, sep = "\n", append = TRUE)

  close(fileConn)
  
  return(xs)
}
