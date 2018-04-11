function MainByDep(tienda, libs, pars, nth, k_n, memo, y, yo){
  inSource <- libs[[1]]
  landing <- libs[[2]]
  model_lib <- libs[[3]]
  output_lib <- libs[[4]]
  
  fi <- pars[[1]]  
  ff <- pars[[2]]
  cut_registros <- pars[[3]]
  cut_ultima_venta <-pars[[4]] 
  cut_tiempo_vida <- pars[[5]] 
  cut_proporcion <- pars[[6]] 
  n_deciles <- pars[[7]] 
  booleano <- pars[[8]] 
  pars[[9]] <- file
  
  pars <- c(tienda, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
  venta <- LoadXdf(file, paste0(file = inSource, filename ='ventas_',tienda,'.xdf'), booleano = booleano, pars = pars)

  rxDataStep(inData = venta, 
             outFile = venta,
             overwrite = TRUE,
             transforms = 
               list(Hora_n = as.numeric(hora), concat = paste0(Pluid, "-", DependenciaCD, "-", dia)) )
  
  # ================ INSTRUCCIONES DE CONSTRUCCION DE CARACTERISTICAS ======================
  chars_namefile1 <- paste0(landing, "chs1_", tienda, ".xdf")
  chars_namefile2 <-  paste0(landing, "chs2_", tienda,".xdf")
  chars_namefile <-  paste0(landing, "characteristics_", tienda, ".xdf")
  
  xs <- RxCharacteristics(z = venta, 
                          name = chars_namefile,
                          name1 = chars_namefile1,
                          name2 = chars_namefile2)
  
  # ================ INSTRUCCIONES PARA LA IDENTIFICACIÓN DE PATRONES ======================
  conn <- h2o.init(ip = "localhost",
                   port=54321, 
                   nthreads = nth, 
                   max_mem_size = memo)
  
    # Todas las caracter?sticas disponibles
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