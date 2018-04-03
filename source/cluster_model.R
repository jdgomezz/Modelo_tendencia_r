
cluster_model <- function(xs, k_n, estimate_k, nth, memo, dep, dia, y, yo){
  t_start <- Sys.time()
  
  xs.dat <- rxImport(xs);          # Conversión a formato de datos de h2o
  xs.dat <- xs.dat[xs.dat$DependenciaCD == dep, ]
  xs.dat <- xs.dat[xs.dat$dia == dia, ]
  
  if (length(yo) == 2){
    I <- which(names(xs.dat) == yo[1])
    J <- which(names(xs.dat) == yo[2]) 
  } else{
    I <- which(names(xs.dat) == yo[1])
    J <- which(names(xs.dat) == yo[2]) 
    K <- which(names(xs.dat) == yo[3]) 
  }
  
  xs.hex <- as.h2o(xs.dat);          # Conversión a formato de datos de h2o
  
  xs.hex <- xs.hex[xs.hex[, "DependenciaCD"] == dep && xs.hex[, "dia"] == dia, ]
  
  xs.hex[,"DependenciaCD"] <- as.factor(xs.hex[,"DependenciaCD"]) # Dependencia
  xs.hex[,"Pluid"] <- as.factor(xs.hex[,"Pluid"]) # Plu
  xs.hex[,"dia"] <- as.factor(xs.hex[,"dia"]) # dia
  
  splits <- h2o.splitFrame(xs.hex,           ##  splitting the H2O frame we read above
                           c(0.6, 0.2),         ## create splits of 60% and 20%; 
                                                ##  H2O will create one more split of 1-(sum of these parameters)
                                                ##  so we will get 0.6 / 0.2 / 1 - (0.6+0.2) = 0.6/0.2/0.2
                           seed=1234);
  
  train <- h2o.assign(splits[[1]], "train.hex")   ## assign the first result the R variable train and the H2O name train.hex
  valid <- h2o.assign(splits[[2]], "valid.hex")   ## R valid, H2O valid.hex
  test <- h2o.assign(splits[[3]], "test.hex")     ## R test, H2O test.hex
  
  x <- setdiff(names(train), y);  #vector of predictor column names
  x <- x[1:3];                    # Variables independientes: dep / plu / dia
  
  # k-means
  if (estimate_k == FALSE){
      cluster_model = h2o.kmeans(training_frame = train, validation_frame = valid, nfolds = 5, 
                                 fold_assignment = "Random", keep_cross_validation_predictions = TRUE,
                                 init="Furthest", keep_cross_validation_fold_assignment = TRUE,
                                  k = k_n, standardize = TRUE, max_iterations = 200, x = y);
      #cluster_model = h2o.kmeans(training_frame = train, 
      #                           validation_frame = valid,
      #                           k = k_n,
      #                           standardize = TRUE,
      #                           x = y);    
  } else{
      cluster_model = h2o.kmeans(training_frame = train, validation_frame = valid, nfolds = 5, 
                                 fold_assignment = "Random", keep_cross_validation_predictions = TRUE,
                                 init="Furthest", keep_cross_validation_fold_assignment = TRUE,
                                 estimate_k = TRUE, standardize = TRUE, max_iterations = 200, x = y);
      #cluster_model = h2o.kmeans(training_frame = train, 
      #                           validation_frame = valid,
      #                           estimate_k = TRUE,
      #                           standardize = TRUE,
      #                           x = y);
      
  }
  
  centros <-as.matrix(cluster_model@model$centers)
  storage.mode(centros) <- "numeric"
  centros <- data.frame(centros)
  centros[, -1]
  
  centros.hex <- as.h2o(centros)
  nc = nrow(centros.hex)
  
  distancia <- matrix(nrow = nc, ncol= nc)
  
  # Cluster distances
  for (i in  1:nc){
    for (j in i:nc){
      aux <- (as.numeric(centros[i, ]) -  as.numeric(centros[j, ]))^2
      distancia[i, j] <- sqrt(sum(aux))
    }
  }
  
  chosen = matrix(nrow = nc, ncol = 1)
  
  # Encontrar los puntos mas cercanos a los centroides
  pos <- matrix(0,nrow = nc,ncol = 2)
  
  for (i in 1:nc){
    dist <- (xs.hex[, y] - centros.hex[i, y])^2
    x <- sqrt(apply(dist, 1, sum))
    yy <- h2o.which_min(x)
    pos[i,1] <- yy[1,1]
    pos[i,2] <- i-1
  }
  
  # Omitir centros repetidos
  pos<- pos[order(pos[, 1], decreasing = FALSE), ]
  aux <- pos[duplicated(pos[, 1]),,drop=F]
  pos <- pos[!duplicated(pos[, 1]),,drop=F]
  
  patrones <- xs.hex[pos[, 1], ];
  patrones <- as.matrix(patrones)   # Extracci?n de patrones de la entidad model
  storage.mode(patrones) <- "numeric"
  patrones <- data.frame(cluster_id = pos[ ,2], Pluid = patrones[, 2], DependenciaCD = patrones[, 1], dia = patrones[, 3])
  
  if (nrow(aux) != 0){
    lista <- list()
    for (i in 1:nrow(aux)){
      lista <-rbind(lista, as.matrix(xs.hex[aux[i, 1], ]))
    }
    storage.mode(lista) <- "numeric"
    lista_ <- data.frame(cluster_id = aux[, 2], Pluid = lista[, 2], DependenciaCD =lista[, 1], dia = lista[, 3])
    patrones <- rbind(patrones, lista_)
  }
  patrones$concat <- paste0(patrones$Pluid,"-", patrones$DependenciaCD, "-", patrones$dia)
  
  t_end <- Sys.time()
  
  dt <- abs(t_start - t_end)
  print(dt)
  
  # ======== GRAFICACI?N DE LOS CLUSTERS ==================
  
  # Localizaci?n de los centroides
  
  if (length(yo) == 2){
    II <- which(names(centros) == yo[1])
    JJ <- which(names(centros) == yo[2]) 
  } else{
    II <- which(names(centros) == yo[1])
    JJ <- which(names(centros) == yo[2]) 
    KK <- which(names(centros) == yo[3]) 
  }
  # Asignaci?n de grupis con todo el conjunto de datos (entrenamiento, prueba, validaci?n)
  
  chars.fit = as.matrix(h2o.predict(object = cluster_model,  newdata = xs.hex));
  
  llaves <- as.data.frame(xs.hex[, c("Pluid", "DependenciaCD", "dia")])
  llaves <- data.frame(Pluid = llaves$Pluid,
                       DependenciaCD = llaves$DependenciaCD, 
                       dia = llaves$dia,
                       cluster_id = chars.fit[, 1])
  
  if (length(yo) == 2){
     p <- plot_ly() %>%
          add_markers(x = xs.dat[, I],
                      y = xs.dat[, J],
                      color = chars.fit[, 1], 
                      colors = "Dark2",
                      name = "Puntos") %>%
          add_markers(x = centros[, II],
                      y =  centros[, JJ], 
                      marker = list(size = 10,
                                    color = 'rgba(255, 182, 193, .9)',
                                    line = list(color = 'rgba(152, 0, 0, .8)',
                                                width = 2)),
                      name = "Centroide")  %>% 
          layout(scene = list(xaxis = list(title = yo[1]),
                              yaxis = list(title = yo[2])))
        
  } else {
     p <- plot_ly() %>%
       add_markers(x = xs.dat[, I],
                   y = xs.dat[, J],
                   z = xs.dat[, K],
                   color = chars.fit[, 1],
                   colors = "Dark2",
                   name = "Puntos") %>%
       add_markers(x = centros[, II],
                   y =  centros[, JJ],
                   z =  centros[, KK],
                   marker = list(size = 10,
                                 color = 'rgba(255, 182, 193, .9)',
                                 line = list(color = 'rgba(152, 0, 0, .8)',
                                             width = 2)),
                   name = "Centroide") %>% 
       layout(scene = list(xaxis = list(title = yo[1]),
                           yaxis = list(title = yo[2]),
                           zaxis = list(title = yo[3])))
  }
  output <- list(cluster_model, patrones, p, llaves)
  
  rm(list= setdiff(ls(), c("cluster_model", "patrones", "p", "llaves", "output")))
  return(output)
}

# Función para construir la el patron a través de la función density.

construir_patrones <- function(datos = auxdata, from = 8, to = 21){
  den <- density(x = datos, kernel = "gaussian", from = 8, to = 21);
  den$y <- den$y/sum(den$y)
  den$x <- floor(den$x)
  data <- data.table(x = den$x, y = den$y)
  # Agrupar por horas enteras
  new <- data.frame(data %>% group_by(x) %>% summarise(y = sum(y)))
  new$cluster_id <- rep(cluster_id, nrow(new))
  
  return(new)
}

