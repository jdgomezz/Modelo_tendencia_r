
cluster_model <- function(xs, k_n, nth, memo, dep, y){
  t_start <- Sys.time()
  
  xs.hex <- as.h2o(rxImport(xs));          # Conversión a formato de datos de h2o
  
  #xs.hex <- xs.hex[xs.hex[, "DependenciaCD"] == dep, ]
  xs.hex[,1] <- as.factor(xs.hex[,1]) # Dependencia
  xs.hex[,2] <- as.factor(xs.hex[,2]) # Plu
  xs.hex[,3] <- as.factor(xs.hex[,3]) # dia
  
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
  
  cluster_model = h2o.kmeans(training_frame = train, validation_frame = valid, nfolds = 5, 
                           fold_assignment = "Random", keep_cross_validation_predictions = TRUE,
                           init="Furthest", keep_cross_validation_fold_assignment = TRUE,
                           k = k_n, standardize = TRUE, max_iterations = 120, y);
  
  centros <- as.matrix(cluster_model@model$centers);
  storage.mode(centros) <- "numeric"
  
  centros.hex <- as.h2o(centros);
  nc = nrow(centros.hex);
  
  distancia <- matrix(nrow = nc, ncol= nc);
  
  # Cluster distances
  for (i in  1:nc){
    for (j in i:nc){
      aux <- (as.numeric(centros[i, ]) -  as.numeric(centros[j, ]))^2;
      distancia[i, j] <- sqrt(sum(aux));
    }
  }
  
  chosen = matrix(nrow = nc, ncol = 1);
  
  # Encontrar los puntos mas cercanos a los centroides
  pos <- array(0,dim =nc);
  
  for (i in 1:nc){
    dist <- (xs.hex[, y] - centros.hex[i, y])^2
    x <- sqrt(apply(dist, 1, sum));
    yy <- h2o.which_min(x);
    pos[i] <- yy[1,1];
  }
  pos_aux <- sort(unique(pos));
  patrones <- xs.hex[pos_aux, ];
  nc <- length(pos_aux);
  
  output <- list(cluster_model, patrones)
  
  t_end <- Sys.time()
  
  dt <- abs(t_start - t_end)
  print(dt)
  return(output)
}
