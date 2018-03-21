
cluster_model <- function(xs, k_n, nth, memo, dep){
  conn <- h2o.init(ip = "localhost", port=54321, nthreads = nth, max_mem_size = memo)
  h2o.removeAll() # Clean slate - just in case the cluster was already running
  
  xs.hex <- as.h2o(xs);          # Conversión a formato de datos de h2o
  
  xs.hex <- xs.hex[xs.hex[, "DependenciaCD"] == dep, ]
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
  
  y <- c("m1", "m2", "m3", "sd", "moda", "q1", "q2", "q3", "sesgo", "curtosis", "asim_fisher", "asim_pearson0",  "asim_pearson", "asim_bowley", "m1h", "m2h", "m3h", "sdh",  "modah", "q1h", "q2h", "q3h", "sesgoh", "curtosish", "asim_fisherh",   "asim_pearson0h", "asim_pearsonh",  "asim_bowleyh")

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
  return(patrones)
}

  # Estimación de densidad de probabilidad con el kernel density estimation package 
  # para hallar las distribuciones de los patrones
  
  library(ks);
  
  deps <- h2o.unique(patrones[, 1]);
  plus <- h2o.unique(patrones[, 2]);
  dias <- h2o.unique(patrones[, 3]);
  
  dat <- data.frame(data$Pluid, data$dia, data$Hora, data$UnidadesVerdaderas);
  dat$concat <- paste(dat$data.Pluid, dat$data.dia, sep = "-");
  
  patrones <- as.data.frame(patrones);
  patrones$concat <- paste(patrones$Pluid, patrones$dia, sep = "-");
  
  dat <- dat[dat$concat %in% patrones$concat, ];
  
  dat2 <- data.frame(data$Pluid, data$dia, data$Hora, data$UnidadesVerdaderas);
  dat2$concat <- paste(dat2$data.Pluid, dat2$data.dia, sep = "-");
  
  #for (i in 1:nc){
  #  auxdata <- dat[dat$concat == patrones[i, "concat"], ]
  #  auxdata <- auxdata[, !(names(auxdata) %in% c("concat", "data.Pluid", "data.dia"))]
  #  print(nrow(auxdata));
  #  Hpi1 <- Hpi(x=auxdata);
  #  fhat.pi1 <- kde(x=auxdata, H=Hpi1);
  #  u <- fhat.pi1$eval.points[[1]];
  #  v <- fhat.pi1$eval.points[[2]];
  #  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  #  plot_ly(x = v, y = u, z= est_dist) %>% add_surface() 
  #}
  
  # Verificar si los clusters son los adecuados.
  
  cluster_model.fit = h2o.predict(object = cluster_model,  newdata = valid);
  cluster_model.fit = as.data.frame(cluster_model.fit);
  
  I = 45; # Cluster
  chars_km.fit2 = which((cluster_model.fit == I) == 1);
  
  nearest_center <- as.data.frame(xs[pos[I], 1:2])
  nearest_center$concat <- paste(nearest_center$Pluid, nearest_center$dia, sep = "-");
  patron <- dat2[dat2$concat == nearest_center$concat, 3:4];
  
  Hpi1 <- Hpi(x=patron);
  fhat.pi1 <- kde(x=patron, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  est_dist2 <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  p1 <- plot_ly(x = v, y = u, z= est_dist2, scene='scene1') %>% add_surface(showscale=FALSE)
  
  i <- 15;
  muestra <- as.data.frame(valid[chars_km.fit2[i], 1:2]);
  muestra$concat <- paste(muestra$Pluid, muestra$dia, sep = "-");
  dato <- dat2[dat2$concat == muestra$concat, 3:4];
  
  Hpi1 <- Hpi(x=dato);
  fhat.pi1 <- kde(x=dato, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  p2 <- plot_ly(x = v, y = u, z= est_dist, scene='scene2') %>% add_surface(showscale=FALSE) 
  
  i <- 6;
  muestra <- as.data.frame(valid[chars_km.fit2[i], 1:2]);
  muestra$concat <- paste(muestra$Pluid, muestra$dia, sep = "-");
  dato <- dat2[dat2$concat == muestra$concat, 3:4];
  
  Hpi1 <- Hpi(x=dato);
  fhat.pi1 <- kde(x=dato, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  p3 <- plot_ly(x = v, y = u, z= est_dist, scene='scene3') %>% add_surface(showscale=FALSE) 
  
  i <- 2;
  muestra <- as.data.frame(valid[chars_km.fit2[i], 1:2]);
  muestra$concat <- paste(muestra$Pluid, muestra$dia, sep = "-");
  dato <- dat2[dat2$concat == muestra$concat, 3:4];
  
  Hpi1 <- Hpi(x=dato);
  fhat.pi1 <- kde(x=dato, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  p4 <- plot_ly(x = v, y = u, z= est_dist, scene='scene4') %>% add_surface(showscale=FALSE) 
  
  subplot(p1, p2, p3, p4)  %>%
    layout(title = "3D Subplots",
           scene1 = list(domain=list(x=c(0,0.5),y=c(0.5,1)),
                         aspectmode='cube'),
           scene2 = list(domain=list(x=c(0.5,1),y=c(0.5,1)),
                         aspectmode='cube'),
           scene3 = list(domain=list(x=c(0,0.5),y=c(0,0.5)),
                         aspectmode='cube'),
           scene4 = list(domain=list(x=c(0.5,1),y=c(0,0.5)),
                         aspectmode='cube'))
  
  return(cluster_model)

