
cluster_model <- function(xs, k_n, nth, memo, dep){
  
  k_n <- 100
  nth <- 10
  memo <- '4g'
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
  cluster_model = h2o.kmeans(training_frame = train, k = k_n, x =  y)
  centers <- as.matrix(cluster_model@model$centers);
  nc = nrow(centers)
  distance <- matrix(nrow = nc, ncol = nc)
  
  # Cluster distances
  
  for (i in  1:nc){
    for (j in i:nc){
      aux <- (as.numeric(centers[i, ]) -  as.numeric(centers[j, ]))^2;
      distance[i, j] <- sqrt(sum(aux));
    }
  }
  
  chosen <- matrix(nrow = nc, ncol = 1)
  
  # Find closest points to centroids
  N <- ncol(centers)
  for (j in 1:nc){
    d <- Inf;
    for (i in 1:nrow(xs.hex)){
      x <- as.numeric(centers[j, 2:N]);
      yy <- as.vector(as.numeric(xs.hex[i, y]));
      yy[is.na(yy)] <- 0;
      d_aux <- sqrt(sum((x - yy)^2));
      if (d_aux < d){
        d <- d_aux;
        chosen[j, 1] <- i
      }
    }
  }
  
  # Plot of scoring history
  
  plot(cluster_model@model$scoring_history$iterations, cluster_model@model$scoring_history$number_of_reassigned_observations)
  
  
  return(cluster_model)
}

