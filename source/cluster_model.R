
cluster_model <- function(chars, k_n, nth, memo){
  conn <- h2o.init(ip = "localhost", port=54321, nthreads = nth, max_mem_size = memo)
  h2o.removeAll() # Clean slate - just in case the cluster was already running
  
  chars.hex <- as.h2o(chars);          # Conversión a formato de datos de h2o
  chars.hex[,1] <- as.factor(chars.hex[,1]) # Plus
  chars.hex[,2] <- as.factor(chars.hex[,2]) # mes
  chars.hex[,3] <- as.factor(chars.hex[,3]) # dia
  
  splits <- h2o.splitFrame(chars.hex,           ##  splitting the H2O frame we read above
                           c(0.6, 0.2),         ## create splits of 60% and 20%; 
                                                ##  H2O will create one more split of 1-(sum of these parameters)
                                                ##  so we will get 0.6 / 0.2 / 1 - (0.6+0.2) = 0.6/0.2/0.2
                           seed=1234);
  
  train <- h2o.assign(splits[[1]], "train.hex")   ## assign the first result the R variable train and the H2O name train.hex
  valid <- h2o.assign(splits[[2]], "valid.hex")   ## R valid, H2O valid.hex
  test <- h2o.assign(splits[[3]], "test.hex")     ## R test, H2O test.hex
  
  y <- c("m1", "m2", "s", "sd", "moda", "q1", "q2", "q3", "asim_fisher", "curtosis", "asim_pearson1", "asim_pearson2", "asim_bowley");
  x <- setdiff(names(train), y);  #vector of predictor column names
  x <- x[1:3];                    # Variables independientes: dep / plu / dia
  
  # k-means
  cluster_model = h2o.kmeans(training_frame = train, k = k_n, estimate_k = TRUE, x =  1:13)
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
  for (j in 1:nc){
    d <- Inf;
    for (i in 1:nrow(chars.hex)){
      x <- as.numeric(centers[j, 2:15]);
      y <- as.vector(as.numeric(chars.hex[i, 4:17]));
      y[is.na(y)] <- 0;
      d_aux <- sqrt(sum((x - y)^2));
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

