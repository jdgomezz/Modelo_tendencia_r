rm(list=ls())
library(reshape2)
library(dplyr)
library(moments)
library(plotly)
library(xlsx)
library(Emcdf)

# Función para computar la moda

Mode <- function(num) {
  unique_num <- unique(num)
  unique_num [which.max(tabulate(match(num, unique_num )))]
}

path <- 'D:/Documents/Desktop/ProyAgotadoEnGondola/Source/dep35conEstructura.csv';
path_hora <- 'D:/Documents/Desktop/ProyAgotadoEnGondola/Source/Hora.csv';

data <- read.table(path, header = TRUE, sep = ",");
hours <- read.table(path_hora, header = TRUE, sep = ",");
hours <- hours[, !(names(hours) %in% "key")];

data <- data[data$UnidadesVerdaderas <= quantile(data$UnidadesVerdaderas, 0.95), ];

x <- data.frame(count(data, Pluid, dia, Hora, UnidadesVerdaderas));
x$concat <- paste(x$Pluid, x$dia, sep = "-");

y <- data.frame(count(data, Pluid, dia));
y = y[order(y[, 3]), ];
y<- y[y[, 3] >= 30, ];
y$concat <- paste(y$Pluid, y$dia, sep = "-");
chosen_keys <- unique(y$concat);

x <- x[x$concat %in% chosen_keys, ];


chars <- data.frame(x %>% group_by(Pluid, dia) %>% summarise(ss = sum(n),
                                                             m1 = sum(UnidadesVerdaderas*n)/sum(n), 
                                                             m2 = sum(n*(UnidadesVerdaderas^2))/sum(n),
                                                             s =  sum(n*((UnidadesVerdaderas - sum(UnidadesVerdaderas*n)/sum(n))^2)),
                                                             sd = sqrt(sum(n*(UnidadesVerdaderas - sum(UnidadesVerdaderas*n)/sum(n))^2)/(sum(n)-1)),
                                                             moda = Mode(rep(UnidadesVerdaderas, n)),
                                                             q1 = quantile(rep(UnidadesVerdaderas, n), 0.25),
                                                             q2 = quantile(rep(UnidadesVerdaderas, n), 0.5),
                                                             q3 = quantile(rep(UnidadesVerdaderas, n), 0.75),
                                                             asim_fisher = (sum(n*(UnidadesVerdaderas - sum(UnidadesVerdaderas*n)/sum(n))^3)/sum(n))/((sum(n*(UnidadesVerdaderas - sum(UnidadesVerdaderas*n)/sum(n))^2)/sum(n))^(3/2)),
                                                             curtosis = (sum(n*(UnidadesVerdaderas - sum(UnidadesVerdaderas*n)/sum(n))^4)/sum(n))/((sum(n*(UnidadesVerdaderas - sum(UnidadesVerdaderas*n)/sum(n))^2)/sum(n))^2),
                                                             asim_pearson1 = ((sum(UnidadesVerdaderas*n)/sum(n)) - (Mode(rep(UnidadesVerdaderas, n))))/(sqrt(sum(n*(UnidadesVerdaderas - sum(UnidadesVerdaderas*n)/sum(n))^2)/(sum(n)-1))),
                                                             asim_pearson2 = 3*((sum(UnidadesVerdaderas*n)/sum(n)) - (quantile(rep(UnidadesVerdaderas, n), 0.5)))/(sqrt(sum(n*(UnidadesVerdaderas - sum(UnidadesVerdaderas*n)/sum(n))^2)/(sum(n)-1))),
                                                             asim_bowley = (quantile(rep(UnidadesVerdaderas, n), 0.75) + quantile(rep(UnidadesVerdaderas, n), 0.25) - 2*quantile(rep(UnidadesVerdaderas, n), 0.5))/(quantile(rep(UnidadesVerdaderas, n), 0.75) - quantile(rep(UnidadesVerdaderas, n), 0.25))
));

chars_x <- data.frame(x %>% group_by(Pluid, dia) %>% summarise(ss = sum(n),
                                                               m1 = sum(Hora*n)/sum(n), 
                                                               m2 = sum(n*(Hora^2))/sum(n),
                                                               s =  sum(n*((Hora - sum(Hora*n)/sum(n))^2)),
                                                               sd = sqrt(sum(n*(Hora - sum(Hora*n)/sum(n))^2)/(sum(n)-1)),
                                                               moda = Mode(rep(Hora, n)),
                                                               q1 = quantile(rep(Hora, n), 0.25),
                                                               q2 = quantile(rep(Hora, n), 0.5),
                                                               q3 = quantile(rep(Hora, n), 0.75),
                                                               asim_fisher = (sum(n*(Hora - sum(Hora*n)/sum(n))^3)/sum(n))/((sum(n*(Hora - sum(Hora*n)/sum(n))^2)/sum(n))^(3/2)),
                                                               curtosis = (sum(n*(Hora - sum(Hora*n)/sum(n))^4)/sum(n))/((sum(n*(Hora - sum(Hora*n)/sum(n))^2)/sum(n))^2),
                                                               asim_pearson1 = ((sum(Hora*n)/sum(n)) - (Mode(rep(Hora, n))))/(sqrt(sum(n*(Hora - sum(Hora*n)/sum(n))^2)/(sum(n)-1))),
                                                               asim_pearson2 = 3*((sum(Hora*n)/sum(n)) - (quantile(rep(Hora, n), 0.5)))/(sqrt(sum(n*(Hora - sum(Hora*n)/sum(n))^2)/(sum(n)-1))),
                                                               asim_bowley = (quantile(rep(Hora, n), 0.75) + quantile(rep(Hora, n), 0.25) - 2*quantile(rep(Hora, n), 0.5))/(quantile(rep(Hora, n), 0.75) - quantile(rep(Hora, n), 0.25))
));

chars = chars[order(chars[, 1], chars[, 2], chars[, 3]), ];
chars_x = chars_x[order(chars_x[, 1], chars_x[, 2], chars_x[, 3]), ];

data_merged = merge(chars, chars_x, by = c("Pluid", "dia"), all.x = TRUE); # Realizar el merge entre los datos
data_merged = data_merged[order(data_merged[, 1], data_merged[, 2]), ];

# Clusterizacion
library(h2o)
print("Launching H2O and initializing connnection object ...")
conn <- h2o.init(ip = "localhost", port=54321, nthreads = 4, max_mem_size = '8g')
h2o.removeAll() # Clean slate - just in case the cluster was already running


chars.hex <- as.h2o(data_merged);# Conversión a formato de datos de h2o
chars.hex[,1] <- as.factor(chars.hex[,1]) #plu
chars.hex[,2] <- as.factor(chars.hex[,2]) #dia

rm(x, chars, chars_x, data_merged); # Remover datos de formato R

splits <- h2o.splitFrame(
  chars.hex,           ##  splitting the H2O frame we read above
  c(0.6,0.2),   ##  create splits of 60% and 20%; 
  ##  H2O will create one more split of 1-(sum of these parameters)
  ##  so we will get 0.6 / 0.2 / 1 - (0.6+0.2) = 0.6/0.2/0.2
  seed=1234);

train <- h2o.assign(splits[[1]], "train.hex")   
## assign the first result the R variable train
## and the H2O name train.hex
valid <- h2o.assign(splits[[2]], "valid.hex")   ## R valid, H2O valid.hex
test <- h2o.assign(splits[[3]], "test.hex")     ## R test, H2O test.hex

# k-means clustering

# Transactional and temporal features

#chars.km1 = h2o.kmeans(training_frame = train, validation_frame = valid, nfolds = 5, 
#                      fold_assignment = "Random", keep_cross_validation_predictions = TRUE,
#                      init="Furthest", keep_cross_validation_fold_assignment = TRUE,
#                      k = 100, max_iterations = 100, standardize = TRUE, x =  4:31);

# Only transactional features

#chars.km1_1 = h2o.kmeans(training_frame = train, validation_frame = valid, nfolds = 5, 
#                       fold_assignment = "Random", keep_cross_validation_predictions = TRUE,
#                       init="Furthest", keep_cross_validation_fold_assignment = TRUE,
#                       k = 200, standardize = TRUE, max_iterations = 120, x =  4:16);

chars.km1_1 = h2o.kmeans(training_frame = train, validation_frame = valid, nfolds = 5, 
                         fold_assignment = "Random", keep_cross_validation_predictions = TRUE,
                         init="Furthest", keep_cross_validation_fold_assignment = TRUE,
                         k = 200, standardize = TRUE, max_iterations = 120, x =  c(4:16, 18:30));

plot_ly(x = chars.km1_1@model$scoring_history$iterations, 
        y = chars.km1_1@model$scoring_history$number_of_reassigned_observations);

plot_ly(x = chars.km1_1@model$scoring_history$iterations, 
        y = chars.km1_1@model$scoring_history$within_cluster_sum_of_squares);

centros <- as.matrix(chars.km1_1@model$centers);
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
  dist <- (chars.hex[, c(4:16, 18:30)] - centros.hex[i, 2:ncol(centros.hex)])^2
  x <- sqrt(apply(dist, 1, sum));
  y <- h2o.which_min(x);
  pos[i] <- y[1,1];
}
pos_aux <- sort(unique(pos));
patrones <- chars.hex[pos_aux, ];
nc <- length(pos_aux);

# Estimación de densidad de probabilidad con el kernel density estimation package 
# para hallar las distribuciones de los patrones

library(ks);

plus <- h2o.unique(patrones[, 1]);
meses <- h2o.unique(patrones[, 2]);
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

chars_km.fit = h2o.predict(object = chars.km1_1,  newdata = valid);
chars_km.fit = as.data.frame(chars_km.fit);

I = 170; # Cluster
chars_km.fit2 = which((chars_km.fit == I) == 1);

nearest_center <- as.data.frame(chars.hex[pos[I], 1:2])
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