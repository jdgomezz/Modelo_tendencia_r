# Cálculo de las características de cada uno de los conjuntos de datos de venta a  nivel hora
# a traves de medidas de tendencia central
# Realizado por: Jesús David Gómez Zuluaga
# Grupo Exito -- Última actualización: Febrero 20 de 2018
#
# Descripción: La presente función calcula carácterísticas de volúmen de venta y tiempo de venta para la ventas a nivel de hora
# a través de diversas medidas de tendencia central tales como: -1er momento
#                                                               -2do momento
#                                                               -Varianza (redundante) ¿?
#                                                               -Desviación estandar
#                                                               -Cuartil 1
#                                                               -Cuartil 2
#                                                               -Cuartil 3
#                                                               -Moda
#                                                               -Asimetría de la distribución de datos
#                                                               -Curtosis
#                                                               -Asimetría de Pearson
#                                                               -Asimetría de Bowley (distancia intercuartílica)

char1 <- function(z){
  start_dt  <- Sys.time();
  
  x <- ddply(vtas, .(Pluid, mes, dia, Hora, UnidadesVendidas), NROW)
  
  chars <- ddply(x, .(Pluid, mes, dia), .fun = summarize, m1 = sum(UnidadesVendidas*V1)/sum(V1), 
                                                          m2 = sum(n*(UnidadesVendidas^2))/sum(V1))
  end_dt <- Sys.time()
  dt <- abs(end_dt - start_dt)
  return(chars)
  print(dt)
}
  
characteristics <- function(z){
  start_dt  <- Sys.time();
  
  hours <- read.table("~/csv/Hora.csv", header = TRUE, sep = ",");
  hours <- hours[, !(names(hours) %in% "key")];
  
  x <- data.frame(count(z, Pluid, mes, dia, Hora, UnidadesVendidas));

  chars <- data.frame(x %>% group_by(Pluid, mes, dia) %>% summarise(m1 = sum(UnidadesVendidas*n)/sum(n), 
                                                                    m2 = sum(n*(UnidadesVendidas^2))/sum(n),
                                                                    s =  sum(n*((UnidadesVendidas - sum(UnidadesVendidas*n)/sum(n))^2)),
                                                                    sd = sqrt(sum(n*(UnidadesVendidas - sum(UnidadesVendidas*n)/sum(n))^2)/(sum(n)-1)),
                                                                    moda = Mode(rep(UnidadesVendidas, n)),
                                                                    q1 = quantile(rep(UnidadesVendidas, n), 0.25),
                                                                    q2 = quantile(rep(UnidadesVendidas, n), 0.5),
                                                                    q3 = quantile(rep(UnidadesVendidas, n), 0.75),
                                                                    asim_fisher = (sum(n*(UnidadesVendidas - sum(UnidadesVendidas*n)/sum(n))^3)/sum(n))/((sum(n*(UnidadesVendidas - sum(UnidadesVendidas*n)/sum(n))^2)/sum(n))^(3/2)),
                                                                    curtosis = (sum(n*(UnidadesVendidas - sum(UnidadesVendidas*n)/sum(n))^4)/sum(n))/((sum(n*(UnidadesVendidas - sum(UnidadesVendidas*n)/sum(n))^2)/sum(n))^2),
                                                                    asim_pearson1 = ((sum(UnidadesVendidas*n)/sum(n)) - (Mode(rep(UnidadesVendidas, n))))/(sqrt(sum(n*(UnidadesVendidas - sum(UnidadesVendidas*n)/sum(n))^2)/(sum(n)-1))),
                                                                    asim_pearson2 = 3*((sum(UnidadesVendidas*n)/sum(n)) - (quantile(rep(UnidadesVendidas, n), 0.5)))/(sqrt(sum(n*(UnidadesVendidas - sum(UnidadesVendidas*n)/sum(n))^2)/(sum(n)-1))),
                                                                    asim_bowley = (quantile(rep(UnidadesVendidas, n), 0.75) + quantile(rep(UnidadesVendidas, n), 0.25) - 2*quantile(rep(UnidadesVendidas, n), 0.5))/(quantile(rep(UnidadesVendidas, n), 0.75) - quantile(rep(UnidadesVendidas, n), 0.25))
  ));
  
  chars_x <- data.frame(x %>% group_by(Pluid, mes, dia) %>% summarise(m1 = sum(Hora*n)/sum(n), 
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
  characteristics <- list(x, chars, chars_x)
  end_dt  <- Sys.time();
  dt <- abs(start_dt - end_dt);
  print(dt)
  return(characteristics)
}

# Cálculo de la probabilidad conjunta empírica
# Realizado por: Jesús David Gómez Zuluaga
# Grupo Exito -- Última actualización: Febrero 20 de 2018
#
# Descripción: El presente Script 

Probs <-function(xx){
  xx$n <- xx$n/sum(xx$n);
  
  Hora <- sort(unique(xx$Hora));
  Unds <- sort(unique(xx$UnidadesVerdaderas));
  cum <- matrix(nrow = length(Unds), ncol= length(Hora));
  den <- matrix(nrow = length(Unds), ncol= length(Hora));
  for (i in  1:length(Unds)){
    for (j in 1:length(Hora)){
      cum[i, j] <- sum(xx[xx$Hora <= Hora[j] & xx$UnidadesVerdaderas <= Unds[i], ]$n); # Función de distribución de probabilidad conjunta
      den[i, j] <-  sum(xx[xx$Hora == Hora[j] & xx$UnidadesVerdaderas == Unds[i], ]$n); # Función de densidad de probabilidad conjunta
    }
  }
  Probs <- list(cum, den)
}

# Realizado por: Jesús David Gómez Zuluaga
# Grupo Exito -- Última actualización: Febrero 22 de 2018
#
# Descripción: El presente script se encarga de calcular la distribución de probabilidad
# Conjunta de un conjunto de datos


estimated_dist <- function (x, y, bool){
  dat <- as.matrix(data.frame(x, y))
  Hpi1 <- Hpi(x=dat)
  fhat.pi1 <- kde(x=dat, H = Hpi1)
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  estimated_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  if (bool == 1){
    plot_ly(x = v, y = u, z= est_dist) %>% add_surface() 
  }
}



