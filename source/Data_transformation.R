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

RxCharacteristics <- function (z, name, name1, name2){
  start_dt  <- Sys.time();
  
  # x <- rxImport(z) %>% group_by(DependenciaCD, Pluid, dia) %>% summarise(m1 = mean(UnidadesVendidas),
  #                                                              m2 = moment(UnidadesVendidas, order=2),
  #                                                              m3 = moment(UnidadesVendidas, order=3),
  #                                                              sd = sd(UnidadesVendidas),
  #                                                              suma = sum(UnidadesVendidas),
  #                                                              n = n(),
  #                                                              moda = Mode(UnidadesVendidas),
  #                                                              q1 = quantile(UnidadesVendidas, 0.25),
  #                                                              q2 = median(UnidadesVendidas),
  #                                                              q3 = quantile(UnidadesVendidas, 0.75),
  #                                                              sesgo = skewness(UnidadesVendidas),
  #                                                              curtosis = kurtosis(UnidadesVendidas)
  #                                            )
  
  x <- data.table(rxImport(z))
  
  agrupacion<-c("DependenciaCD", "Pluid", "dia")
  
  x <- x[,.(
    suma = sum(UnidadesVendidas),
    m1 = mean(UnidadesVendidas),
    m2 = moment(UnidadesVendidas, order=2),
    m3 = moment(UnidadesVendidas, order=3),
    sd = sd(UnidadesVendidas),
    n = length(UnidadesVendidas),
    moda = Mode(UnidadesVendidas),
    q1 = quantile(UnidadesVendidas, 0.25),
    q2 = median(UnidadesVendidas),
    q3 = quantile(UnidadesVendidas, 0.75),
    sesgo = skewness(UnidadesVendidas),
    curtosis = kurtosis(UnidadesVendidas)
  ),by=agrupacion]
  
  x <- rxDataStep(inData = x, 
                  outFile = name1, 
                  transforms = list(asim_fisher = m3/(sd^3),
                                    asim_pearson0 = (m1 - moda)/sd,
                                    asim_pearson = 3*(m1 - q2)/sd, 
                                    asim_bowley = (q3 + q1 -2*q2)/(q3- q1)),
                  overwrite = TRUE)
  
  # xh <- rxImport(z) %>% group_by(DependenciaCD, Pluid, dia) %>% summarise(m1h = mean(Hora_n),
  #                                                                         m2h = moment(Hora_n, order=2),
  #                                                                         m3h = moment(Hora_n, order=3),
  #                                                                         sdh = sd(Hora_n),
  #                                                                         sumah = sum(Hora_n),
  #                                                                         nh = n(),
  #                                                                         modah = Mode(Hora_n),
  #                                                                         q1h = quantile(Hora_n, .25),
  #                                                                         q2h = median(Hora_n),
  #                                                                         q3h = quantile(Hora_n, 0.75),
  #                                                                         sesgoh = skewness(Hora_n),
  #                                                                         curtosish = kurtosis(Hora_n)
  # )
  
  xh <- data.table(rxImport(z))
  
  xh <- xh[,.(
    suma = sum(Hora_n),
    m1 = mean(Hora_n),
    m2 = moment(Hora_n, order=2),
    m3 = moment(Hora_n, order=3),
    sd = sd(Hora_n),
    n = length(Hora_n),
    moda = Mode(Hora_n),
    q1 = quantile(Hora_n, 0.25),
    q2 = median(Hora_n),
    q3 = quantile(Hora_n, 0.75),
    sesgo = skewness(Hora_n),
    curtosis = kurtosis(Hora_n)
  ),by=agrupacion]
  
  xh <- rxDataStep(inData = xh, 
                   outFile = name2, 
                   transforms = list(asim_fisherh = m3h/(sdh^3),
                                     asim_pearson0h = (m1h - modah)/sdh,
                                     asim_pearsonh = 3*(m1h - q2h)/sdh, 
                                     asim_bowleyh = (q3h + q1h -2*q2h)/(q3h- q1h)),
                   overwrite = TRUE)
  
  xs <- rxMerge(inData1 = x, inData2 = xh, outFile = name, matchVars = c("DependenciaCD", "Pluid", "dia"), type = "inner", overwrite = TRUE)
  end_dt  <- Sys.time();
  dt <- abs(start_dt - end_dt);
  print(dt)
  
  return(xs)
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