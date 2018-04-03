# Definir catacteristicas utilizando instrucciones de RevolutionScale
rxDataStep(inData = venta, outFile = venta, overwrite = TRUE, transforms = list(concat = paste0(Pluid, "-", dia)))

# split the data by 'part' variable (which has to be a factor)
 xdfList <- rxSplit(venta, outFilesBase="base", splitByFactor= "dia", numOut = 1, overwrite = TRUE)

 smry <- rxExec(function(xdf) {
  df <- rxImport(xdf)
  # median of a numeric variable
  xmed <- sum(df$UnidadesVendidas)
  data.frame(part=as.character(df$dia), xmed=xmed)
}, xdf = rxElemArg(xdfList))
smry <- do.call(rbind, smry)




# ================= OTRA ALTERNATIVA DE CONSTRUCCI?N DE PATRONES =======================
## Distribuciones Hora-Unidades
pattern <- data.frame(patron = 1,
                      Hora = 1,
                      unds = 1,
                      weight = 1)
pattern[-1, ]

for (i in 1:nc){
  
  auxdata <- dat[dat$concat == llaves_patrones[i], ]
  auxdata <- auxdata[, !(names(auxdata) %in% c("concat", "DependenciaCD", "Pluid", "dia"))]
  
  Hpi1 <- Hpi(x=auxdata);
  fhat.pi1 <- kde(x=auxdata, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  
  # Adimensionalización de la distribición [a, b] -> [0, 1]
  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  # 
  plot_ly(x = v, y = u, z= est_dist) %>% add_surface() 
  
  # Estimación de función de densidad con kernel gaussiano
  
  Hpi1 <- Hpi(x=auxdata);
  fhat.pi1 <- kde(x=auxdata, H=Hpi1);
  u <- fhat.pi1$eval.points[[1]];
  v <- fhat.pi1$eval.points[[2]];
  
  # Adimensionalización de la distribición [a, b] -> [0, 1]
  est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
  # 
  plot_ly(x = v, y = u, z= est_dist) %>% add_surface() 
  
  xy <- expand.grid(u, v)
  est_dist <- as.vector(est_dist)
  
  aux <- data.frame(patron = i,
                    Hora = xy[, 1],
                    unds = xy[, 2],
                    weight = est_dist)
  
  pattern <- cbind(aux, pattern)
  # convertir la matriz en un vector
}

# =================== VALIDACI?N DE PATRONES (verificar si los miembros de los grupos si son similares a sus centroides) ==============
