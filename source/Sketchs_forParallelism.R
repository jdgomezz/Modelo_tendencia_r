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