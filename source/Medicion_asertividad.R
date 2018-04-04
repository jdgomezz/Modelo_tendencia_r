library(lubridate)
ptrns_file <- "xdf/tendencia.csv"
vtas_file <- "xdf/ventahora.csv"
inventario_file <- "xdf/inventariohora.csv"

rxImport(inData = ptrns_file, outFile = "xdf/tendencia.xdf")
rxImport(inData = vtas_file, outFile = "xdf/ventahora.xdf")
rxImport(inData = inventario_file, outFile = "xdf/inventariohora.xdf")

patrones <- ptrns_file
ventas <- vtas_file
inventario <- inventario_file

out <- validacion(patrones = ptrns_file, ventas = vtas_file, inventario = inventario_file)

validacion <- function(patrones, ventas, inventario){
  
  # Extracción de información
  ptrns <- read.csv(patrones)
  ptrns$Fecha <- as.Date(ptrns$Fecha)
  vtas <- read.csv(ventas, header = TRUE, sep = ",")
  names(vtas) <- c("Fecha", "Hora", "storeid", "CadenaCD", "Cadena_desc", "SubzonaCD", "Subzona_desc", "MarcaCD", "Marca_desc", "Plucd", "Pluid", "Plu_desc", "undsvendidas", "undsnetas", "ventaneta", "vtasinimpuesto")
  
  inv <- read.csv(inventario, header = TRUE, sep = ",")
  names(inv) <- c("storeid", "Plucd", "Clase", "Fecha", "Hora", "Cantidad")
  
  vta_inv <- merge(x = vtas, y = inv, by = (c("Plucd", "storeid", "Hora")), all.x = TRUE)
  vta_inv <- vta_inv[, (names(vta_inv) %in%  c("Fecha.x", "Hora", "storeid",  "CadenaCD", "Cadena_desc", "SubzonaCD", "Subzona_desc", "MarcaCD", "Marca_desc", "Plucd", "Pluid", "Plu_desc", "undsvendidas", "undsnetas", "ventaneta", "vtasinimpuesto", "Cantidad"))]
  colnames(vta_inv)[4] <- "Fecha"
  colnames(vta_inv)[17] <- "Inventario"
  vta_inv[is.na(vta_inv$Inventario), "Inventario"] <- 0
  
  hoy <- Sys.Date()
  hora_actual <- hour(Sys.time()) - 5
  nh <- hora_actual - 7
  
  ptrns_hoy <- ptrns[ptrns$Fecha == hoy, ]
  ptrns_hoy <- ptrns_hoy[ptrns_hoy$Hora %between% c(7, hora_actual), ]
  ptrns_vta <- merge(x = ptrns_hoy, y = vta_inv, by = c("storeid", "Pluid", "Hora"), all.x = TRUE)
  
  ptrns_vta <- ptrns_vta[, !(names(ptrns_vta) %in%  c("Fecha.y", "CadenaCD", "Cadena_desc",  "CadenaCD", "SubzonaCD", "Subzona_desc", "MarcaCD", "Marca_desc", "Plu_desc"))]
  
  ptrns_vta[is.na(ptrns_vta$undsvendidas), 7:11] <-0
  
  llaves <- unique(ptrns_vta[, 1:2])
  for (j in 1:nrow(llaves)){
    llave <- llaves[j, ]
    ptn_vta <- inner_join(ptrns_vta, llave, by = c("storeid", "Pluid"))
    for (i in 2:nh+1){
      p_i_1 <- ptn_vta$Perfil[i-1]
      p_i <-   ptn_vta$Perfil[i]
      ratio <- p_i/p_i_1
      py <- ptn_vta$undsvendidas[i-1]*ratio
    }
  }
  
  x <- list(ptrns, vtas)
  return(x)
}