library(lubridate)

plus <- td_Xdf(query = "select pluid as Pluid, plucd as Plucd from bd_ddpo.nvm_products_a where gen_id = 40 and pluvigente = 1", name = "xdf/plus.xdf")
plus <- rxImport(plus)

rxImport(inData = ptrns_file, outFile = "xdf/tendencia.xdf")
rxImport(inData = vtas_file, outFile = "xdf/ventahora.xdf")
rxImport(inData = inventario_file, outFile = "xdf/inventariohora.xdf")

ptrns_file <- "xdf/tendencia.csv"
vtas_file <- "xdf/ventahora.csv"
inventario_file <- "xdf/inventariohora.csv"
lag_day <- 1
init_hour <- 8

hoy <- Sys.Date() - lag_day
hora_actual <- hour(Sys.time()) - 5
hora_actual <- 10
nh <- hora_actual - init_hour

patrones <- ptrns_file
ventas <- vtas_file
inventario <- inventario_file

#out <- validacion(patrones = ptrns_file, ventas = vtas_file, inventario = inventario_file)

#validacion <- function(patrones, ventas, inventario){
  
  # Extracción de información

  # Patrones
  ptrns <- read.csv(patrones)
  ptrns$Fecha <- as.Date(ptrns$Fecha)
  ptrns_hoy <- ptrns[ptrns$Fecha == hoy, ]
  ptrns_hoy <- ptrns_hoy[ptrns_hoy$Hora %between% c(init_hour, hora_actual), ]
  
  # Venta
  vtas <- read.csv(ventas, header = TRUE, sep = ",")
  names(vtas) <- c("Fecha", "Hora", "storeid", "CadenaCD", "Cadena_desc", "SubzonaCD", "Subzona_desc", "MarcaCD", "Marca_desc", "Plucd", "Pluid", "Plu_desc", "undsvendidas", "undsnetas", "ventaneta", "vtasinimpuesto")
  
  # Inventario
  inv <- read.csv(inventario, header = TRUE, sep = ",")
  names(inv) <- c("storeid", "Plucd", "Clase", "Fecha", "Hora", "Inventario")
  inv <- inner_join(inv, plus, by = c("Plucd"))
  
  # Cruce entre patrones e inventario
  ptrns_inv <- merge(x = ptrns_hoy, y = inv, by = c("storeid", "Pluid", "Hora"), all.x = TRUE)
  ptrns_inv <- ptrns_inv[, (names(ptrns_inv) %in%  c("Fecha.x", "Hora", "storeid", "Plucd", "Pluid", "Clase","Inventario", "Perfil"))]
  
  # Cruce entre patrones venta e inventario
  ptrns_inv_vta <- merge(x = ptrns_inv, y = vtas, by = (c("Pluid", "storeid", "Hora")), all.x = TRUE)
  ptrns_inv_vta <- ptrns_inv_vta[, (names(ptrns_inv_vta) %in%  c("Pluid", "storeid", "Hora", "Plucd.x", "Fecha.x", "Clase", "Perfil", "Inventario", "undsvendidas", "undsnetas", "ventaneta", "vtasinimpuesto"))]
  colnames(ptrns_inv_vta)[4] <- "Fecha"
  colnames(ptrns_inv_vta)[6] <- "Plucd"
  ptrns_inv_vta[is.na(ptrns_inv_vta$undsvendidas), 9:12] <- 0
  ptrns_inv_vta <- cbind(ptrns_inv_vta, py)
  ptrns_inv_vta$concat <- paste0(ptrns_inv_vta$Pluid, "-", ptrns_inv_vta$storeid)
  # Lógica de agotado en góndola
  
  llaves <- unique(ptrns_inv_vta$concat)
  for (j in 1:length(llaves)){
    llave <- llaves[j]
    pos <- which(ptrns_inv_vta$concat == llave)
    ini <- head(pos, n=1)
    endi <- tail(pos, n=1)
    for (i in ini+1:endi){
      p_i_1 <- ptrns_inv_vta$Perfil[i-1]
      p_i <-   ptrns_inv_vta$Perfil[i]
      ratio <- p_i/p_i_1
      ptrns_inv_vta$py[i] <- ptrns_inv_vta$undsvendidas[i-1]*ratio
    }
  }
  
  x <- list(ptrns, vtas)
  return(x)
#}