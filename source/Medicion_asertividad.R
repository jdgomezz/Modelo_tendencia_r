
ptrns_file <- "xdf/patterns.csv"
vtas_file <- "xdf/ventahora.csv"

out <- validacion(patrones = ptrns_file, ventas = vtas_file)

validacion <- function(patrones, ventas){
  ptrns <- read.csv(patrones)
  ptrns$Fecha <- as.Date(ptrns$Fecha)
  vtas <- read.csv(ventas, header = TRUE, sep = ",")
  names(vtas) <- c("Fecha", "Hora", "DependenciaCD", "CadenaCD", "Cadena_desc", "SubzonaCD", "Subzona_desc", "MarcaCD", "Marca_desc", "Plucd", "Pluid", "Plu_desc", "undsvendidas", "undsnetas", "ventaneta", "vtasinimpuesto")
  
  hoy <- Sys.Date()
  hora_actual <- hour(Sys.time()) - 5
  rango_analisis <- 7:hora_actual
  ptrns_hoy <- ptrns[ptrns$Fecha == hoy, ]

  ptrns_vta <- merge(x = ptrns_hoy, y = vtas, by = c(, all.x = TRUE)
  
  
  vtas$a.fechacontable <- as.Date(vtas$a.fechacontable)
  
  
  x <- list(ptrns, vtas)
  return(x)
}