rm(list = ls())

# Onlinux
 setwd("~/")

# On Windows
# setwd("C:/Users/User/Desktop")

wd <- getwd()
root <- paste0(wd, '/Agotado_en_gondola')
inSource <- "xdf/"
landing <- "xdf/"
model_lib <- "xdf/"
output_lib <- "xdf/"
log_lib <- "xdf/"

libs <- list(inSource, landing, model_lib, output_lib, log_lib)
# ================== DECLARACIÓN DE LIBRERIAS EXTERNAS Y PROPIAS ===================

source(paste0(root, '/source/Load_libs.R'))
Load_libs(root)

RxComputeContext("RxLocalParallel") # Cambiar contexto de ejecución de la máquina a paralelo

# ================ INSTRUCCIONES DE CARGA DE DATOS ========================================

# Cargar datos de venta utilizanzo función de R Open a partir de una consulta en Teradata e imprimir
# una tabla de datos con formato xdf

fi <- "'2018-01-01'"                         # Fecha inicial
ff <- "'2018-03-31'"                         # Fecha final
cut_registros <- 0                          # Número m??nimo de registro por plu-dep
cut_ultima_venta <- "'2017-11-01'"           # Fecha de última venta realizada
cut_tiempo_vida <- 30                         # Tiempo de vida m??nimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
cut_proporcion <- 1                          # Proporción de venta m??nima (Nro. registros)/(Tiempo de vida)
n_deciles <- 100                             # Nro de deciles a seccionar la muestra
booleano <- TRUE
query <- '~/Agotado_en_gondola/querys/query_extraccion_limpieza_v2.txt'
pars <- list(fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles, booleano)
querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL and DependenciaCD in (41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569)"
#querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL"
connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
dep <- rxImport(odbcDS)$DependenciaCD

tiendas <- c(41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569)

nodes <- 8

cl <-makeCluster(nodes)
registerDoParallel(cl)
system.time( 
  result <- foreach (i = 1:length(tiendas), .combine='cbind', .export = c('LoadXdf', "RxCharacteristics", "cluster_model", "Mode"), .packages = c("moments", "data.table", "h2o", "plotly")) %dopar% {
    centralChars <-  CharsByDep(tiendas[i], query, libs, pars, fileConn, FALSE)
  }
)
stopCluster(cl)
rm(cl)  

# ==================== CONSOLIDACIÓN DEL ARCHIVO DE SALIDA ============================
outfile_patrones <- paste0(output_lib, "CentralChars_all_deps.xdf")

DEPS <- 1
outfile<- rxImport(paste0(output_lib, "CentralChars_", tiendas[DEPS] , ".xdf"))
outfile <- outfile[order(outfile$Pluid, outfile$dia, outfile$Hora_n), ]

outfile <-with(outfile, expand.grid(unique(DependenciaCD), unique(Pluid), unique(dia), unique(Hora_n))) %>%
  setNames(c("DependenciaCD", "Pluid", "dia", "Hora_n")) %>%
  left_join(., outfile, by=c("DependenciaCD", "Pluid", "dia", "Hora_n"))

outfile <- outfile[order(outfile$Pluid, outfile$dia, outfile$Hora_n), ]

for (DEPS in 2:length(tiendas)){
  aux <- rxImport(paste0(output_lib, "CentralChars_",  tiendas[DEPS] , ".xdf"))
  aux <-with(aux, expand.grid(unique(DependenciaCD), unique(Pluid), unique(dia), unique(Hora_n))) %>%
             setNames(c("DependenciaCD", "Pluid", "dia", "Hora_n")) %>%
             left_join(., aux, by=c("DependenciaCD", "Pluid", "dia", "Hora_n"))
  
  aux <- aux[order(aux$Pluid, aux$dia, aux$Hora_n), ]
  
  outfile <- rbind(outfile, aux)
  print(DEPS)
}

patrones_all <- rxImport(inData = outfile,
                         outFile = outfile_patrones,
                         varsToDrop = c("sd", "m1", "q1", "q2", "q3"),
                         overwrite = TRUE)
# ==================== ESCRITURA DE ARCHIVO DE SALIDA DE PATRÓN =====================

# Organizaci?n de formatro de archivo de salida

dias <- c(Sys.Date()- as.POSIXlt(Sys.Date())$wday,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 1,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 2,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 3,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 4,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 5,
          Sys.Date()- as.POSIXlt(Sys.Date())$wday + 6)# Primer d?a de la semana
fecha_dia = data.frame(dia = 1:7, diafecha = dias)

outfile <- merge(x = outfile, y = fecha_dia, by = "dia", all.x = TRUE)
outfile <- subset(outfile, select = c(11, 3, 2, 4, 7, 5, 9))
colnames(outfile) <- c("Fecha", "Pluid", "storeid", "Hora", "P1", "P2", "P3")

outfile[is.na(outfile[, 5]), c(5, 6, 7)] <- 0
outfile <- outfile[order(outfile$Fecha, outfile$storeid, outfile$Pluid, outfile$Hora), ]# Escritura de archivo final

outfile$Fecha <- format(outfile$Fecha, "%Y-%m-%d")
outfile$storeid <- as.numeric(as.character(outfile$storeid))
outfile$Pluid <- as.numeric(as.character(outfile$Pluid))
outfile$Hora <- as.numeric(as.character(outfile$Hora))
outfile$Fecha <- as.Date(outfile$Fecha) 
write.table(outfile, file = paste0(output_lib, "tendencia_.csv"), sep=",", row.names = FALSE)

system('sshpass -p "hadoop" scp ~/xdf/tendencia_.csv hdp_agotadoln@10.2.113.138:/data/LZ/Agotados/Datos/tendencia_.csv')

# ================= Evaluaci?n de los patrones =========================================
# Extracción de venta histórica

query <- "SELECT DependenciaCD as storeid, Pluid, Fecha, Hora, UnidadesVendidas  FROM bd_ddpo.vwventashora where (DependenciaCD in (&dep.)) and (Fecha between &fi and &ff) and UnidadesVendidas > 0 "
query <- gsub("&dep.", "41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569", query);
query <- gsub("&fi", "'2018-04-08'" , query);                 # Fecha inicial
query <- gsub("&ff", "'2018-04-14'" , query);                 # Fecha final

connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = query,connectionString = connectionString)
validation_data <- rxImport(odbcDS)
validation_data$Hora <- as.numeric(validation_data$Hora)
validation_data$Fecha <- weekdays(as.Date(validation_data$Fecha))

validation_data_ <- validation_data
validation_data_$Hora <- validation_data_$Hora + 1

# Modelo de tendencia
tendencia <- read.csv("xdf/tendencia.csv", header = TRUE)
tendencia$Fecha <- weekdays(as.Date(tendencia$Fecha))

tendencia2 <- tendencia
tendencia2$Hora <-tendencia2$Hora + 1
# Modelo de características
modelo <- read.csv("xdf/tendencia_.csv", header = TRUE)
modelo$Fecha <- weekdays(as.Date(modelo$Fecha))

outfile_a <- merge(x = tendencia,
                   y = tendencia2, 
                   by = c("Fecha", "Pluid", "storeid", "Hora"),
                   all.x = TRUE)

outfile_b <- merge(x = outfile_a,
                 y = modelo, 
                 by = c("Fecha", "Pluid", "storeid", "Hora"),
                 all.x = TRUE)

outfile_c <- merge(x = outfile_b,
                 y = validation_data, 
                 by = c("Fecha", "Pluid", "storeid", "Hora"),
                 all.x = TRUE)

outfile_d <- merge(x = outfile_c,
                   y = validation_data_, 
                   by = c("Fecha", "Pluid", "storeid", "Hora"),
                   all.x = TRUE)

outfile_d[is.na(outfile_d$Perfil.y), 6] <- 0
outfile_d[is.na(outfile_d$UnidadesVendidas.x), 10] <- 0
outfile_d[is.na(outfile_d$UnidadesVendidas.y), 11] <- 0

outfile_d$Proy <- outfile_d$UnidadesVendidas.y*ifelse(outfile_d$Perfil.y == 0, 0, (outfile_d$Perfil.x/outfile_d$Perfil.y))

outfile_d$abs_proy <- abs(outfile_d$UnidadesVendidas.x-outfile_d$Proy)
outfile_d$abs_proy2 <- abs(outfile_d$UnidadesVendidas.x-outfile_d$P1)
outfile_d$abs_proy3 <- abs(outfile_d$UnidadesVendidas.x-outfile_d$P2)
outfile_d$abs_proy4 <- abs(outfile_d$UnidadesVendidas.x-outfile_d$P3)

query_prods <- "SELECT SUBLINEA_ID, SUBLINEA_DESC, CATEGORIA_ID, CATEGORIA_DESC, SUBCATEGORIA_ID, SUBCATEGORIA_DESC, MARCA_ID, MARCA_DESC, PLUID, PLUCD, PLU_DESC FROM bd_ddpo.NVM_PRODUCTS_A where GEN_ID = 40 and PLUVIGENTE = 1"

connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = query_prods,connectionString = connectionString)
products <- rxImport(odbcDS)

query_deps <- "SELECT LocCD, TIENDA_DC_DESC, CADENA_DESC FROM bd_ddpo.NVM_LOCATION_A"
odbcDS <-RxOdbcData(sqlQuery = query_deps,connectionString = connectionString)
deps <- rxImport(odbcDS)

outfile_d <- merge(x = outfile_d,
                   y = products, 
                   by.x  = c("Pluid"),
                   by.y = c("PluID"),
                   all.x = TRUE)

outfile_d <- merge(x = outfile_d,
                   y = deps, 
                   by.x  = c("storeid"),
                   by.y = c("LocCD"),
                   all.x = TRUE)

outfile_d <- outfile_d[order(outfile_d$Fecha, outfile_d$Pluid, outfile_d$storeid, outfile_d$Hora), ]

write.table(outfile_d, file = paste0(output_lib, "modelo_deps.csv"), sep=",", row.names = FALSE)
write.table(outfile_aux, file = paste0(output_lib, "modelo_deps.csv"), sep=",", row.names = FALSE)


# ============ MEDICION DE ASERTIVIDAD ==========
outfile_aux <- outfile_d[outfile_d$UnidadesVendidas.x != 0, ]
#outfile_aux <- outfile_d

asertividad <- data.table(outfile_aux %>% group_by(SUBLINEA_ID, SUBLINEA_DESC, 
                                                   CATEGORIA_ID, CATEGORIA_DESC, 
                                                   SUBCATEGORIA_ID, SUBCATEGORIA_DESC,
                                                   MARCA_ID, MARCA_DESC,
                                                   Pluid, PLUCD, PLU_DESC,
                                                   storeid, TIENDA_DC_DESC, CADENA_DESC, Fecha) %>% summarise(sum_mod1 = sum(UnidadesVendidas.x),
                                                                                                       asert_mod1 = 1-100*sum(abs_proy)/sum(UnidadesVendidas.x),
                                                                                                       sum_mod2 = sum(P1),
                                                                                                       asert_mod2 = 100*(1-sum(abs_proy2)/sum(P1)),
                                                                                                       sum_mod3 = sum(P2),
                                                                                                       asert_mod3 = 100*(1-sum(abs_proy3)/sum(P2)),
                                                                                                       sum_mod4 = sum(P3),
                                                                                                       asert_mod4 = 100*(1-sum(abs_proy4)/sum(P3))))

asertividad_subl <- data.table(outfile_aux %>% group_by(SUBLINEA_ID, SUBLINEA_DESC,storeid, TIENDA_DC_DESC, Fecha,  Hora) %>% summarise(sum_mod1 = sum(UnidadesVendidas.x),
                                                                                                  asert_mod1 = 1-100*sum(abs_proy)/sum(UnidadesVendidas.x),
                                                                                                  sum_mod2 = sum(P1),
                                                                                                  asert_mod2 = 100*(1-sum(abs_proy2)/sum(P1)),
                                                                                                  sum_mod3 = sum(P2),
                                                                                                  asert_mod3 = 100*(1-sum(abs_proy3)/sum(P2)),
                                                                                                  sum_mod4 = sum(P3),
                                                                                                  asert_mod4 = 100*(1-sum(abs_proy4)/sum(P3))))

asertividad_fecha <- data.table(outfile_aux %>% group_by(Fecha, Hora) %>% summarise(sum_mod1 = sum(UnidadesVendidas.x),
                                                                                    asert_mod1 = 1-100*sum(abs_proy)/sum(UnidadesVendidas.x),
                                                                                    sum_mod2 = sum(P1),
                                                                                    asert_mod2 = 100*(1-sum(abs_proy2)/sum(P1)),
                                                                                    sum_mod3 = sum(P2),
                                                                                    asert_mod3 = 100*(1-sum(abs_proy3)/sum(P2)),
                                                                                    sum_mod4 = sum(P3),
                                                                                    asert_mod4 = 100*(1-sum(abs_proy4)/sum(P3))))


asertividad_dep <- data.table(outfile_aux %>% group_by(storeid, TIENDA_DC_DESC) %>% summarise(sum_mod1 = sum(UnidadesVendidas.x),
                                                                                              asert_mod1 = 1-100*sum(abs_proy)/sum(UnidadesVendidas.x),
                                                                                              sum_mod2 = sum(P1),
                                                                                              asert_mod2 = 100*(1-sum(abs_proy2)/sum(P1)),
                                                                                              sum_mod3 = sum(P2),
                                                                                              asert_mod3 = 100*(1-sum(abs_proy3)/sum(P2)),
                                                                                              sum_mod4 = sum(P3),
                                                                                              asert_mod4 = 100*(1-sum(abs_proy4)/sum(P3))))


asertividad_hora <- data.table(outfile_aux %>% group_by(Hora) %>% summarise(sum_mod1 = sum(UnidadesVendidas.x),
                                                                            asert_mod1 = 1-100*sum(abs_proy)/sum(UnidadesVendidas.x),
                                                                            sum_mod2 = sum(P1),
                                                                            asert_mod2 = 100*(1-sum(abs_proy2)/sum(P1)),
                                                                            sum_mod3 = sum(P2),
                                                                            asert_mod3 = 100*(1-sum(abs_proy3)/sum(P2)),
                                                                            sum_mod4 = sum(P3),
                                                                            asert_mod4 = 100*(1-sum(abs_proy4)/sum(P3))))

# ================ ENVIAR E-MAIL DE FINALIZACIÓN DE PROCESO =============================

send.mail(from = "jgomezz101@gmail.com",                                                     # Desde
          to = "jdgomezz@grupo-exito.com",                                                  # Para
          subject = "Reporte eficiencia modelo de tendencia",                                # Asunto
          body = "Buena tarde",                                                              # Cuerpo
          # html = T,  # recibe HTML encoding para el cuerpo
          encoding = "utf-8",                                                                # Codificacion
          #attach.files = c(pathLog),                                                        # rutas de los archivos a adjuntar
          #file.names = c("Log Ejecucion"),                                                  # nombres de los archivos a adjuntar
          smtp = list(host.name = "smtp.gmail.com",                                          # Host del mail
                      port = 465,                                                            # Puerto del host
                      user.name = "jgomezz101@gmail.com",                                    # Usuario que envia
                      passwd = "N1nt3nd0\""),                                                 # Contrase?a del que envia
          ssl = TRUE,
          authenticate = FALSE,                                                              # Autenticar
          send = TRUE,
          debug=TRUE)
