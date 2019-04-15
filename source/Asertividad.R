# ================= Evaluaci?n de los patrones =========================================
# Extracción de venta histórica

query <- "SELECT DependenciaCD as storeid, Pluid, Fecha, Hora, UnidadesVendidas  FROM bd_ddpo.vwventashora where (DependenciaCD in (&dep.)) and (Fecha between &fi and &ff) and UnidadesVendidas > 0 "
query <- gsub("&dep.", "41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569", query);
query <- gsub("&fi", "'2018-04-08'" , query);                 # Fecha inicial
query <- gsub("&ff", "'2018-04-21'" , query);                 # Fecha final

connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = query,connectionString = connectionString)
validation_data <- rxImport(odbcDS)
validation_data$Hora <- as.numeric(validation_data$Hora)
validation_data$Semana <- floor_date(as.Date(validation_data$Fecha, "%Y-%m-%d"), unit="week")
validation_data$dia <- weekdays(as.Date(validation_data$Fecha))

validation_data_ <- validation_data
validation_data_$Hora <- validation_data_$Hora + 1

tiendas <- c(41, 54, 75, 33, 35, 31, 568, 4701, 94, 92, 564, 83, 581, 81, 86, 88, 4043, 84, 356, 569)

acum <- data.frame(DependenciaCD = -1,
                   Pluid = -1, 
                   Primera_Venta = -1,
                   Ultima_Venta = -1, 
                   Tiempo_de_vida = -1,
                   Proporcion = -1, 
                   numDias = -1,
                   Proporcion_dias = -1)

# Características globales de los registros
acum <- acum[-1, ]
for (i in 1:length(tiendas)){
  a <- rxImport(paste0("xdf/ventas_", tiendas[i], ".xdf"))
  x <- unique(a[, c(1, 4, 24, 25, 26, 27, 28, 29)])
  acum <- rbind(acum, x)
}
# Modelo de tendencia
tendencia <- read.csv("xdf/tendencia.csv", header = TRUE)
tendencia$Fecha <- weekdays(as.Date(tendencia$Fecha))
colnames(tendencia)[which(names(tendencia) == "Fecha")] <- "dia"

tendencia2 <- tendencia
tendencia2$Hora <-tendencia2$Hora + 1
# Modelo de características
modelo <- read.csv("xdf/tendencia_.csv", header = TRUE)
modelo$Fecha <- weekdays(as.Date(modelo$Fecha))
colnames(modelo)[which(names(modelo) == "Fecha")] <- "dia"

semanas <- unique(validation_data$Semana)

outfile_a <- merge(x = tendencia,
                   y = tendencia2, 
                   by = c("dia", "Pluid", "storeid", "Hora"),
                   all.x = TRUE)

outfile_b <- merge(x = outfile_a,
                   y = modelo, 
                   by = c("dia", "Pluid", "storeid", "Hora"),
                   all.x = TRUE)

outfile_all <- data.frame(Pluid=-1,
                          storeid=-1,
                          dia = -1,
                          Hora = -1,
                          Perfil.x = -1,
                          Perfil.y = -1,
                          P1 = -1,
                          P2 = -1,
                          P3 = -1, 
                          UnidadesVendidas.x = -1, 
                          UnidadesVendidas.y = -1,
                          Semana = as.Date("2018-01-01"))

outfile_all <- outfile_all[-1, ]
for (i in 1:length(semanas)){
    val <- validation_data[validation_data$Semana == semanas[i], c(1,2,4,5, 7)]
    
    val_ <-  validation_data_[validation_data_$Semana == semanas[i], c(1,2,4,5, 7)]
    
    outfile_c <- merge(x = outfile_b,
                       y = val, 
                       by = c("Pluid", "storeid", "dia", "Hora"),
                       all.x = TRUE)
    
    outfile_d <- merge(x = outfile_c,
                       y = val_, 
                       by = c("Pluid", "storeid", "dia", "Hora"),
                       all.x = TRUE)
    
    outfile_d[is.na(outfile_d$Perfil.y), 6] <- 0
    outfile_d[is.na(outfile_d$UnidadesVendidas.x), 10] <- 0
    outfile_d[is.na(outfile_d$UnidadesVendidas.y), 11] <- 0
    
    outfile_d$Semana <- semanas[i]
    outfile_all <- rbind(outfile_d, outfile_all)
}
    outfile_all$Proy <- outfile_all$UnidadesVendidas.y*ifelse(outfile_all$Perfil.y == 0, 0, (outfile_all$Perfil.x/outfile_d$Perfil.y))
    
    outfile_all$abs_proy <- abs(outfile_all$UnidadesVendidas.x-outfile_all$Proy)
    outfile_all$abs_proy2 <- abs(outfile_all$UnidadesVendidas.x-outfile_all$P1)
    outfile_all$abs_proy3 <- abs(outfile_all$UnidadesVendidas.x-outfile_all$P2)
    outfile_all$abs_proy4 <- abs(outfile_all$UnidadesVendidas.x-outfile_all$P3)
    
    query_prods <- "SELECT SUBLINEA_ID, SUBLINEA_DESC, CATEGORIA_ID, CATEGORIA_DESC, SUBCATEGORIA_ID, SUBCATEGORIA_DESC, MARCA_ID, MARCA_DESC, PLUID, PLUCD, PLU_DESC FROM bd_ddpo.NVM_PRODUCTS_A where GEN_ID = 40 and PLUVIGENTE = 1"
    
    connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
    odbcDS <-RxOdbcData(sqlQuery = query_prods,connectionString = connectionString)
    products <- rxImport(odbcDS)
    
    query_deps <- "SELECT LocCD, TIENDA_DC_DESC, CADENA_DESC FROM bd_ddpo.NVM_LOCATION_A"
    odbcDS <-RxOdbcData(sqlQuery = query_deps,connectionString = connectionString)
    deps <- rxImport(odbcDS)
    
    outfile_all <- merge(x = outfile_all,
                       y = products, 
                       by.x  = c("Pluid"),
                       by.y = c("PluID"),
                       all.x = TRUE)
    
    outfile_all <- merge(x = outfile_all,
                       y = deps, 
                       by.x  = c("storeid"),
                       by.y = c("LocCD"),
                       all.x = TRUE)

outfile_all <- outfile_all[order(outfile_all$dia, outfile_all$Pluid, outfile_all$storeid, outfile_all$Hora), ]
outfile_all <- merge(x = outfile_all,
                     y = acum,
                     by.x = c("storeid", "Pluid"),
                     by.y = c("DependenciaCD", "Pluid"),
                     all.x = TRUE)
write.table(outfile_all, file = paste0(output_lib, "modelo_deps.csv"), sep=",", row.names = FALSE)

outfile_aux <- outfile_all[outfile_all$UnidadesVendidas.x != 0, ]
write.table(outfile_aux[outfile_aux$Semana == semanas[1], ], file = paste0(output_lib, "modelo_deps_1.csv"), sep=",", row.names = FALSE)
write.table(outfile_aux[outfile_aux$Semana == semanas[2], ], file = paste0(output_lib, "modelo_deps_2.csv"), sep=",", row.names = FALSE)

# ============ MEDICION DE ASERTIVIDAD ==========
#outfile_aux <- outfile_d

asertividad <- data.table(outfile_aux %>% group_by(SUBLINEA_ID, SUBLINEA_DESC, 
                                                   CATEGORIA_ID, CATEGORIA_DESC, 
                                                   SUBCATEGORIA_ID, SUBCATEGORIA_DESC,
                                                   MARCA_ID, MARCA_DESC,
                                                   Pluid, PLUCD, PLU_DESC,
                                                   storeid, TIENDA_DC_DESC, CADENA_DESC, dia) %>% summarise(sum_mod1 = sum(UnidadesVendidas.x),
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

