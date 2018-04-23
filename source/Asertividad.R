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
validation_data$Semana <- week(as.Date(validation_data$Fecha))
validation_data$dia <- weekdays(as.Date(validation_data$Fecha))

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

