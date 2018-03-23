# Extracción de venta histórica desde teradata
# Realizado por: Jesús David Gómez Zuluaga
# Grupo Exito -- Última actualización: Febrero 20 de 2018
#
# Descripción: El presente script extrae la venta histórica en unidades
# a nivel de plu/dep/fecha/hora considerando diversos filtros de negocios
# tales como la vrentana de tiempo a extraer (Fecha inicial, fecha final)
# Número m�?nimo de registros de venta a considerar, Fecha de última venta,
# tiempo de vida del producto, como también frecuencia de venta ()
  
  # LoadXdf: La presente función extrae datas directamente desde Teradata y escribe un archivo con formato xdf
  
  LoadXdf <-function(server, uid, pwd, file, filename, booleano, pars){
    query <- readLines(file)
    query <- paste(query, collapse='\n')
    
    # LIMPIEZA DE DATOS
    
    # Definición de filtros:
    
    query <- gsub("&dep.", pars[1], query);                           # Dependencia(s)
    query <- gsub("&fi", pars[2], query);                 # Fecha inicial
    query <- gsub("&ff", pars[3], query);                 # Fecha final
    query <- gsub("&cut_registros.", pars[4], query);                  # Número m�?nimo de registro por plu-dep
    query <- gsub("&cut_ultima_venta", pars[5], query);   # Fecha de última venta realizada
    query <- gsub("&cut_tiempo_vida.", pars[6], query);               # Tiempo de vida m�?nimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
    query <- gsub("&cut_proporcion.", pars[7], query);                 # Proporción de venta m�?nima (Nro. registros)/(Tiempo de vida)
    query <- gsub("&n_deciles.", pars[8], query);                    # Número de deciles a segmentar la base de datos 
    
    
    ti <- Sys.time()
    ventas <- td_Xdf(server = server, uid = uid, pwd = pwd, query = query, name = filename, booleano = booleano);
    tf <- Sys.time()
    dt <- (tf - ti)
    print(dt)
    return(ventas)
  }
  
  Load_data <-function(file){
    query <- readLines(file)
    query <- paste(query, collapse='\n')
    
    # LIMPIEZA DE DATOS
    
    # Definición de filtros:
    
    query <- gsub("&dep.", 35, query);                           # Dependencia(s)
    query <- gsub("&fi", "'2017-01-01'", query);                 # Fecha inicial
    query <- gsub("&ff", "'2018-02-20'", query);                 # Fecha final
    query <- gsub("&cut_registros.", 0, query);                  # Número m�?nimo de registro por plu-dep
    query <- gsub("&cut_ultima_venta", "'2017-11-01'", query);   # Fecha de última venta realizada
    query <- gsub("&cut_tiempo_vida.", 30, query);               # Tiempo de vida m�?nimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
    query <- gsub("&cut_proporcion.", 1, query);                 # Proporción de venta m�?nima (Nro. registros)/(Tiempo de vida)
    query <- gsub("&n_deciles.", 100, query);                    # Número de deciles a segmentar la base de datos 
    
    ti <- Sys.time()
    ventas <- td(query = query);
    tf <- Sys.time()
    dt <- (tf - ti)
    print(dt)
    return(ventas)
  }
  
  Load_data_par <- function(npart = 20,
                            nodes = 8, 
                            params,
                            querydep = "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL", 
                            query,
                            server = "10.2.113.66", 
                            user = "jdgomezz",
                            pwd = "jdgomezz01", 
                            path = "xdf/ventas"){

    fi = pars[1]                # Fecha inicial
    ff = pars[2]                # Fecha final
    cut_registros = pars[3]     # Número m�?nimo de registro por plu-dep
    cut_ultima_venta = pars[4]  # Fecha de última venta realizada
    cut_tiempo_vida = pars[5]   # Tiempo de vida m�?nimo por plu-dep ABS(Fecha 1ra venta - Fecha última venta)
    cut_proporcion = pars[6]    # Proporción de venta m�?nima (Nro. registros)/(Tiempo de vida)
    n_deciles = pars[7]

    #querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL"
    #connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
    connectionString <-paste0("Driver=Teradata;DBCNAME=", server, ";UID=", user, ";PWD=", pwd, ";")
    
    odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
    dep <- rxImport(odbcDS)$DependenciaCD
    delta <- round(length(dep)/npart)
    
    cl <-makeCluster(nodes)
    registerDoParallel(cl)
    system.time( 
      result <- foreach (i = 0:(npart-1), .combine='cbind', .export = c('LoadXdf')) %dopar% {
        deptemp <- dep[(1+i*delta):(1+(i+1)*delta)]
        deptemp <- deptemp[!is.na(deptemp)]
        deptemp <- paste(deptemp, collapse = ', ')
        pars <- c(deptemp, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
        venta <- LoadXdf(server, user, pwd, query, paste0(path, '_',i,'.xdf'), booleano, pars)
        print(paste0('Finalizado iteracion ',i))
      })
    
    # Instrucci�n para realizar el merge entre todas las tablas 
    # output <-rxMerge(inData = xdf1, inData2 = xdf2, type = "union")
    return(output)
  }
  
  Load_csv <- function(file){
    ti <- Sys.time()
    
    x <- read.csv(file, header = TRUE, sep = ",")
    
    tf <- Sys.time()
    dt <- (tf - ti)
    print(dt)
    
    return(x)
  }
  
  # Load_xdf: La presente función extrae datos de un archivo csv o de tipo data.frame y los convierte en un dataset del tipo
  # .xdf, el cual tiene la ventaja que trabaja con formato tidy-data y se guarda en disco, no en memoria.
  Load_xdf_csv <- function(file, outfile, boolean){
    ti <- Sys.time()
    
    dataColClasses <- c(DependenciaCD = "factor", SubZonaCD = "factor", CiudadCD = "factor", Pluid = "factor", PluCD = "factor",           
                        ArticuloID = "factor", SubCategoriaCD = "factor", CategoriaCD = "factor", SubLineaCD = "factor", SubDireccionCD = "factor", 
                        DireccionCD = "factor", Fecha = "Date", dia = "factor", semana = "factor", mes = "factor",             
                        anio = "factor", Hora = "factor", ClasificacionABC = "factor", SaleFactor = "integer", UnidadesVendidas = "float32",
                        undsReales = "float32", unidadesAcum = "float32", undsAdim = "float32", corte = "float32")
    
    x <- rxImport(inData = file, outFile = outfile, overwrite = TRUE, colClasses= dataColClasses)
    if (boolean == TRUE){
      file.remove( outfile )
    } 
    
    tf <- Sys.time()
    dt <- (tf - ti)
    print(dt)
    # rxSummary(formula = ~dia:UnidadesVendidas, data = x)            función para crear resumen    
    return(x)
  }
  
  