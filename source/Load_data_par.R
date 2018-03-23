querydep <- "SELECT * FROM bd_ddpo.vtdependencia where FechaCierre IS NULL"
connectionString <-"Driver=Teradata;DBCNAME=10.2.113.66;UID=jdgomezz;PWD=jdgomezz01;"
odbcDS <-RxOdbcData(sqlQuery = querydep,connectionString = connectionString)
dep <- rxImport(odbcDS)$DependenciaCD
npart <- 20
delta <- round(length(dep)/npart)

library(doParallel)
#registerDoParallel(cores=300)
cl <-makeCluster(8)
registerDoParallel(cl)
system.time( 
result <- foreach (i = 0:(npart-1), .combine='cbind', .export = c('LoadXdf')) %dopar% {
  deptemp <- dep[(1+i*delta):(1+(i+1)*delta)]
  deptemp <- deptemp[!is.na(deptemp)]
  deptemp <- paste(deptemp, collapse = ', ')
  pars <- c(deptemp, fi, ff, cut_registros, cut_ultima_venta, cut_tiempo_vida, cut_proporcion, n_deciles)
  
  
  venta <- LoadXdf(file, paste0('xdf/ventas_',i,'.xdf'), booleano, pars)
  print(paste0('Finalizado iteracion ',i))
})

results <- foreach(i=1:n, .combine='cbind', .export=c('function1', 'function2'), .packages='package1') %dopar% {
  # do something cool
}
  