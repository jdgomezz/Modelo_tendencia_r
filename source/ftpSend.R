library(RCurl)
ftpUpload("Gondola/Modelo_tendencia_r/tendencia.csv", "ftp://svilla:svilla01@10.2.113.138:22//home/svilla/prueba.txt")
ftpUpload("Gondola/Modelo_tendencia_r/tendencia.csv", "ftp://cargas:cargas@10.2.113.107//home/dwh/datos/prueba.txt")

system('sshpass -p "hadoop" scp Gondola/Modelo_tendencia_r/tendencia.csv hdp_agotadoln@10.2.113.138:/data/LZ/Agotados/Datos/tendencia.csv')