
#WriteViaSSH
# Funci�n para transmitir un archivo desde un servidor al otro
WriteViaSSH <- function (infile = "Gondola/Modelo_tendencia_r/tendencia.csv",             # Archivo (con su respectiva ruta) que se desea transmitir
                         server = "10.2.113.138:",                                        # Direcci�n IP del servidor
                         user = "hdp_agotadoln",                                          # Usuario del servidor
                         pwd = "hadoop",                                                  # Constrase�a del servidor
                         outfile = "/data/LZ/Agotados/Datos/tendencia.csv"){              # Ruta y nombre del archivo que se escribir� en el servidor receptor
  
  str <- paste0("sshpass -p '", pwd, "' scp ", infile, " ", user, "@", server, outfile)
  system(str)
  #system('sshpass -p "hadoop" scp Gondola/Modelo_tendencia_r/tendencia.csv hdp_agotadoln@10.2.113.138:/data/LZ/Agotados/Datos/tendencia.csv')
  #ftpUpload("Gondola/Modelo_tendencia_r/tendencia.csv", "ftp://cargas:cargas@10.2.113.107//home/dwh/datos/prueba.txt")
}
