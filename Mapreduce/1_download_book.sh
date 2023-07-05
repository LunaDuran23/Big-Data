#! /bin/bash

#archivo sh para cargar los libros
#antes de iniciar cree una carpeta vacia en el mismo directorio donde esta llamada 'input'
#----------------------------------------------------------------------------------------------------------
file=$1  #leer archivo sh
count=1 #contador para nombrar cada archivo
for li in $(<$file); do #leer linea por linea el archivo
    #carpeta input
    wget "$li" -O input/libro$count.txt #descargamos los archivos y guardamos como libros_luna(count).txt
    let count++ #sumamos uno al contador
done < $file

#para correr este archivo en la terminal usar los comandos:
#"libros_luna.txt" es el archivo de texto con los links de los libros
#> ./1_download_book.sh libros_luna.txt
