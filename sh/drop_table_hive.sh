#!/bin/bash

#Author : Arturo Gonzalez Becerril
#Version: 0.1

#Objetivo del script       : Eliminar una tabla en hive
#Parametros que recibe     : Nombre de la tabla
#Forma de invocar el script: ./drop_table_hive.sh nombreDeLaTabla


function print_message_highlight(){
   verde="\e[92m"
   default="\e[39m"
   echo "${verde}${1}${default}"
}



if output=$(hive -e "drop table $1"); then
    print_message_highlight "Eliminando la tabla temporal en hive"
    echo "Salida: $output"
else
    echo
    print_message_highlight "Fallo al lanzar el comando, $output"
fi

