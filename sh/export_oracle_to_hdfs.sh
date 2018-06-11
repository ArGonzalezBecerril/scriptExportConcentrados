#!/bin/bash

#Author: Arturo Gonzalez Becerril
#Version: 0.1
#Parametros
#
#  $1   Directorio HDFS donde se almacenaran los datos
#  $2   Fichero .scala que se construye con base en los parametros que se le pasa como argumentos, este fichero lo tomara un
#       shell de spark y ejecutara cada linea de código que tenga.
#  $3   Fichero temporal que contiene una bitacora de las tablas que se estan exportando a HDFS
#  $4   Es un bean de java que automaticamente crea sqoop para mappear la tabla de oracle a una clase java.
#  $5   Url JDBC de la conexion origen en oracle
#  $6   Usuario de la base de datos oracle
#  $7   Contraseña del usuario
#  $8   Tabla que se va a exportar
#  $9   Columnas de la tabla
#  $10  Directorio HDFS en hive donde se almacenara la información
#  $11  Esquema donde se depositara la tabla hive
#  $12  Nombre de la tabla destino en hive(Por defecto tendra la misma que el de origen en oracle)
#  $13  Se usa para especificar la columna de la tabla utilizada para generar nuevas filas
#  $14  Numero de Mappers que serán lanzados en el proceso de map-reduce
#  $15  Tipo de dato de cada columna

#Objetivo del script: Exportar una tabla de oracle a HDFS(Hive)

############################ Funciones de uso general #################################################################

function print_message(){
   echo "${1}"
}


function remove_file(){
     output_command=$(rm $1)
}

function file_exist(){
    get_date=$(date +"%Y-%m-%d %H:%M:%S,%3N")
    message=""
    if [ -e $1 ]
     then
         message=$(remove_file $1)
         print_message "$get_date Removiendo ficheros temporales '$1' $message"
     else
         print_message "$get_date Eliminando ficheros temporales ...$message"
    fi
}

function remove_all_files_temp(){
 IFS=';' read -ra file_temp <<< "$1"

 for i in "${file_temp[@]}"; do
    file_exist $i
 done
}

################################### Ficheros temporales  ###############################################################
stored_directory_hdfs=${1}
partition_save_file_tmp=${2}
tables_document_oracle_tmp=${3}
files_java=${4}

####################################  Declaración de variables  ########################################################
url_jdbc=${5}
user=${6}
password=${7}
table=${8}
columns=${9}

# ************* INFORMACION DE DESTINO TEMPORAL EN HIVE HDFS ***********************************************************
path_hdfs_hive=${10}
schema_hive=${11}
table_hive=${12}

#  ****************************** PARAMETROS PROPIOS DE SQOOP **********************************************************
split_by=${13}
number_mappers=${14}
data_type_columns=${15}


#********* SCRIPT EN SQOOP PARA IMPORTAR DATOS DE ORACLE A UNA TABLA HIVE **********************************************
sqoop import --connect ${url_jdbc} --username ${user} --password ${password} --table ${table} --columns ${columns} --map-column-hive ${data_type_columns} --warehouse-dir ${path_hdfs_hive} --m ${split_by} --hive-import --hive-table ${schema_hive}"."${table} --num-mappers ${number_mappers} --compression-codec org.apache.hadoop.io.compress.SnappyCodec


# **********************     ELIMINAR FICHEROS TEMPORALES, SI ES QUE EXISTEN *******************************************
remove_all_files_temp "${partition_save_file_tmp};${tables_document_oracle_tmp};${files_java}"
