#!/bin/bash

# Version 0.1
# Author:Arturo Gonzalez Becerril

# *******************************     Descripcion      *****************************************************************
# Este script crea codigo en scala con base en los parametros que se le pasa como argumento para
# migrar la informacion de oracle a hdfs, posteriormente se pasara este fichero a un  shell de spark
#
# Parametros
# - $1 Nombre de la tabla que se va a exportar
# - $2 Esquema en hive donde se almacenara la tabla exportada
# - $3 Directorio HDFS donde se almacenara la informacion final procesada
# - $4 Nombre del fichero que contendra el codigo en scala a ejecutar en un shell de spark
# - $5 Este fichero contiene el registro de las tablas que se exportaron a hdfs.
# - $6 Usuario de la Base de datos oracle Destino
# - $7 Drive jdbc en este caso es el drive de oracle
# - $8 Contraseña del Usuario
# - $9 URL jdbc
# - $10Nombre de la tabla en oracle donde se va a depositar la información

# Objetivo del Script:
# Es necesario ya que construye un fichero que contiene la logica para leer la tabla y almacenarla particionada por fecha
# en hadoop adicionalmente se integra la logica para transimitir la informacion a oracle.

#################################################################
# Actualizacion:                                                #
#                                                               #
# Para esta version se desactiva la linea que almacena en HDFS  #
#################################################################


function print_message(){
   get_date=$(date +"%Y-%m-%d %H:%M:%S,%3N")
   echo "$get_date $1"
}


 if [ $# -ne 11 ]
   then
     print_message "El numero de parametros es incorrecto"
     exit 1
   else
     table=$1
     schema_hive=$2
     stored_directory_hdfs=$3
     processing_to_hdfs=$4
     tables_exported=$5

     user_dest=${6}
     driver=${7}
     password_dest=${8}
     url_jdbc_dest=${9}
     table_dest=${10}
     table_hive=${11}
     touch ${processing_to_hdfs} && touch ${tables_exported}

     # Logica para almacenar en HDFS
     echo "println(\" Obteniendo la tabla  ${table_hive} desde Hive\")" >> ${processing_to_hdfs}
     echo "val cliente = hiveContext.sql(\"select * from $schema_hive"."${table_hive}\")" >> ${processing_to_hdfs}
     echo "//cliente.toDF().selectExpr(\"*\",\"substring(id_mes,0,4) as year\", \"substring(id_mes,5,2) as month\").repartition(10).write.mode(SaveMode.Append).partitionBy(\"year\",\"month\").mode(SaveMode.Append).parquet(\"$stored_directory_hdfs\")" >> $processing_to_hdfs

     # Logica para exportar de HDFS a oracle
     echo "println(\"Estableciendo la conexion a Oracle para transmitir la informacion\")" >> ${processing_to_hdfs}
     echo "val prop = new java.util.Properties" >> ${processing_to_hdfs}
     echo "prop.setProperty(\"user\", \"${user_dest}\")" >> ${processing_to_hdfs}
     echo "prop.setProperty(\"driver\", \"${driver}\")" >> ${processing_to_hdfs}
     echo "prop.setProperty(\"password\", \"${password_dest}\")" >> ${processing_to_hdfs}
     echo "val url = \"${url_jdbc_dest}\"" >> ${processing_to_hdfs}
     echo "val table = \"${table_dest}\"" >> ${processing_to_hdfs}

     echo "println(\"Guardando la informacion por medio de jdbc a oracle:\")" >> ${processing_to_hdfs}
     echo "cliente.write.mode(\"append\").jdbc(url,table,prop)" >> ${processing_to_hdfs}
     echo "val dirHDFSCliente:String=\"$stored_directory_hdfs\"" >> ${processing_to_hdfs}
     #Add tables oracle will by remove at the end Script
     echo "${schema_hive}"."${table}"  >> ${tables_exported}
 fi
