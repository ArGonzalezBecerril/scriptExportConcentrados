#!/bin/bash

# Author: Arturo Gonzalez Becerril
# Version: 0.1
# Script principal el cual llama a todos los subprocesos en bash
# Descripción: Script que transmite la informacion de la fuente de DWMB_BC_CONCENTRADO


# *********************** Si algun comando falla el script abortara inmediatamente *************************************
set -e

#**************************** FUNCIONES DE USO GENERAL *****************************************************************

function print_message(){
   echo "$1"
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
         print_message "$get_date ...$message"
    fi
}

function remove_all_files_temp(){
 IFS=';' read -ra file_temp <<< "$1"

 for i in "${file_temp[@]}"; do
    file_exist ${i}
 done
}


#####################################INICIO ############################################################################
# *****************  Validar que existan parametros ********************************************************************
if [ $# -ne 1 ]
    then
        print_message "Argumentos incompletos, es necesario el nombre de las tablas"
        exit 1
    else
        list_tables=$1
fi

#***********************************  Fichero temporales ***************************************************************

stored_directory_hdfs="/pro/workspace/crm/gestion_clientes"      # $1
processing_to_hdfs="tmp/processing_to_hdfs.scala"                # $2
exported_tables="tmp/exported_tables.txt"                        # $3
files_java="*.java"                                              # $4

# ****************** CONEXION A ORACLE (ORIGEN DE LOS DATOS) ***********************************************************
url_jdbc="jdbc:oracle:thin:@180.181.182.40:1630:MCRMACRO"        # $5
user="USRLINK"                                                   # $6
password="linkusr01"                                             # $7
table=${list_tables}                                             # $8
columns="MES_BURO,TIPO_BURO,BUC,ID_BURO,TOTAL_CUENTAS,CUENTAS_EXCELENTES,MES_APE_ANT,MES_APE_REC,MES_APE_ANT_TARJETA,MES_APE_ANT_PL,MES_APE_ANT_AUTO,MES_APE_ANT_HIPOTECARIO,MES_APE_ANT_SERVICIO,MES_APE_REC_TARJETA,MES_APE_REC_PL,MES_APE_REC_AUTO,MES_APE_REC_HIPOTECARIO,MES_APE_REC_SERVICIO,TOTAL_CUENTAS_ABIERTAS,TOTAL_CUENTAS_CERRADAS,CUENTAS_ABIERTAS_TARJETA,CUENTAS_ABIERTAS_PL,CUENTAS_ABIERTAS_AUTO,CUENTAS_ABIERTAS_HIPOTECARIO,CUENTAS_ABIERTAS_SERVICIO,CUENTAS_CERRADAS_TARJETA,CUENTAS_CERRADAS_PL,CUENTAS_CERRADAS_AUTO,CUENTAS_CERRADAS_HIPOTECARIO,CUENTAS_CERRADAS_SERVICIO,SUMA_ACTUAL_TARJETA,SUMA_ACTUAL_PL,SUMA_ACTUAL_AUTO,SUMA_ACTUAL_HIPOTECARIO,SUMA_ACTUAL_SERVICIO,SUMA_MAXIMO_SERVICIO,SUMA_MAXIMO_TARJETA,SUMA_LIMITE_TARJETA,MEJOR_LIMITE_CREDITO,SUMA_MAXIMO_PL,SUMA_MAXIMO_AUTO,SUMA_MAXIMO_HIPOTECARIO,SUMA_IMPORTE_TARJETA,SUMA_IMPORTE_PL,SUMA_IMPORTE_AUTO,SUMA_IMPORTE_HIP,SUMA_IMPORTE_SERVICIO,PEOR_MOP_12,PEOR_MOP_ACT_BC,SDO_PEOR_MOP_ACT,BC_SCORE,INDICE_CONSULTA,CUENTAS_NUEVAS_TARJETA_6MESES,CUENTAS_NUEVAS_AEXP_6MESES,CTAS_NUEVAS_SERVICIOS_6MESES,CTAS_NUEVAS_HIPOTECARIO_6MESES,CTAS_NUEVAS_AUTO_6MESES,CTAS_NUEVAS_PL_6MESES,CTAS_NUEVAS_TARJETA_12MESES,CTAS_NUEVAS_AEXP_12MESES,CTAS_NUEVAS_SERVICIOS_12MESES,CTAS_NUEVAS_HIP_12MESES,CTAS_NUEVAS_AUTO_12MESES,CTAS_NUEVAS_PL_12MESES,CONSULTAS_MES,CONSULTAS_6MESES,SUMA_VENCIDO_HIPOTECARIO,SUMA_VENCIDO_TARJETA,SUMA_VENCIDO_AUTO,SUMA_VENCIDO_SERVICIO,SUMA_VENCIDO_PL,FLG_ULTIMA_CONSULTA,PEOR_MOP_NS_12,PEOR_MOP_NS_ACT_BC,SDO_PEOR_MOP_NS_ACT"

# ******************  INFORMACION DE TABLA DESTINO EN HIVE   ***********************************************************
path_hdfs_hive="/pro/workspace/crm/cliente"                      # $10
schema_hive="sb_crm"                                             # $11
table_hive="DWMB_BC_CONCENTRADO"                                      # $12

#  ****************************** PARAMETROS DE SQOOP ******************************************************************
split_by="1"                                                     # $13 primary key
number_mappers="12"                                              # $14
data_type_columns="MES_BURO=INT,TIPO_BURO=STRING,BUC=INT,ID_BURO=INT,TOTAL_CUENTAS=STRING,CUENTAS_EXCELENTES=INT,MES_APE_ANT=INT,MES_APE_REC=INT,MES_APE_ANT_TARJETA=INT,MES_APE_ANT_PL=INT,MES_APE_ANT_AUTO=INT,MES_APE_ANT_HIPOTECARIO=INT,MES_APE_ANT_SERVICIO=INT,MES_APE_REC_TARJETA=INT,MES_APE_REC_PL=INT,MES_APE_REC_AUTO=INT,MES_APE_REC_HIPOTECARIO=INT,MES_APE_REC_SERVICIO=INT,TOTAL_CUENTAS_ABIERTAS=STRING,TOTAL_CUENTAS_CERRADAS=STRING,CUENTAS_ABIERTAS_TARJETA=INT,CUENTAS_ABIERTAS_PL=INT,CUENTAS_ABIERTAS_AUTO=INT,CUENTAS_ABIERTAS_HIPOTECARIO=INT,CUENTAS_ABIERTAS_SERVICIO=INT,CUENTAS_CERRADAS_TARJETA=INT,CUENTAS_CERRADAS_PL=INT,CUENTAS_CERRADAS_AUTO=INT,CUENTAS_CERRADAS_HIPOTECARIO=INT,CUENTAS_CERRADAS_SERVICIO=INT,SUMA_ACTUAL_TARJETA=DOUBLE,SUMA_ACTUAL_PL=DOUBLE,SUMA_ACTUAL_AUTO=DOUBLE,SUMA_ACTUAL_HIPOTECARIO=DOUBLE,SUMA_ACTUAL_SERVICIO=DOUBLE,SUMA_MAXIMO_SERVICIO=DOUBLE,SUMA_MAXIMO_TARJETA=DOUBLE,SUMA_LIMITE_TARJETA=DOUBLE,MEJOR_LIMITE_CREDITO=DOUBLE,SUMA_MAXIMO_PL=DOUBLE,SUMA_MAXIMO_AUTO=DOUBLE,SUMA_MAXIMO_HIPOTECARIO=DOUBLE,SUMA_IMPORTE_TARJETA=DOUBLE,SUMA_IMPORTE_PL=DOUBLE,SUMA_IMPORTE_AUTO=DOUBLE,SUMA_IMPORTE_HIP=DOUBLE,SUMA_IMPORTE_SERVICIO=DOUBLE,PEOR_MOP_12=INT,PEOR_MOP_ACT_BC=INT,SDO_PEOR_MOP_ACT=DOUBLE,BC_SCORE =INT,INDICE_CONSULTA=INT,CUENTAS_NUEVAS_TARJETA_6MESES=INT,CUENTAS_NUEVAS_AEXP_6MESES=INT,CTAS_NUEVAS_SERVICIOS_6MESES=INT,CTAS_NUEVAS_HIPOTECARIO_6MESES =INT,CTAS_NUEVAS_AUTO_6MESES=INT,CTAS_NUEVAS_PL_6MESES=INT,CTAS_NUEVAS_TARJETA_12MESES=INT,CTAS_NUEVAS_AEXP_12MESES=INT,CTAS_NUEVAS_SERVICIOS_12MESES=INT,CTAS_NUEVAS_HIP_12MESES=INT,CTAS_NUEVAS_AUTO_12MESES=INT,CTAS_NUEVAS_PL_12MESES=INT,CONSULTAS_MES=INT,CONSULTAS_6MESES=INT,SUMA_VENCIDO_HIPOTECARIO=DOUBLE,SUMA_VENCIDO_TARJETA=STRING,SUMA_VENCIDO_AUTO=DOUBLE,SUMA_VENCIDO_SERVICIO=STRING,SUMA_VENCIDO_PL=DOUBLE,FLG_ULTIMA_CONSULTA=INT,PEOR_MOP_NS_12=INT,PEOR_MOP_NS_ACT_BC=INT,SDO_PEOR_MOP_NS_ACT=DOUBLE"
                                                                 # $15
# ************************* CONEXION A ORACLE (DESTINO DE LOS DATOS) ***************************************************

user_dest="usrstg"                                               # $16
driver="oracle.jdbc.driver.OracleDriver"                         # $17
password_dest="Secreta01"                                        # $18
url_jdbc_dest="jdbc:oracle:thin:@180.181.169.83:1660:pdmanltc"   # $19
table_dest="DWMB_BC_CONCENTRADO"                                      # $20

# **************************** PATH DEL SHELL DE SPARK Y LIBRERIAS NESESARIAS ******************************************
shell_spark='./sh/spark-shell.sh'
library_hive='scala/header.scala'

################################# EXPORTAR LA TABLA ORACLE A HDFS      #################################################

./sh/export_oracle_to_hdfs.sh ${stored_directory_hdfs} ${processing_to_hdfs} ${exported_tables} ${files_java} ${url_jdbc} ${user} ${password} ${table} ${columns} ${path_hdfs_hive} ${schema_hive} ${table_hive} ${split_by} ${number_mappers} ${data_type_columns}


############## GENERAR EL CODIGO PARA ALMACENAR EN HDFS Y TAMBIEN PARA ENVIAR A ORACLE NUEVAMENTE #####################

#***Este script genera el codigo de ${processing_to_hdfs} el cual sera ejecutado por un shell de spark**
sh/save_hdfs_and_write_jdbc.sh ${table} ${schema_hive} ${stored_directory_hdfs} ${processing_to_hdfs} ${exported_tables} ${user_dest} ${driver} ${password_dest} ${url_jdbc_dest} ${table_dest} ${table_hive}

##########################  INVOCAR EL SHELL DE SPARK PARA ENVIAR LOS DATOS DE HDFS A ORACLE. ##########################

cat scala/header.scala ${processing_to_hdfs} | ./sh/spark-shell.sh

########### ELIMINAR LA TABLA TEMPORAL EN HIVE #########################################################################

./sh/drop_table_hive.sh ${schema_hive}"."${table_hive}

#########################  Remover ficheros temporales        #########################################################

remove_all_files_temp "${processing_to_hdfs};${exported_tables};${files_java}"
print_message "Script finalizado"





