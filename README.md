# Script "Oracle - Lago(HDFS) - Oracle"
Descripción: El siguiente script tiene como finalidad exportar una o n tablas con el mismo esquema de Oracle - Lago - Oracle

[![N|Solid](https://trello-attachments.s3.amazonaws.com/5a1baed8137a75335503cc3a/5afcd584aa5675f32966baf0/9aedf7c3b929d88d5eafa31d6a7aa0ad/oracletoHdsf.PNG)](https://nodesource.com/products/nsolid)

## Directorios

- **lib:** Librerías necesarias para la ejecución del script
- **logs:** Log que genera el script en cada ejecución tomando como base la fecha de su ejecución, en caso de que el script se invoque más de una vez al día se agregara en el mismo archivo el trace del script
- **scala:** Códigos en scala necesarios para el script, para esta versión solo se agregan algunos imports para crear el contexto en Hive.
- **sh:** Módulos escritos en bash el cual realizan tareas pequeñas como eliminar una tabla en hive o invocar un Shell de spark cada uno de estos programas son invocados por el script principal.

### Especificaciones para hacer uso del script
**1** Antes de ejecutar el script  es necesario configurar los siguientes parámetros de conexión a oracle: (**Todas las variables se encuentran dentro el fichero run.sh**)
- ***Url_JDBC*** Se encuentra como **url_jbc**  debajo del comentario **"Origen de datos"**
- ***Usuario*** Usuario de la base de datos oracle(Nombre de la variable es **user**)
- ***Password*** Constraseña del usuario(Nombre de la variable es **password**)
- ***Tabla*** Nombre de la tabla que se va a exportar a HDFS(Nombre de la variable es **table**)
- ***Columnas*** Nombre de las columnas que se van a exportar, cada una debe ir separada por comas: Columna1, Columna2...(Nombre de la variable es **columns**)
##### **Información de ¿Cómo almacenarlo en Hive?**
Es necesario indicar al script como se almacenara en HDFS la tabla a exportar por ello se debe indicar 3 puntos importantes
- ***Nombre de la tabla destino:*** Por default tendrá el mismo nombre de la tabla origen, pero si se desea cambiar el nombre  la variable se llama ***table_hive***
- ***Esquema:*** Es importarte indicar el esquema donde se van almacenar las tablas por default es ***sb_crm***)
- ***Directorio HDFS:*** Es el directorio en HDFS donde se depositara la tabla, el nombre de la variable es ***path_hdfs_hive***)

##### **Parámetros adicionales en sqoop**

- ***Split by:*** Se usa para especificar la columna de la tabla utilizada para generar nuevas filas.
- ***Mappers:*** Numero de Mappers que serán lanzados en el proceso de map-reduce
- ***DataType columns:*** Tipo de dato de cada columna

**2** Clonar el repositorio o descargar las fuentes en formato zip
```sh
# Pasos para ejecutar el script **[Run.sh]**
arturo@arturo$ git clone https://github.com/ArturoGonzalezBecerril/scriptOracleTableToHDFS.git
arturo@arturo$ ls -lht
drwxr-xr-x 2 xxxxx domain arturo 4.0K Jun  6 18:52 sh
drwxr-xr-x 2 xxxxx domain arturo 4.0K Jun  6 18:52 scala
-rwxr-xr-x 1 xxxxx domain arturo  14K Jun  6 18:52 run.sh
-rw-r--r-- 1 xxxxx domain arturo 1.6K Jun  6 18:52 README.md
drwxr-xr-x 2 xxxxx domain arturo 4.0K Jun  6 18:52 lib
drwxr-xr-x 2 xxxxx domain arturo 4.0K Jun  7 12:37 tmp
# El script principal requiere 1 parametro el cual es el nombre de la tabla
arturo@arturo$ ./run.sh nombreDeLaTabla
# Si son n tablas, pueden ir separadas por comas
arturo@arturo$ ./run.sh tabla1,tabla2,tabla3
```


> **Instalación**
> Asegurarse de tener los permisos de ejecución,
> El script no nesesita de modulos adicionales ya que es out-of-the-box

Licencia
**BSD**
Mayo 23 2018