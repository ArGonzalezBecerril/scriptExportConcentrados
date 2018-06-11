#!/bin/bash

#Author  : Arturo González Becerril
#Version : 0.1

# Objetivo del script
# Invocar un shell de spark ya que mediante jdbc exporta un dataframe a una base de datos oracle
#
bash spark-shell --master yarn --executor-memory 12G  --jars lib/spark-csv_2.10-1.5.0.jar,lib/univocity-parsers-2.1.2.jar,lib/commons-csv-1.4.jar,lib/ojdbc7-12.1.0.2.jar --conf spark.ui.showConsoleProgress=true --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops -XX:+UseG1GC"
