'''
    # Asignatura: Sistemas de Gestion de Datos y de la Informacion
    # Practica 1 (MapReduce y ApacheSpark)
    # Grupo 10 (Adrian Garcia y Frank Julca)
    # Declaracion de integridad: Adrian Garcia y Frank Julca
    declaramos que esta solucion es fruto exclusivamente de nuestro trabajo personal.
    No hemos sido ayudados por ninguna otra persona ni hemos obtenido la solucion de fuentes externas,
    y tampoco hemos compartido nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
    deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demas.
'''

import sys
from pyspark import SparkContext

# sys.argv debe contener el nombre de fichero a procesar
if len(sys.argv) != 2:
  print ("Falta el fichero!")
  exit(-1)

# Creamos un contexto local y cargamos el fichero
sc = SparkContext(master="local")
lines = sc.textFile(sys.argv[1])

# Dividimos los campos por tabulaciones
fields = lines.map(lambda x: x.split('\t'))

# Filtramos por aquellos que tengan Twitter Rank y felicidad media menor que 2
filteredWords = fields.filter(lambda x: x[4] != '--' and float(x[2])<2)

# Creamos parejas (palabra, felicidad) y sacamos las 5 con mayor media
# Usamos una funcion anonima en el parametro key de takeOrdered para sacar los ultimos 5 valores
orderedTuples = filteredWords.map(lambda x: (x[0],float(x[2]))).takeOrdered(5, key= lambda x: -x[1])

# Mostramos por pantalla
for tuple in orderedTuples:
	print (str(tuple[0]) + '\t' + str(tuple[1]))
sc.stop()
