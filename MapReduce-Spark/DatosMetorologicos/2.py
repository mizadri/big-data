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
if len(sys.argv) != 3:
    print ("Se necesitan 2 ficheros!")
    exit(-1)

# Creamos un contexto local y cargamos los ficheros
sc = SparkContext(master="local")
firstFile = sc.textFile(sys.argv[1])
SecondFile = sc.textFile(sys.argv[2])

# Unimos el contenido de los ficheros en un solo RDD
lines = firstFile.union(SecondFile)

# GETPAIRS recibe una lista con los campos ya separados y devuelve un pareja (fecha, bateria)
def GetPairs(list):
    formatDate = list[0].split("/")[1] + "/" + list[0].split("/")[0]
    return (formatDate, float(list[8]))

# Dividimos los campos, filtramos la cabecera y aplicamos la funcion GETPAIRS
pairs = lines.map(lambda x: x.split(',')) \
                .filter(lambda x: x[0]!='date-time') \
                .map(GetPairs)

# Obtenemos los maximos, minimos y las medias de cada fecha en forma de diccionario
res = pairs.groupByKey() \
            .map(lambda x: (x[0], {'max':max(x[1]), 'avg': round(sum(x[1])/len(x[1]),2), 'min':min(x[1])})) \
            .collect()

# Mostramos por pantalla
for pair in res:
    print (str(pair[0]) + '\t\t' + str(pair[1]))

sc.stop()
