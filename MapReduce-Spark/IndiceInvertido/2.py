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

# Creamos un contexto local y cargamos el fichero        
sc = SparkContext(master="local")

# Cargamos todos los ficheros de la carpeta books en el directorio actual
lines = sc.wholeTextFiles("books")

# getTuplesFileLines recibe una tupla de cadenas (path, texto) con la ruta al fichero leido y
# todo el texto que contiene. Devuelve una lista de tuplas con el nombre del archivo
# (sin la ruta completa) y cada una de sus lineas.
def getTuplesFileLines(tuple):
    tuples = []
    # Extraemos el nombre del fichero de la ruta.
    file = tuple[0].split('/')[len(tuple[0].split('/')) - 1]
    for line in tuple[1].split('\n'):
        tuples.append((file, line))
    return tuples

# getTuplesWordFileCounter recibe una tupla (fichero, lineas) con el nombre del fichero y
# cada una de sus lineas. Devuelve una lista de tuplas (palabra:archivo, contador) con
# cada palabra de la linea y el nombre del archivo unidos por dos puntos y un contador de apariciones.
def getTuplesWordFileCounter(tuple):
    tuples = []
    # Eliminamos los caracteres especiales salvo que sean espacios en blanco o comillas simples.
    words = ''.join(e for e in tuple[1] if e.isalpha() or e == ' ' or e == "'")
    for word in words.lower().split():
        tuples.append(((word+":"+tuple[0]), 1))
    return tuples

# getTuplesWordFileCounter recibe una tupla (palabra, lista) con una palabra y una lista de tuplas de
# libros en los que aparece y el numero de veces que aparece en ellos. Devuelve si la palabra aparece
# mas de veinte veces en algunos de los libros de su lista.
def greaterThan20(tuple):
    veinte = False
    for book in tuple[1]:
        if (book[1] > 20):
            veinte = True
    return veinte

# getTuplesWordFile recibe una tupla (palabra:archivo, contador) con cada palabra de la linea
# y el nombre del archivo unidos por dos puntos y un contador de apariciones. Devuelve una tupla
# (palabra, (libro, apariciones)) con la palabra y otra una tupla con el libro en el que aparece y el
# numero de apariciones.
def getTuplesWordFile(tuple):
    tag = tuple[0].split(':')
    return (tag[0], (tag[1],tuple[1]))

# sortList recibe una tupla (palabra, lista) con una palabra y una lista de tuplas de
# libros en los que aparece y el numero de veces que aparece en ellos. Devuelve la misma tupla
# pero ordenadando los libros por numero de apareciones.
def sortList(tuple):
    return (tuple[0], sorted(tuple[1], key=lambda tup: tup[1], reverse=True))

# Aplanamos las lineas del texto para conseguir tuplas de palabras y apariciones.
wordsWithFiles = lines.flatMap(getTuplesFileLines)\
            .flatMap(getTuplesWordFileCounter)

# Sumamos las apariciones de las palabras en cada libro, obtenemos tuplas con la palabra y el libro
# en el que aparece y agrupamos por la palabra.
countWords = wordsWithFiles.reduceByKey(lambda x,y: x + y)\
            .map(getTuplesWordFile)\
            .groupByKey()

# Filtramos aquellas tuplas que sean no aparezcan mas de 20 veces en algun libro
filterCountWords = countWords.filter(greaterThan20)

# Ordenamos la lista de libros de cada palabra por numero de apariciones.
orderedCountWords = filterCountWords.map(sortList)

# En lugar de almacenar en disco, recolectamos y mostramos por pantalla
result = orderedCountWords.collect()
for tuple in result:
    infoBooks = ', '.join(str((book,num)) for book,num in tuple[1])
    print (str(tuple[0]) + ' \t ' + str(infoBooks))

sc.stop()
