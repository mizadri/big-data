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

# Esta clase sirve para implentar las funciones requeridas para definir un acumulador personalizado de Strings(elemento inicial y acumular)
class StringAcccumulator():
    def zero(self, initialValue):
        return ""

    def addInPlace(self, v1, v2):
        v1 += v2 + '\n'
        return v1

# Inicializar contexto de spark y cargar el archivo de log a un rdd de lineas
sc = SparkContext(master="local")
lines = sc.textFile("access_log_Jul95")

# Inicializar un acumulador en el spark context con nuestra clase personalizada que acumula Strings
errors = sc.accumulator("",StringAcccumulator())

'''
 Esta funcion toma una linea y la procesa para devoler una tupla (host , (1, response_size, is_error)), 
 is_error solo vale 1 si el codigo de respuesta es 4xx o 5xx.
 El formato de la segunda posicion de la tupla permite que luego se pueda agrupar por host y sumar las posiciones 
 para tener la informacion requerida en la solucion (numero de peticiones, tamano de respuestas, numero de errores).
'''
def getHost(line):		
	host = line.split(" - - ")[0]
	frags = line.split(" ")
	frag_len = len(frags)

	size_str = frags[frag_len-1]
	response_code = int(frags[frag_len-2])
	response_size = 0

	if(size_str != '-'):
		response_size = int(size_str)

	is_error = 0
	if response_code >= 400 and response_code < 600:
		is_error = 1

	return (host, (1, response_size, is_error))

# Esta funcion filtra las lineas que no tengan los campos necesarios para considerarse una linea valida 
# Debe tener host, y que los ultimos dos strings de la linea sean el tamano de respuesta(int) y el codigo(int||'-').
def checkFormat(line):
	global errors
	correct = False
	host = line.split(" - - ")[0]
	frags = line.split(" ")
	frag_len = len(frags)
	size_str = frags[frag_len-1]
	code_str = frags[frag_len-2]

	if host != None and (size_str.isdigit() or size_str == '-') and code_str.isdigit():
		correct = True
	else:
		errors.add(line)

	return correct

# Primero se aplican las funciones filter y map (para parsear las lineas y obtener el formato que permite calcular el resultado).
# Tras esto se hace un reduceByKey, que aplica la suma de forma independiente a cada posicion para calcular el resultado: host, (numero peticiones, tam_peticiones, numero errores)
result = lines.filter(checkFormat)\
				.map(getHost)\
				.reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1],x[2]+y[2]))

output = result.collect()
for host, tupla in output:
	print( host + " " + str(tupla) ) 
print("\n List of errors: \n" + errors.value)
sc.stop()
