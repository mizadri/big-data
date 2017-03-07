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
from mrjob.job import MRJob

class MRLog(MRJob):

	'''
	El mapper recibe una linea y la procesa para devoler una tupla con el siguiente formato(si no falta informacion):  (host , (1, response_size, is_error)), 
	 El formato de la segunda posicion de la tupla permite que luego se pueda agrupar por host y sumar las posiciones 
	 para tener la informacion requerida en la solucion (numero de peticiones, tamano de respuestas, numero de errores)
	 is_error solo vale 1 si el codigo de respuesta es 4xx o 5xx.
	'''
	def mapper(self, key, line):
		host = line.split(" - - ")[0]
		frags = line.split(" ")
		frag_len = len(frags)
		size_str = frags[frag_len-1]
		code_str = frags[frag_len-2]

		if host != None and (size_str.isdigit() or size_str == '-') and code_str.isdigit():
			
			is_error = 0
			response_code = int(code_str)
			if response_code >= 400 and response_code < 600:
				is_error = 1
	
			response_size = 0
			if size_str != '-':
				response_size = int(size_str)

			yield (host, (1, response_size, is_error))

	'''
	El combiner recibe para cada host(key) una lista en values con la informacion de cada peticion realizada por ese host(que hay en ese chunk): (1, response_size, is_error)
	Al sumar la primera posicion se obtienen el numero de peticiones, con la segunda el tam. total de las respuestas y con la tercera el numero de errores
	Esta funcion devuelve a su vez una tupla con el mismo formato pero que contiene la informacion agregada para cada host de forma local a un nodo
	'''
	def combiner(self, key, values):
		npetitions = 0
		nbytes = 0
		nerrors = 0

		for stats in values:
			npetitions += stats[0]
			nbytes += stats[1]
			nerrors += stats[2]

		yield (key, (npetitions, nbytes, nerrors) )

	'''
	El reducer recibe para cada host(key) una lista en values con la informacion de [numero peticiones, tam_respuestas, numero errores] que se ha calculado en la fase 
	combiner de cada nodo local. 
	'''
	def reducer(self, key, values):
		npetitions = 0
		nbytes = 0
		nerrors = 0

		for stats in values:
			npetitions += stats[0]
			nbytes += stats[1]
			nerrors += stats[2]

		yield (key, (npetitions, nbytes, nerrors) )

if __name__ == '__main__':
	MRLog.run()
