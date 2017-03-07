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
import heapq

#Variable global que actua de monticulo
h = []

class MRHappiness(MRJob):
	
	# Fase MAP (line es una cadena de texto) se encarga insertar
        # las parejas (palabra, media) con Twitter Rank y felicidad media
        # menor que 2 al monticulo h.
	def mapper(self, key, line):
		
		global h
		line = line.split('\t')
		happiness = float(line[2])
		if line[4]!='--' and happiness < 2:
                    # heapq.headpush() inserta un elemento (pareja) en un monticulo (h)
			heapq.heappush(h, (line[0], happiness))
	
	# Fase REDUCE (key es un numero, values un generador de parejas (palabra, media))
        # emite las 5 parejas con mayor felicidad media.
	def reducer(self, key, values):

		# Transformamos el generador values en una lista ordenada por la felicidad media.
		valuesList = sorted(values, key=lambda pareja: pareja[1], reverse=True)

		for i in range(5):
			yield valuesList[i][0], valuesList[i][1]
			
	# Disparador MAPPER_FINAL (se emiten las 5 palabras mas felices al final de la
        # ejecucion de cada mapper).
	def mapper_final(self):
            
		global h
                # heapq.nlargest() obtiene los n elementos mas grandes del monticulo h segun una clave key
		for word, happiness in heapq.nlargest(5,h, key=lambda pareja: pareja[1]):
			yield 1,(word,happiness)
	

if __name__ == '__main__':
	MRHappiness.run()
