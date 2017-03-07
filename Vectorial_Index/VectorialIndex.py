'''
	# Asignatura: Sistemas de Gestion de Datos y de la Informacion
	# Practica 2 (Mineria de datos y recuperacion de la informacion)
	# Grupo 10 (Adrian Garcia y Frank Julca)
	# Declaracion de integridad: Adrian Garcia y Frank Julca
	declaramos que esta solucion es fruto exclusivamente de nuestro trabajo personal.
	No hemos sido ayudados por ninguna otra persona ni hemos obtenido la solucion de fuentes externas,
	y tampoco hemos compartido nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
	deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demas.
	
'''
import string
from math import log,sqrt
from os import walk
import codecs

# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea):
  return filter(lambda x: len(x) > 0, 
	map(lambda x: x.lower().strip(string.punctuation), linea.split()))


class VectorialIndex(object):

	'''
	Recorre recursivamente todos los ficheros que hay en directorio y crea un indice invertido para relacionar 
	las palabras con una lista de (documento, peso). Los pesos se calculan usando la tecnica TF-IDF,
	y los documentos dentro del indice se representan como numeros enteros. El constructor tambien acepta
	una lista de palabras vacias stop que se ignoran al procesar los ficheros y que por tanto no aparecen 
	en el indice invertido. Esta lista se almacena para procesar las consultas mas adelante.
	'''
	def __init__(self, directorio, stop=[]):

		ndocs = 0
		self.pesos = {}
		self.doc_modulo = {}
		self.doc_ruta = {}
		self.stopList = stop
		
		frecuencias = {}
		for (path, carpetas, archivos) in walk(directorio):
			if len(archivos) > 0:
				for archivo in archivos:
					
					ruta = path+'/'+archivo

					if archivo != '.DS_Store':
						sr = codecs.open(ruta, 'r', 'iso-8859-1')

						# Usamos el numero de documento como indice y lo guardamos en un diccionario para recuperar la ruta mas tarde
						self.doc_ruta[ndocs] = ruta
						
						for line in sr:
							for word in extrae_palabras(line):

								# Obviamos los terminos que aparezcan en stop
								if word not in stop:

									# Almacenamos la frecuencia de las palabras por documento
									if word in frecuencias:
										if ndocs in frecuencias[word]:
											frecuencias[word][ndocs] += 1
										else:
											frecuencias[word][ndocs] = 1
									else:
										frecuencias[word] = {ndocs: 1}
						ndocs += 1
	   

		# Asignamos pesos a los terminos clave de cada documuento con la tecnica TF-IDF
		for word in frecuencias:
			self.pesos[word] = []
			for doc in frecuencias[word]:

				tfij = (1 + log(frecuencias[word][doc],2))
				idfi = log(ndocs/len(frecuencias[word]),2)
				peso = tfij * idfi
				
				# Guardamos los pesos en un diccionario de palabras. 
				# Por cada una de ellas guardamos una lista de tuplas (numero_documento, peso)
				self.pesos[word].append((doc,peso))
				
				# Aprovechamos para calcular el cuadrado de los pesos de cada documento
				if doc not in self.doc_modulo:
					self.doc_modulo[doc] = peso ** 2
				else:
					self.doc_modulo[doc] += peso ** 2

		# Obtenemos el modulo de los pesos de cada documento antes de terminar
		for doc in self.doc_modulo:			
			self.doc_modulo[doc] = sqrt(self.doc_modulo[doc])
		
	'''
		Dada una consulta representada como una cadena de palabras no repetidas separadas por espacios,
		devuelve una lista de parejas (fichero, relevancia) con los n resultados mas relevantes usando 
		el modelo de recuperacion vectorial.
	'''
	def consulta_vectorial(self, consulta, n=3):
		doc_relevancias = {}
		wordList = consulta.lower().split()

		# Eliminamos las palabras que no esten en el indice
		for word in wordList:
			if word in self.stopList:
				wordList.remove(word)

		# Calculamos el modulo de la consulta
		moduloQ = sqrt(len(wordList))

		# Sumamos los pesos de cada documento en funcion de las palabras de la consulta
		for word in wordList:
			if word in self.pesos:
				for doc,peso in self.pesos[word]:
					if doc not in doc_relevancias:
						doc_relevancias[doc] = peso
					else:
						doc_relevancias[doc] += peso

		# Caculamos la relevancia de cada documento
		for doc in doc_relevancias:
			doc_relevancias[doc] *= ( 1 / (self.doc_modulo[doc] * moduloQ))
	
		# Agregamos los documentos con peso 0 que no aparecian en los pesos de cada palabra
		for numDoc in range(len(self.doc_ruta)):
			if numDoc not in doc_relevancias:
				doc_relevancias[numDoc] = 0.0

		return self.getNResults(doc_relevancias, n)

	'''
	Dada una consulta representada como una cadena de palabras no repetidas separadas por espacios que se entiende 
	como una conjuncion, devuelve una lista de nombres de fichero con todos los resultados en los que aparecen
	todas las palabras de la consulta.
	'''
	def consulta_conjuncion(self, consulta):
		
		results = []
		countDocs = {}
		wordList = consulta.lower().split()
		appearances = []

		# Eliminamos las palabras que no esten en el indice
		for word in wordList:
			if word in self.stopList:
				wordList.remove(word)

		for word in wordList:
			if word in self.pesos:
				# Obtenemos una lista de documentos a partir de los pesos de las palabras
				doc_list = [doc for doc,relevancia in self.pesos[word]]

				# Agregamos cada lista de las palabras que aparecen en la consulta
				appearances.append(doc_list)
			else:
				return []

		# Obtenemos una lista de documentos en los que aparecen todas las palabras de la consulta
		docs = self.intersecGeneric(appearances)

		for doc in docs:
			results.append(self.doc_ruta[doc])


		return results
	
	# Recibe un diccionario con las relevancias y un numero de resultados y obtiene una lista de n tuplas 
	# (nombre_fichero, relevancia).
	def getNResults(self, doc_relevancias, n):

		# Ordenamos el diccionario por los valores de las relevancias
		sorted_result = sorted(doc_relevancias.items(), key=lambda x: x[1], reverse=True)

		result_n = []
		i = 0
		# Devolvemos los n nombres de los ficheros mas relevantes
		for doc,relevancia in sorted_result:
			if i < n:
				result_n.append((self.doc_ruta[doc],relevancia))
			else:
				break
			i += 1
		return result_n

	# Recibimos un lista de listas de documentos en los que aparecen las palabras, cruzamos las listas
	# y nos quedamos con los documentos comunes y devolvemos una lista con ellos.
	def intersecGeneric(self, appearances):

		# Ordenamos crecientemente por tamano de la lista para intersectar primero las listas mas cortas
		terms = sorted(appearances, key=lambda l: len(l), reverse=False)

		answer = self.head(terms)
		terms = self.tail(terms)

		# Si la consulta es de un solo termino, devolvemos todos sus documentos
		if len(terms) == 0 :
			return answer

		while len(terms) != 0 and len(answer) != 0:
			e = self.head(terms)
			answer = self.intersecLists(answer, e)
			terms = self.tail(terms)

		return answer

	# Obtiene el primer elemento de la lista pasada como parametro
	def head(self, appearances):
		return appearances[0]
		
	# Obtiene todos los elementos salvo el primero de la lista pasada como parametro
	def tail(self, appearances):
		return appearances[1:]

	# Intersectamos dos listas pasadas como parametro y devolvemos una listas con los documentos que son
	# comunes a ellas dos,
	def intersecLists(self, up1, up2):
		answer = []

		# Ordenamos crecientemente las listas por numero de documento
		p1 = sorted(up1, reverse=False)
		p2 = sorted(up2, reverse=False)

		while len(p1) != 0 and len(p2) != 0:
			if self.docID(p1) == self.docID(p2):
				answer.append(self.docID(p1))
				p1 = self.next(p1)
				p2 = self.next(p2)

			elif self.docID(p1) < self.docID(p2):
				p1 = self.next(p1)
			else:
				p2 = self.next(p2)

		return answer

	# Devuelve el primer documento de la lista 'p'
	def docID(self, p):
		return p[0]

	# Obtiene todos los documentos salvo el primero de la lista pasada como parametro
	def next(self, p):
		return p[1:]

#stoplist = ['the' , 'a', 'an']
c = VectorialIndex("docs")

print(str(c.consulta_conjuncion('France Health Service')))
print(str(c.consulta_conjuncion('Duo Dock')))
print(str(c.consulta_vectorial('Duo Dock',n=10)))

