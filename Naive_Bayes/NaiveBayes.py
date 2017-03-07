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

import csv
import math

class NaiveBayes(object):
	
	'''
	El constructor de la clase lee el conjunto de entrenamiento completo almacenado en fichero , calculando y almacenando 
	como atributos las distintas probabilidades que luego necesitara para predecir la clase de nuevas instancias. 
	Acepta el factor de suavizado de Laplace smooth que se utilizara para calcular probabilidades.
	'''
	def __init__(self, fichero, smooth=1):

		# Diccionario con la probabilidad de cada valor de la clase
		self.probClass = {}

		# Diccionario con la probabilidad de cada valor de la clase para cada valor de cada atributo
		self.prob = {}

		# Numero de instancias
		self.countRows = 0

		# Lectura del fichero y almacenamiento de los datos en las estructuras de datos anteriores
		self.readFile(fichero)

		# Muestra la informacion de los datos almacenados
		self.showInfo()

		# Calcula las probabilidades tanto de los atributos como de la clase
		self.calculaProb(smooth)

	#### Se lee el fichero almacenado en 'fichero' y calculan los datos en los diccionarios de 'probClass' y 'prob'
	### y el contador 'countRows' ####
	def readFile(self, fichero):

		input_file = csv.DictReader(open(fichero))

		for row in input_file:

			self.countRows += 1

			# Inicializamos los atributos en el diccionario con la primera instancia leida
			if not self.prob:
				for attr in row:
					self.prob[attr] = {}

			# Contamos las instancias de cada valor de la clase
			if row['class'] in self.probClass:
				self.probClass[row['class']] += 1
			else:
				self.probClass[row['class']] = 1

			# Contamos el numero de instancias de cada valor de cada clase para cada valor que puede tomar cada atributo
			for attr in self.prob:
				if row[attr] not in self.prob[attr]:
					self.prob[attr][row[attr]] = {row['class']: 1}
				else:
					if row['class'] in self.prob[attr][row[attr]]:
						self.prob[attr][row[attr]][row['class']] += 1
					else:
						self.prob[attr][row[attr]][row['class']] = 1

		# Agregamos los valores de las clases que no se han contado, inicializados a 0.
		for attr in self.prob:
			for attrValue in self.prob[attr]:
				for classValue in self.probClass:
					if classValue not in self.prob[attr][attrValue]:
						self.prob[attr][attrValue][classValue] = 0

	#### Se muestra toda la informacion de las instancias leidas ####
	def showInfo(self):
	
		# Numero de instancias leidas
		print ('\nTotal instancias: ' + str(self.countRows) + '\n')
		
		# Posibles valores de los atributos y clase
		for attr in self.prob:
			print ("Atributo '" + attr + "': {'" + "', '".join(self.prob[attr].keys()) + "'}")
		print ("Clase: {'" + "', '".join(self.probClass.keys()) + "'}")
		print ('\n')

		# Numero de instancias de cada clase
		for classAtr in self.probClass:
			print ("Instancias clase '" + classAtr + "': " + str(self.probClass[classAtr]))
		print ('\n')

		# Numero de instancias para cada valor de cada atributo y una determinada clase
		for attr in self.prob:
			for valueAttr in self.prob[attr]:
				for classValue in self.prob[attr][valueAttr]:
					print ('Instancias (' + attr + ' = ' + valueAttr + ', class = ' + classValue + '): '+ str(self.prob[attr][valueAttr][classValue]))

		print ('\n')
					
	#### Se reutilizan los diccionarios de 'probClass' y 'prob' para calcular sus probabilidas en funcion del factor de suavizado ####
	def calculaProb(self, smooth):

		# Calculamos la probabilidad de cada valor de la clase para cada valor de cada atributo
		for attr in self.prob:
			for valueAttr in self.prob[attr]:
				for classValue in self.prob[attr][valueAttr]:
					self.prob[attr][valueAttr][classValue] = (self.prob[attr][valueAttr][classValue] + smooth)/ (float(self.probClass[classValue]) + (smooth * float(len(self.prob[attr]))))

		# Calculamos la probabilidad de cada valor de la clase
		for classAtr in self.probClass:
			self.probClass[classAtr] = self.probClass[classAtr] / float(self.countRows)

	'''
	A partir de la informacion de probabilidades previamente obtenida en el constructor y una instancia devuelve 
	una cadena de texto con la clase que predice el clasificador. Para el calculo de las probabilidades de cada clase 
	se usa la suma de logaritmos en lugar de multiplicacion. Supondremos que la instancia pasada como argumento tiene 
	el mismo numero y nombre de atributos que las instancias de entrenamiento.
	'''
	def clasifica(self, instancia):
		
		probInstance = {}
		
		# Calculamos el logaritmo de la probabilidad de cada valor de la clase
		for classValue in self.probClass:
			probInstance[classValue] = math.log(self.probClass[classValue],2)
		
		# Calculamos el logaritmo de la probabilidad de cada valor de la clase para cada valor de cada atributo
		for attrValue in instancia:
			for classValue in self.prob[attrValue][instancia[attrValue]]:
				probInstance[classValue] += math.log(self.prob[attrValue][instancia[attrValue]][classValue],2)

		# El valor de la clase predicha sera aquel que tenga mayor probabilidad
		predictedClass = max(probInstance, key=probInstance.get)

		return predictedClass

	'''
	Este metodo sirve para medir la tasa de aciertos del clasicador previamente entrenado. Para ello recibe un conjunto
	de test almacenado en fichero , que tiene el mismo formato CSV con cabecera que el fichero usado en el constructor, 
	y clasifica cada instancia de test leida. Devuelve una tupla (aciertos , total , tasa) conteniendo el numero total 
	de instancias de test clasificadas correctamente, el numero total de instancias de test y la tasa de aciertos 
	(aciertos/total).
	'''
	def test(self, fichero):
		
		input_file = csv.DictReader(open(fichero))
		
		aciertos = 0
		total = 0
		
		print ('Test:')
		for row in input_file:

			# Preparamos las instancias sin la clase
			instance = {}
			for attr in row:
				if attr != 'class':
					instance[attr] = row[attr]

			# Mostramos los valores de los atributos de la instancia completa, incluida la clase.
			print (str(instance) + '-' + row['class'])
			
			# Obtenemos la clase predicha a partir de la instancia.
			predictedClass = self.clasifica(instance)
			
			# Mostramos la clase que predice el clasificador Naive Bayes.
			print ('Clase predicha: ' + predictedClass)
			
			# Indicamos si la clasificacion acierta o falla y actualizamos los contadores.
			if predictedClass == row['class']:
				aciertos += 1
				print ('\t--> Acierto')
			else:
				print ('\t--> Fallo')
			
			total += 1
				
			print ('\n')

		# Devolvemos una tupla con los resultados del test
		tasa = aciertos / total
		resultTest = (aciertos, total, tasa)
		
		#print (str(resultTest))
		
		return resultTest

nb = NaiveBayes('car.train', 1)
#clase = nb.clasifica({'day': 'weekday', 'season' : 'winter' , 'wind': 'high' , 'rain': 'heavy'})
(aciertos , total , tasa) = nb.test('car.test')
