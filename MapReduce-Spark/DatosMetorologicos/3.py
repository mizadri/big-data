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

class MRWeather(MRJob):

    # Fase MAP (line es una cadena de texto) emite la fecha en forma de cadena como clave
    # y un diccionario como valor, con el maximo de la bateria, la suma de valores, el numero de valores,
    # el minimo.
    def mapper(self, key, line):
        words = line.split(',')
        
        # Evitamos la cabecera
        if words[0] != 'date-time':
            date, hour = words[0].split()
            formatDate = date.split("/")[1] + "/" + date.split("/")[0]
            
            # Emitimos un diccionario con los datos {maximo,suma de valores, numero de valores,minimo}
            battery = {}
            battery['max'] = float(words[8])
            battery['sum'] = float(words[8])
            battery['num'] = 1
            battery['min'] = float(words[8])
            
            yield formatDate, battery

    # Fase REDUCE (key es una cadena texto, values un generador de diccionarios) emite la fecha como clave
    # y un diccionario con el maximo, la media y el minimo definitivos de dicha fecha.
    def reducer(self, key, values):
        battery = {}
        valuesList = list(values)
        
        # Transformamos el generador en una lista y devolvemos un diccionario con los resultados
        battery['max'] = max(d['max'] for d in valuesList)
        avg = sum(d['sum'] for d in valuesList) / sum(d['num'] for d in valuesList)
        battery['avg'] = round(avg,2)
        battery['min'] = min(d['min'] for d in valuesList)
        
        yield key, battery

    # Fase COMBINER (key es una cadena texto con la fecha, values un generador de diccionarios)
    # emite la fecha como clave y un diccionario como valor, con el maximo de la bateria, la suma
    # de valores, el numero de valores y el minimo.
    def combiner(self, key, values):
        battery = {}
        valuesList = list(values)
        
        # Emitimos un diccionario con los resultados de cada nodo tras la fase mapper
        battery['max'] = max(d['max'] for d in valuesList)
        battery['min'] = min(d['min'] for d in valuesList)
        battery['sum'] = sum(d['sum'] for d in valuesList)
        battery['num'] = sum(d['num'] for d in valuesList)
        
        yield key, battery


if __name__ == '__main__':
    MRWeather.run()
