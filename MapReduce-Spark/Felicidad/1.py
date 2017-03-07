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

class MRHappiness(MRJob):
    
    # Fase MAP (line es una cadena de texto) emite un numero como clave y como valor
    # las parejas (palabra, media) con Twitter Rank y felicidad media menor que 2.
    def mapper(self, key, line):
        
        line = line.split('\t')
        happiness = float(line[2])
        if line[4]!='--' and happiness < 2:
            # Emitimos con la misma clave para que vayan al mismo reducer.
            yield 0,(line[0],happiness)
        
    # Fase REDUCE (key es un 0, values un generador de parejas (palabra, media))
    # emite las 5 parejas (palabra, media) con mayor felicidad media.
    def reducer(self, key, values):
        
        # Transformamos el generador values en una lista ordenada por la felicidad media.
        valuesList = sorted(values, key=lambda pareja: pareja[1], reverse=True)
            
        for i in range(5):
            yield valuesList[i][0], valuesList[i][1]

if __name__ == '__main__':
    MRHappiness.run()
