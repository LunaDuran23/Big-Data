import re
from mrjob.job import MRJob
from mrjob.step import MRStep


WORD_REGREX = re.compile(r'\b\w+\b')


#el siguiente codigo fue logrado con ayuda de la informacion encontrada en:
#https://mrjob.readthedocs.io/en/latest/guides/quickstart.html#running-your-job-different-ways


class Punto2(MRJob):
    def steps(self):
        #en este caso es necesario usar mas de un solo paso en el
        #trabajo. Para definir varios pasos se crea la funcion steps()
        return [
            MRStep(mapper=self.mapper_words,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_sort),
            MRStep(reducer=self.reducer_find_20)
        ]
    def mapper_words(self, _, line):
        for word in WORD_REGREX.findall(line): 
            #tomamos cada palabra
            if len(word)>5: 
            #tomamos las mayores a 5 en longitud
                yield(word.lower(), 1) 
    
    def combiner_count(self, word, count):
        yield(word, sum(count)) 
        #sumamos las ocurrencias de cada palabra

    def reducer_count(self, word, count):
        yield None, (sum(count), word) 

    def reducer_sort(self, _, count):
        yield None, sorted(count, reverse= True)
        # organizar los valores con respecto a las ocurrencias de forma descendente

    def reducer_find_20(self, _, word_count):
        lista = list(word_count)[0]
        for i in range(20):
        #tomar los 20 primeros    
            yield(lista[i])


if __name__=='__main__':
    Punto2.run()

#para correr este archivo en la terminal usar los comandos:
#input es la carpeta de destino que debe ya estar creada en el paso anterior
#3_out.txt es el archivo de salida con el top 20 de palabras
#python3 2_Wordcount.py input > 3_out.txt
