"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

def getNewLine(line, colSpecs):
    newLine = list()

    for start, end in colSpecs:
        newLine.append(line[start:end].strip())
    
    newLine[2] = newLine[2].strip(' %').replace(',', '.')
    
    return ';'.join(newLine)

def ingest_data():

    #
    # Inserte su código aquí
    #
    with open('clusters_report.txt', mode='r') as file_data:
        clustersData = file_data.readlines()
        colNames = ['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave']

        ## getting column indexes ##
        colSpecs = list()
        pos = 0

        firstLine = clustersData[0]
        for i in range(3, len(firstLine)):
            if firstLine[i] != " " and firstLine[i] != "\n" and firstLine[i-2:i] == "  ":
                colSpecs.append((pos, i))
                pos = i

            if len(colSpecs) >= len(colNames) - 1:
                break
        
        colSpecs.append((pos, -1))
        
        ## creating clean data file ##
        with open('cleanData.txt', 'w') as cleanData:
            #regex to combine several spaces into one
            combineSpaces = re.compile(r"\s+")
            
            firstCols = ''
            tags = ''

            for line in clustersData[4:]:
                if line.strip() != '':
                    if firstCols == '':
                        firstCols = line[:pos]
                    
                    tags += line[pos:].strip() + ' '
                    continue

                tags = combineSpaces.sub(" ", tags).strip()

                if tags[-1] != '.':
                    tags += '.'

                newLine = getNewLine(firstCols + tags, colSpecs)
                cleanData.write(newLine + '\n')

                firstCols = ''
                tags = ''
    
    df = pd.read_csv('cleanData.txt', sep = ';', names = colNames)

    return df
