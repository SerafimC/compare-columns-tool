'''
Helper script to replace a string inside a group of files.
'''

from os import listdir
from os.path import isfile, join

folder_path = './testfolder/'

onlyfiles = [f for f in listdir(folder_path) if isfile(join(folder_path, f))]


for filename in onlyfiles:
    x = 'pipe.startTime.minusHours(10).toString()'#input("enter text to be replaced:")
    y = 'pipe.startTime.toString()'#input("enter text that will replace:")

    #read input file
    fin = open(folder_path + filename, "rt")
    #read file contents to string
    data = fin.read()
    #replace all occurrences of the required string
    data = data.replace(x, y)
    #close the input file
    fin.close()
    #open the input file in write mode
    fin = open(folder_path + filename, "wt")
    #overrite the input file with the resulting data
    fin.write(data)
    #close the file
    fin.close()

