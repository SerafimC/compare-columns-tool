from os import listdir
from os.path import isfile, join

log_file = open('./compare.log', 'w+')
log_file.truncate()

def write_to_log(message):
    log_file.write(str(message) + '\n')