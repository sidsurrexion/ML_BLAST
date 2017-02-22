import os
import sys
import time

os.environ['SPARK_HOME'] = "/home/sidsurrexion/Downloads/spark-1.5.2/"
sys.path.append("/home/sidsurrexion/Downloads/spark-1.5.2/python")
sys.path.append("/home/sidsurrexion/Downloads/spark-1.5.2/python/build")


def nucleotide_dictionary():
    nucleotides = {'0000': 'AA', '0001': 'AC', '0010': 'AG', '0011': 'AT', '0100': 'CA', '0101': 'CC', '0110': 'CG',
                   '0111': 'CT', '1000': 'GA', '1001': 'GC', '1010': 'GG', '1011': 'GT', '1100': 'TA', '1101': 'TC',
                   '1110': 'TG', '1111': 'TT', '0': 'A', '1': 'T', '000': 'AA', '001': 'AG', '010': 'CA', '011': 'CG',
                   '100': 'GA', '101': 'GG', '110': 'TA', '111': 'TT', '00': 'A', '01': 'C', '10': 'T', '11': 'G'}
    return nucleotides


def create_dna_string(input_string, nucleotides):
    dna_string = ""
    if len(input_string) > 8:
        if len(input_string) % 8 != 0:
            for i in range(0, (len(input_string) - len(input_string) % 8), 4):
                dna_string += nucleotides[input_string[0:4]]
            if len(input_string) % 8 <= 4:
                dna_string += nucleotides[input_string[(len(input_string) - len(input_string) % 8):len(input_string)]]
            else:
                dna_string \
                    += nucleotides[input_string[(len(input_string) - len(input_string) % 8): len(input_string) - 4]] + \
                       nucleotides[input_string[len(input_string) - 4: len(input_string)]]
        else:
            for i in range(0, len(input_string), 4):
                dna_string += nucleotides[input_string[i:i + 4]]
    elif len(input_string) == 8:
        dna_string = nucleotides[input_string[0:4]] + nucleotides[input_string[4:8]]
    elif 4 < len(input_string) < 8:
        dna_string = nucleotides[input_string[0:4]] + nucleotides[input_string[4:len(input_string)]]
    else:
        dna_string = nucleotides[input_string[0: len(input_string)]]
    return dna_string


def select_valid_bits(hex_dec):
    hex_dec = hex_dec.replace("\r\n", "")
    if len(str(hex_dec)) >= 8:
        return '??'
    return hex_dec


def section_parse(tup):
    word_list = tup[1].split(" ")
    return map(lambda x: (select_valid_bits(x), 1), word_list)


def pull_string_dna(tup):
    t = bin(int(tup[1], 16))[2:]
    n = nucleotide_dictionary()
    return create_dna_string(t, n)


def string_join(frequent):
    freq_join = []
    for tup in frequent:
        freq_join.append(tup[1])
    return ''.join(freq_join)


def prepare_mutations(file_path_mutation):
    reduce_values = sc.wholeTextFiles(file_path_mutation).flatMap(section_parse).reduceByKey(lambda x, y: x + y).map(
        lambda x: (x[1], x[0])).sortByKey(False)
    values = reduce_values.collect()
    most_frequent = string_join(map(lambda x: (pull_string_dna(x), 1), values[1:3]))
    least_frequent = string_join(map(lambda x: (pull_string_dna(x), 1), values[-2:]))
    return [most_frequent, least_frequent]


def file_path_read(inputpath):
    client = MongoClient()
    db = client.organisms
    collection = db.classifications
    errorprocess = db.errorlog
    folders = os.listdir(inputpath)
    for folder in folders:
        folderpath = inputpath + '/' + folder
        frequent_dna = prepare_mutations(folderpath)
        try:
            collection.insert_one({"class": folder, "most_significant_dna": frequent_dna[0],
                                   "least_significant_dna": frequent_dna[1],
                                   "process": "FrequencyCalculationForMutationModel"})
        except Exception as ex:
            errorprocess.insert_one({"class": folder, "filepath": folderpath, "message": ex.message,
                                     "process": "FrequencyCalculationForMutationModel"})


def get_log_file_path(directory_path):
    current_date_time = str(time.time())
    current_date_time = current_date_time.replace(":", "")
    logfile = directory_path + 'log_file' + '_' + current_date_time + '.txt'
    return logfile


if __name__ == "__main__":
    startTime = time.time()
    input_file = sys.argv[0]
    percent = sys.argv[1]
    mode = sys.argv[2]
    log_file = '/home/sidsurrexion/Microsoft/Log'
    logPath = get_log_file_path(log_file)
    logMessages = 'Process: FrequencyCalculationForMutationModel \n'
    try:
        from pyspark import SparkContext
        from pyspark import SparkConf
        from pymongo import MongoClient

        sc = SparkContext('local')
        file_path_read(input_file)
        endTime = time.time()
        logMessages += 'Time to process files: ' + str(endTime - startTime) + ' seconds.' + '\n'
        logMessages += 'success'
        logFile = open(logPath, 'a')
        logFile.write(logMessages)
        logFile.close()

    except ImportError as e:
        endTime = time.time()
        logMessages += e.message + '\n'
        logMessages += 'error'
        logFile = open(logPath, 'a')
        logFile.write(logMessages)
        logFile.close()
        sys.exit(1)
