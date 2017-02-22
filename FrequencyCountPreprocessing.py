import os
import sys
import time

os.environ['SPARK_HOME'] = "/home/sidsurrexion/Downloads/spark-1.5.2/"
sys.path.append("/home/sidsurrexion/Downloads/spark-1.5.2/python")
sys.path.append("/home/sidsurrexion/Downloads/spark-1.5.2/python/build")


def compute_dictionary(valueKey):
    binaryCount = []
    listedGate = valueKey.collect()
    for entry in listedGate:
        if str(entry[1]) != '??' and len(str(entry[1])) != 8:
            binaryCount.append(str(entry[1]))
    return binaryCount


def generate_final_dna(filePath, zeroesMap, nucleotideMap, mode):
    reduce_values = sc.textFile(filePath).flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(
        lambda x, y: x + y)
    if mode == 'a':
        value_key = reduce_values.map(lambda x: (x[1], x[0])).sortByKey(False)
    else:
        value_key = reduce_values.map(lambda x: (x[1], x[0])).sortByKey(True)
    binarycount = compute_dictionary(value_key)
    binaryfeed = generate_binary_map(binarycount, zeroesMap)
    return construct_dna(nucleotideMap, binaryfeed)


def file_path_read(inputpath, mode, fraction, phase):
    if fraction >= 100:
        fraction = 80
    client = MongoClient()
    db = client.organisms
    collection = db.classifications
    errorprocess = db.errorlog
    zeroesmap = zero_string()
    nucleotidemap = nucleotide_dictionary()
    directories = os.listdir(inputpath)
    for allfolders in directories:
        if phase == "train" and allfolders == "testBed":
            continue
        f = inputpath + allfolders
        folders = os.listdir(f)
        for folder in folders:
            folderpath = inputpath + '/' + f
            if os.path.isdir(folderpath):
                files = os.listdir(folderpath)
                train_length = len(files)
                test_length = 0.2 * len(files)
                count = 0
                for entity in files:
                    entity = folderpath + '/' + entity
                    try:
                        if train_length == count:
                            if phase == "test":
                                break
                            else:
                                if (train_length + test_length) == count:
                                    break
                                else:
                                    collection.insert_one({"class": "testbed",
                                                           "dna": generate_final_dna(entity, zeroesmap,
                                                                                     nucleotidemap,
                                                                                     mode),
                                                           "filepath": entity,
                                                           "training": folder,
                                                           "process": "ContextAlignmentWithNoMutation"})
                        else:
                            collection.insert_one({"class": folder, "dna": generate_final_dna(entity, zeroesmap,
                                                                                              nucleotidemap, mode),
                                                   "fraction": fraction, "process": "FrequencyCountProcessing"})
                    except Exception as ex:
                        errorprocess.insert_one({"class": folder, "filepath": entity, "message": ex.message,
                                                 "process": "ContextAlignmentWithNoMutation"})
                    count += 1
            else:
                try:
                    collection.insert_one({"class": allfolders, "dna": generate_final_dna(entity, zeroesmap,
                                                                                          nucleotidemap, mode),
                                           "filepath": folderpath})
                except Exception as exc:
                    errorprocess.insert_one({"class": allfolders, "filepath": folderpath, "message": exc.message,
                                             "process": "ContextAlignmentWithNoMutation"})


def generate_binary_map(binaryCount, zeroesMap):
    binaryFeed = []
    for key in binaryCount:
        if len(bin(int(key, 16))[2:]) != 8:
            binaryValue = zeroesMap[8 - len(bin(int(key, 16))[2:])] + bin(int(key, 16))[2:]
        else:
            binaryValue = bin(int(key, 16))[2:]
        binaryFeed.append(binaryValue)
    return binaryFeed


def zero_string():
    zeroes = {1: '0', 2: '00', 3: '000', 4: '0000', 5: '00000', 6: '000000', 7: '0000000', 8: '00000000'}
    return zeroes


def nucleotide_dictionary():
    nucleotides = {'0000': 'AA', '0001': 'AC', '0010': 'AG', '0011': 'AT', '0100': 'CA', '0101': 'CC', '0110': 'CG',
                   '0111': 'CT', '1000': 'GA', '1001': 'GC', '1010': 'GG', '1011': 'GT', '1100': 'TA', '1101': 'TC',
                   '1110': 'TG', '1111': 'TT'}
    return nucleotides


def construct_dna(nucleotides, binary_data):
    dna_string = ""
    for binary_code in binary_data:
        dna_string += nucleotides[binary_code[0:4]] + nucleotides[binary_code[4:8]]
    return dna_string


def get_log_file_path(directory_path):
    current_date_time = str(time.time())
    current_date_time = current_date_time.replace(":", "")
    logfile = directory_path + 'log_file' + '_' + current_date_time + '.txt'
    return logfile


if __name__ == "__main__":
    startTime = time.time()
    input_file = sys.argv[0]
    mode = sys.argv[1]
    fraction = sys.argv[2]
    phase = sys.argv[3]
    log_file = '/home/sidsurrexion/Microsoft/Log'
    logPath = get_log_file_path(log_file)
    logMessages = 'Process: FrequencyCount \n'
    try:
        from pyspark import SparkContext
        from pyspark import SparkConf
        from pymongo import MongoClient

        sc = SparkContext('local')

        file_path_read(input_file, mode, fraction, phase)
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
