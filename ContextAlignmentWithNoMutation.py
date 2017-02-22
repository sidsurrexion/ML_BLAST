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


def prepare_binary_specimens(filePath, nucleotides):
    mappedBits = sc.textFile(filePath).flatMap(lambda x: x.split(" ")).map(
        lambda x: (sample_hexadecimal_conversion_to_binary(x, nucleotides), 1))
    mappedBits = mappedBits.collect()
    dna_string = []
    for mappedBit in mappedBits:
        if mappedBit[0] is not None:
            dna_string.append(mappedBit[0])
    return ''.join(dna_string)


def sample_hexadecimal_conversion_to_binary(value, nucleotides):
    if value != '??':
        return create_dna_string(bin(int(value, 16))[2:], nucleotides)


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


def file_path_read(inputpath, fraction, phase):
    if fraction >= 100:
        fraction = 80
    client = MongoClient()
    db = client.organisms
    collection = db.classifications
    errorprocess = db.errorlog
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
                                    collection.insert_one(
                                        {"class": "testbed", "dna": prepare_binary_specimens(folderpath
                                                                                             , nucleotidemap),
                                         "filepath": entity,
                                         "training": folder, "process": "ContextAlignmentWithNoMutation"})
                        else:
                            collection.insert_one({"class": folder, "dna": prepare_binary_specimens(entity,
                                                                                                    nucleotidemap),
                                                   "fraction": fraction, "process": "ContextAlignmentWithNoMutation"})
                    except Exception as ex:
                        errorprocess.insert_one({"class": folder, "filepath": entity, "message": ex.message,
                                                 "process": "ContextAlignmentWithNoMutation"})
                    count += 1
            else:
                try:
                    collection.insert_one({"class": allfolders, "dna": prepare_binary_specimens(folderpath,
                                                                                                nucleotidemap),
                                           "filepath": folderpath,
                                           "process": "ContextAlignmentWithNoMutation"})
                except Exception as exc:
                    errorprocess.insert_one({"class": allfolders, "filepath": folderpath, "message": exc.message,
                                             "process": "ContextAlignmentWithNoMutation"})


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
    logMessages = 'Process: ContextAlignmentWithNoMutation \n'
    try:
        from pyspark import SparkContext
        from pyspark import SparkConf
        from pymongo import MongoClient

        sc = SparkContext('local')
        nucleotide_map = nucleotide_dictionary()
        file_path_read(input_file, percent, mode)
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
