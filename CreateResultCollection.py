import sys
import time


def get_log_file_path(directory_path):
    current_date_time = str(time.time())
    current_date_time = current_date_time.replace(":", "")
    logfile = directory_path + 'log_file' + '_' + current_date_time + '.txt'
    return logfile


def purge_collections(classification_collection, error_collection):
    classification_collection.delete_many({"process": "FrequencyCalculationForMutationModel"})
    error_collection.delete_many({"process": "FrequencyCalculationForMutationModel"})


def add_result_collection(database):
    collection = database.result
    collection.insert_one({
        "class": "",
        "fraction": 0,
        "process": "test",
        "insertion": "",
        "deletion": "",
        "replacement": "",
        "editdistance": 0,
        "maxCount": 0,
        "filename": ""
    })

if __name__ == "__main__":
    startTime = time.time()
    log_file = '/home/sidsurrexion/Microsoft/Log'
    logPath = get_log_file_path(log_file)
    logMessages = 'Process: Purge \n'
    try:
        from pymongo import MongoClient

        client = MongoClient()
        db = client.organisms
        add_result_collection(db)

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