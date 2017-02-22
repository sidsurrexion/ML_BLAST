import sys
import time


class ClassesCount(object):
    __class_name = None
    __insertion = None
    __deletion = None
    __replacement = None
    __fraction = None
    __maxCount = None
    __count = None
    __process = None

    def __init__(self, class_name, insertion, deletion, replacement, fraction, maxcount, count, process):
        self.__class_name = class_name
        self.__insertion = insertion
        self.__deletion = deletion
        self.__replacement = replacement
        self.__fraction = fraction
        self.__maxCount = maxcount
        self.__count = count
        self.__process = process

    def get_class_name(self):
        return self.__class_name

    def get_insertion(self):
        return self.__insertion

    def get_deletion(self):
        return self.__deletion

    def get_replacement(self):
        return self.__replacement

    def get_fraction(self):
        return self.__fraction

    def get_maxCount(self):
        return self.__maxCount

    def get_count(self):
        return self.__count

    def get_process(self):
        return self.__process

    def set_insertion(self, insertion):
        self.__insertion + insertion

    def set_deletion(self, deletion):
        self.__deletion + deletion

    def set_replacement(self, replacement):
        self.__replacement + replacement

    def set_maxCount(self, maxCount):
        self.__maxCount + maxCount

    def set_count(self):
        self.__count += 1


def prepare_edit_distance_parameters(result_coll, output_file_path):
    cursor = result_coll.find()
    class1 = ClassesCount("class1", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])
    class2 = ClassesCount("class2", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])
    class3 = ClassesCount("class3", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])
    class4 = ClassesCount("class4", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])
    class5 = ClassesCount("class5", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])
    class6 = ClassesCount("class6", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])
    class7 = ClassesCount("class7", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])
    class8 = ClassesCount("class8", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])
    class9 = ClassesCount("class9", 0, 0, 0, int(cursor[0]["fraction"]), 0, 0, cursor[0]["process"])

    for document in cursor:
        if document["class"] == "class1":
            class1.set_insertion(float(document["insertion"]))
            class1.set_deletion(float(document["deletion"]))
            class1.set_replacement(float(document["replacement"]))
            class1.set_maxCount(float(document["replacement"]))
            class1.set_count()
        elif document["class"] == "class2":
            class2.set_insertion(float(document["insertion"]))
            class2.set_deletion(float(document["deletion"]))
            class2.set_replacement(float(document["replacement"]))
            class2.set_maxCount(float(document["replacement"]))
            class2.set_count()
        elif document["class"] == "class3":
            class3.set_insertion(float(document["insertion"]))
            class3.set_deletion(float(document["deletion"]))
            class3.set_replacement(float(document["replacement"]))
            class3.set_maxCount(float(document["replacement"]))
            class3.set_count()
        elif document["class"] == "class4":
            class4.set_insertion(float(document["insertion"]))
            class4.set_deletion(float(document["deletion"]))
            class4.set_replacement(float(document["replacement"]))
            class4.set_maxCount(float(document["replacement"]))
            class4.set_count()
        elif document["class"] == "class5":
            class5.set_insertion(float(document["insertion"]))
            class5.set_deletion(float(document["deletion"]))
            class5.set_replacement(float(document["replacement"]))
            class5.set_maxCount(float(document["replacement"]))
            class5.set_count()
        elif document["class"] == "class6":
            class6.set_insertion(float(document["insertion"]))
            class6.set_deletion(float(document["deletion"]))
            class6.set_replacement(float(document["replacement"]))
            class6.set_maxCount(float(document["replacement"]))
            class6.set_count()
        elif document["class"] == "class7":
            class7.set_insertion(float(document["insertion"]))
            class7.set_deletion(float(document["deletion"]))
            class7.set_replacement(float(document["replacement"]))
            class7.set_maxCount(float(document["replacement"]))
            class7.set_count()
        elif document["class"] == "class8":
            class8.set_insertion(float(document["insertion"]))
            class8.set_deletion(float(document["deletion"]))
            class8.set_replacement(float(document["replacement"]))
            class8.set_maxCount(float(document["replacement"]))
            class8.set_count()
        elif document["class"] == "class9":
            class9.set_insertion(float(document["insertion"]))
            class9.set_deletion(float(document["deletion"]))
            class9.set_replacement(float(document["replacement"]))
            class9.set_maxCount(float(document["replacement"]))
            class9.set_count()

    f = open(output_file_path, 'w')
    f.write("Class1:" + ",Insertion:" + (class1.get_insertion() / class1.get_count()) + ",Deletion:" + (
        class1.get_deletion() / class1.get_count()) + ",Replacement:" + (
            class1.get_replacement() / class1.get_count()) + ",maxCount:" + (
            class1.get_maxCount() / class1.get_count()) + ",Process:"+class1.get_process())
    f.write("Class:" + ",Insertion:" + (class2.get_insertion() / class2.get_count()) + ",Deletion:" + (
        class2.get_deletion() / class2.get_count()) + ",Replacement:" + (
            class2.get_replacement() / class2.get_count()) + ",maxCount:" + (
            class2.get_maxCount() / class2.get_count()) + ",Process:"+class2.get_process())
    f.write("Class:" + ",Insertion:" + (class3.get_insertion() / class3.get_count()) + ",Deletion:" + (
        class3.get_deletion() / class3.get_count()) + ",Replacement:" + (
            class3.get_replacement() / class3.get_count()) + ",maxCount:" + (
            class3.get_maxCount() / class3.get_count()) + ",Process:"+class3.get_process())
    f.write("Class:" + ",Insertion:" + (class4.get_insertion() / class4.get_count()) + ",Deletion:" + (
        class4.get_deletion() / class4.get_count()) + ",Replacement:" + (
            class4.get_replacement() / class4.get_count()) + ",maxCount:" + (
            class4.get_maxCount() / class4.get_count()) + ",Process:"+class4.get_process())
    f.write("Class:" + ",Insertion:" + (class5.get_insertion() / class5.get_count()) + ",Deletion:" + (
        class5.get_deletion() / class5.get_count()) + ",Replacement:" + (
            class5.get_replacement() / class5.get_count()) + ",maxCount:" + (
            class5.get_maxCount() / class5.get_count()) + ",Process:"+class5.get_process())
    f.write("Class:" + ",Insertion:" + (class6.get_insertion() / class6.get_count()) + ",Deletion:" + (
        class6.get_deletion() / class6.get_count()) + ",Replacement:" + (
            class6.get_replacement() / class6.get_count()) + ",maxCount:" + (
            class6.get_maxCount() / class6.get_count()) + ",Process:"+class6.get_process())
    f.write("Class:" + ",Insertion:" + (class7.get_insertion() / class7.get_count()) + ",Deletion:" + (
        class7.get_deletion() / class7.get_count()) + ",Replacement:" + (
            class7.get_replacement() / class7.get_count()) + ",maxCount:" + (
            class7.get_maxCount() / class7.get_count()) + ",Process:"+class7.get_process())
    f.write("Class:" + ",Insertion:" + (class8.get_insertion() / class8.get_count()) + ",Deletion:" + (
        class8.get_deletion() / class8.get_count()) + ",Replacement:" + (
            class8.get_replacement() / class8.get_count()) + ",maxCount:" + (
            class8.get_maxCount() / class8.get_count()) + ",Process:"+class8.get_process())
    f.write("Class:" + ",Insertion:" + (class9.get_insertion() / class9.get_count()) + ",Deletion:" + (
        class9.get_deletion() / class9.get_count()) + ",Replacement:" + (
            class9.get_replacement() / class9.get_count()) + ",maxCount:" + (
            class9.get_maxCount() / class9.get_count()) + ",Process:"+class9.get_process())
    f.close()


def get_log_file_path(directory_path):
    current_date_time = str(time.time())
    current_date_time = current_date_time.replace(":", "")
    logfile = directory_path + 'log_file' + '_' + current_date_time + '.txt'
    return logfile


if __name__ == "__main__":
    startTime = time.time()
    outputPath = sys.argv[0]
    log_file = '/home/sidsurrexion/Microsoft/Log'
    logPath = get_log_file_path(log_file)
    logMessages = 'Process: Purge \n'
    try:
        from pymongo import MongoClient

        client = MongoClient()
        db = client.organisms
        result = db.result
        prepare_edit_distance_parameters(result, outputPath)
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
