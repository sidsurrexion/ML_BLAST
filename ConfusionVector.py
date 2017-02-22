import sys
import time
import csv


class ClassesCount(object):
    __class1 = None
    __class2 = None
    __class3 = None
    __class4 = None
    __class5 = None
    __class6 = None
    __class7 = None
    __class8 = None
    __class9 = None
    __output_dict = None

    def __init__(self, class1, class2, class3, class4, class5, class6, class7, class8, class9, output_dict):
        self.__class1 = class1
        self.__class2 = class2
        self.__class3 = class3
        self.__class4 = class4
        self.__class5 = class5
        self.__class6 = class6
        self.__class7 = class7
        self.__class8 = class8
        self.__class9 = class9
        self.__output_dict = output_dict

    def increment_class1(self):
        self.__class1 += 1

    def increment_class2(self):
        self.__class2 += 1

    def increment_class3(self):
        self.__class3 += 1

    def increment_class4(self):
        self.__class4 += 1

    def increment_class5(self):
        self.__class5 += 1

    def increment_class6(self):
        self.__class6 += 1

    def increment_class7(self):
        self.__class7 += 1

    def increment_class8(self):
        self.__class8 += 1

    def increment_class9(self):
        self.__class9 += 1

    def set_output_list(self, output_dict):
        self.__output_dict = output_dict

    def get_class1(self):
        return self.__class1

    def get_class2(self):
        return self.__class2

    def get_class3(self):
        return self.__class3

    def get_class4(self):
        return self.__class4

    def get_class5(self):
        return self.__class5

    def get_class6(self):
        return self.__class6

    def get_class7(self):
        return self.__class7

    def get_class8(self):
        return self.__class8

    def get_class9(self):
        return self.__class9

    def get_output_list(self):
        return self.__output_dict


def prepare_actual_output_dict(file_path):
    output_dict = {}
    classes_count = ClassesCount(0, 0, 0, 0, 0, 0, 0, 0, 0, output_dict)
    with open(file_path, 'rb') as f:
        reader = csv.reader(f)
        converted_list = list(reader)
        for line in converted_list:
            split_line = line.split(",")
            output_dict[split_line[0]] = split_line[1]
            if split_line[1] == "class1":
                classes_count.increment_class1()
            elif split_line[1] == "class2":
                classes_count.increment_class2()
            elif split_line[1] == "class3":
                classes_count.increment_class3()
            elif split_line[1] == "class4":
                classes_count.increment_class4()
            elif split_line[1] == "class5":
                classes_count.increment_class5()
            elif split_line[1] == "class6":
                classes_count.increment_class6()
            elif split_line[1] == "class7":
                classes_count.increment_class7()
            elif split_line[1] == "class8":
                classes_count.increment_class8()
            elif split_line[1] == "class9":
                classes_count.increment_class9()
        classes_count.set_output_list(output_dict)
    return classes_count


def build_confusion_matrix(classes_count, result_coll, output_file_path):
    cursor = result_coll.find()
    actual_class_count = ClassesCount(0, 0, 0, 0, 0, 0, 0, 0, 0, {})
    for document in cursor:
        if classes_count.get_output_list(document["filename"]) == document["class"]:
            if document["class"] == "class1":
                actual_class_count.increment_class1()
            elif document["class"] == "class2":
                actual_class_count.increment_class2()
            elif document["class"] == "class3":
                actual_class_count.increment_class3()
            elif document["class"] == "class4":
                actual_class_count.increment_class4()
            elif document["class"] == "class5":
                actual_class_count.increment_class5()
            elif document["class"] == "class6":
                actual_class_count.increment_class6()
            elif document["class"] == "class7":
                actual_class_count.increment_class7()
            elif document["class"] == "class8":
                actual_class_count.increment_class8()
            elif document["class"] == "class9":
                actual_class_count.increment_class9()
    f = open(output_file_path, 'w')
    f.write("Class1: " + ((actual_class_count.get_class1() / classes_count.get_class1()) * 100))
    f.write("Class1: " + ((actual_class_count.get_class2() / classes_count.get_class2()) * 100))
    f.write("Class1: " + ((actual_class_count.get_class3() / classes_count.get_class3()) * 100))
    f.write("Class1: " + ((actual_class_count.get_class4() / classes_count.get_class4()) * 100))
    f.write("Class1: " + ((actual_class_count.get_class5() / classes_count.get_class5()) * 100))
    f.write("Class1: " + ((actual_class_count.get_class6() / classes_count.get_class6()) * 100))
    f.write("Class1: " + ((actual_class_count.get_class7() / classes_count.get_class7()) * 100))
    f.write("Class1: " + ((actual_class_count.get_class8() / classes_count.get_class8()) * 100))
    f.write("Class1: " + ((actual_class_count.get_class9() / classes_count.get_class9()) * 100))
    f.close()


def get_log_file_path(directory_path):
    current_date_time = str(time.time())
    current_date_time = current_date_time.replace(":", "")
    logfile = directory_path + 'log_file' + '_' + current_date_time + '.txt'
    return logfile


if __name__ == "__main__":
    startTime = time.time()
    filePath = sys.argv[0]
    outputPath = sys.argv[1]
    log_file = '/home/sidsurrexion/Microsoft/Log'
    logPath = get_log_file_path(log_file)
    logMessages = 'Process: Purge \n'
    try:
        from pymongo import MongoClient

        client = MongoClient()
        db = client.organisms
        result = db.result
        build_confusion_matrix(prepare_actual_output_dict(filePath), result, outputPath)
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