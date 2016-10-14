from urlparse import unquote
from unidecode import unidecode
import os, sys, shutil, re
import pyspark as ps
from time import time

if 'sc' not in locals():
    sc = ps.SparkContext('local[40]')
sc.setLogLevel("ERROR")

RE = re.compile(r'''^\(['\"](.*)['\"], (\d*)\)''')

def get_en(p):
    if len(p) == 4:
        label, place, count, _ = p
    else:
        return None
    if label != 'en':
        return None
    return True


def grab_info(p):
    l, place, count, _ = p
    return (unidecode(unquote(place)), int(count))


def process_folder(input_folder, write_folder):
    rdd = sc.textFile('{}/*'.format(input_folder))
    rdd = rdd.map(lambda x: x.split()).filter(get_en).map(grab_info)
    rdd.saveAsTextFile(write_folder)
    rdd.unpersist()


def process_line(line):
    matches = RE.search(line)
    if matches:
        return (matches.group(1), int(matches.group(2)))
    else:
        return (None, 0)


def second_pass(input_folder, write_folder):
    rdd = sc.textFile('{}/*'.format(input_folder))
    rdd = rdd.map(process_line).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1]>0)
    rdd = rdd.sortBy(lambda x: -x[1])
    rdd.saveAsTextFile(write_folder)
    rdd.unpersist()


def main():
    st = time()
    day_folder = sys.argv[1]
    write_folder = '{}-tmp'.format(day_folder)
    if os.path.isdir(day_folder):
        if len(os.listdir(day_folder)) > 1:
            process_folder(day_folder, write_folder)
            print 'starting second phase'
            second_pass(write_folder, '{}-processed'.format(day_folder))
            shutil.rmtree(write_folder)
            shutil.rmtree(day_folder)
    et = time()
    with open('timing_log.txt','a') as f:
        f.write('day {}: {}\n'.format(day_folder, et-st))

if __name__ == '__main__':
    main()