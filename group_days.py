'''
assumes input of form python group_days.py MONTH NUM_DAYS START_DAY
'''
from process_wiki import process_line, RE, second_pass
import sys


def main():
    month = sys.argv[1]
    days_to_group = sys.argv[2] 
    if len(sys.argv) > 3:
        start_date = sys.argv[3]
    else:
        start_date = 0
    input_folder = "/data/2016{:0>2}-proccessed".format(month)
    write_folder = "/data/2016{:0>2}-grouped-{}-{}".format(month, days_to_group, start_date)
    second_pass(input_folder, write_folder)


if __name__ == '__main__':
    if 'sc' not in locals():
        sc = ps.SparkContext('local[40]')
    sc.setLogLevel("ERROR")
    main()