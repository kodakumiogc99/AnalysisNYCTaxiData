import datatable as db
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', '--fold', type=str, default='/BigData/BigData/HW1/')

    args = parser.parse_args()

    fold = args.fold
