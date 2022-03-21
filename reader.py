import datatable as db
import os, shutil, glob
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', '--fold', type=str, default='/BigData/BigData/HW1/')

    args = parser.parse_args()

    fold = args.fold
    file = glob.glob(f'{fold}*')

    frame = db.Frame()
    [frame.rbind(db.fread(f, fill=True), force=True, bynames=True) for f in file]

    columns = frame.names

    Start_Lon_Unique = frame['Start_Lon'].nunique1()
    print(Start_Lon_Unique)




