import csv
import random
import string
from optparse import OptionParser
import shutil
import sqlite3
import datetime
import time
import subprocess

FIELDNAMES = ["Data", "Type"]
NUMBEROFLINES = 10 ** 5


def timeit(method):
    """
    Decorator for time measure
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print("{} completed within {:.4f} ms".format(method.__name__, (te - ts) * 1000))
        return result
    return timed


def random_generator(size=16, chars=string.ascii_uppercase + string.digits):
    """For generating random values in Data column"""
    return "".join(random.choice(chars) for _ in range(size))


@timeit
def new_file():
    """
    make initial csv file
    """
    print("Creating new csv file")
    with open("initial.csv", "w") as f:
        writer = csv.DictWriter(f, delimiter=";", fieldnames=FIELDNAMES)
        writer.writeheader()
        for line in range(NUMBEROFLINES):
            writer.writerow({
                "Data": random_generator(),
                "Type": "Initial_type"})
    print("New file created!")


@timeit
def update_file():
    """
    update initial.csv file with new values from TestData.csv
    """
    print("Updating csv file")
    data_to_add = []

    with open("TestData.csv", "r") as f:
        reader = csv.DictReader(f, delimiter=";", fieldnames=FIELDNAMES)
        next(reader, None)  # skip the headers
        for row in reader:
            data_to_add.append(row)

    initial_data = []
    with open("initial.csv", "r") as f:
        reader = csv.DictReader(f, delimiter=";", fieldnames=FIELDNAMES)
        next(reader, None)  # skip the headers
        for row in reader:
            initial_data.append(row)

    for item in data_to_add:
        position = random.randint(1, 1000)
        initial_data.insert(position, item)

    data_to_add = initial_data

    with open("initial.csv", "w") as f:
        writer = csv.DictWriter(f, delimiter=";", fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in data_to_add:
            writer.writerow(row)
    print("File updated!")


@timeit
def copy_file():
    """ Copy initial.csv before update """
    shutil.copy(r"initial.csv", r"initial_old.csv")


@timeit
def compare_files():  # method 1
    """
    Method 1
    Compare 2 file versions: before and after updating
    """
    new_data = []

    with open("initial_old.csv", "r") as f_old:
        with open("initial.csv", "r") as f_new:
            old_line = f_old.readline()
            new_line = f_new.readline()

            while old_line:
                if new_line == old_line:
                    old_line = f_old.readline()
                    new_line = f_new.readline()
                else:
                    new_data.append(new_line)
                    new_line = f_new.readline()

    with open("new_lines.csv", "w") as f:
        f.write(";".join(FIELDNAMES) + "\n")
        for row in new_data:
            f.write(row)


def drop_table():
    """
    Delete table in DB for correct test
    """
    con = sqlite3.connect("test.db")
    cur = con.cursor()
    sql = "drop TABLE datatypes;"
    cur.execute(sql)
    cur.close()
    con.close()


def create_table():
    """
    Create table in DB
    """
    con = sqlite3.connect("test.db")
    cur = con.cursor()
    sql = """CREATE TABLE datatypes (
            id INTEGER PRIMARY KEY, 
            data TEXT UNIQUE ON CONFLICT IGNORE, 
            types TEXT,
            date NUMERIC
            );"""
    cur.execute(sql)
    cur.close()
    con.close()



@timeit
def db_upload_with_old_values():
    """
    Method 2
    Upload old data in sqlite database
    """
    with open("initial_old.csv", "r") as f_old:
        f_old.readline()
        data_parsed = list(f_old.readline().strip().split(";"))
        # set connection with DB
        con = sqlite3.connect("test.db")
        cur = con.cursor()
        while len(data_parsed) > 1:
            old_line = (data_parsed[0], data_parsed[1], datetime.date(year=2017, month=2, day=1))

            sql = "INSERT INTO datatypes (data, types, date) VALUES (?, ?, ?);"
            cur.execute(sql, old_line)
            con.commit()
            data_parsed = list(f_old.readline().strip().split(";"))
        # close connection with DB
        cur.close()
        con.close()
    print("Old values uploaded to DB")


@timeit
def db_upload_with_new_values():
    """
    Method 2
    Upload new data in sqlite database
    """

    with open("initial.csv", "r") as f_new:
        f_new.readline()
        data_parsed = list(f_new.readline().strip().rsplit(sep=";", maxsplit=1))
        # set connection with DB
        con = sqlite3.connect("test.db")
        cur = con.cursor()
        while len(data_parsed) > 1:
            new_line = (data_parsed[0], data_parsed[1], datetime.date.today())

            sql = "INSERT INTO datatypes (data, types, date) VALUES (?, ?, ?);"
            cur.execute(sql, new_line)
            con.commit()
            data_parsed = list(f_new.readline().strip().rsplit(sep=";", maxsplit=1))
        # close connection with DB
        cur.close()
        con.close()

    print("New values uploaded to DB")

    with open("new_lines.csv", "w") as f:
        f.write(";".join(FIELDNAMES) + "\n")

        today = (datetime.date.today(),)

        con = sqlite3.connect("test.db")
        cur = con.cursor()
        sql = "SELECT * FROM datatypes WHERE date=(?)"
        cur.execute(sql, today)

        for item in cur.fetchall():
            data = ";".join((item[1], item[2] + "\n"))
            f.write(data)
        cur.close()
        con.close()


@timeit
def git_update():
    """
    Method 3
    Add and commit old version of file
    """
    process = subprocess.Popen(['git', 'add', '.'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = process.communicate()
    process.kill()
    process = subprocess.Popen(['git', 'commit', '-m', '"updating"'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = process.communicate()
    process.kill()


@timeit
def git_check():
    """
    Method 3
    Checking new lines in file with Git.
    bash_command = "git diff | grep '^\+[^+]' | cut -c 2- > new_lines.csv
    """
    process1 = subprocess.Popen(["git", "diff"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process2 = subprocess.Popen(["grep", "^\+[^+]"], stdin=process1.stdout, stdout=subprocess.PIPE)
    process1.stdout.close()
    process3 = subprocess.Popen(["cut", "-c", "2-"], stdin=process2.stdout, stdout=subprocess.PIPE)
    process2.stdout.close()
    outs, errs = process3.communicate()

    new_data = outs.decode("utf-8")
    with open("new_lines.csv", "w") as f:
        f.write(";".join(FIELDNAMES) + "\n")
        for row in new_data:
            f.write(row)


if __name__ == "__main__":
    # additional option to start file
    parser = OptionParser()

    parser.add_option('-c', '--createcsv', dest='create',
                      action='store_true', default=False,
                      help="create file with initial values")
    parser.add_option('-u', '--updatecsv', dest='update',
                      action='store_true', default=False,
                      help="update csv file with new values and save previous version")
    parser.add_option('--method1', dest='method1',
                      action='store_true', default=False,
                      help="compare 2 files")
    parser.add_option('--method2', dest='method2',
                      action='store_true', default=False,
                      help="using DB")
    parser.add_option('--method3', dest='method3',
                      action='store_true', default=False,
                      help="using Git")

    # Check if file has options
    options, _ = parser.parse_args()

    if options.create:
        new_file()

    elif options.update:
        copy_file()
        update_file()

    elif options.method1:
        new_file()
        copy_file()
        update_file()
        compare_files()

    elif options.method2:
        new_file()
        copy_file()
        update_file()
        drop_table()
        create_table()
        db_upload_with_old_values()
        db_upload_with_new_values()

    elif options.method3:
        new_file()
        git_update()
        update_file()
        git_check()


