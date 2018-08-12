import csv
from optparse import OptionParser


FIELDNAMES = ["Data", "Type"]


def read_data(file):
    """
    Read initial data in a list
    """
    raw_data = list()
    with open(file, "r") as f:
        reader = csv.DictReader(f, delimiter=";", fieldnames=FIELDNAMES)
        next(reader, None)  # skip the headers
        for row in reader:
            raw_data.append(row)
    return raw_data


def data_with_category(raw_data):
    """
    Divide data by category
    """
    # Select types
    types = set()
    for elem in raw_data:
        types.add(elem["Type"].strip())
    # Sort out initial values
    data_sorted = dict()
    for elem in types:
        data_sorted[elem] = list()
    for elem in raw_data:
        data_sorted[elem["Type"].strip()].append(elem["Data"])

    return data_sorted, types


if __name__ == "__main__":
    raw_data = read_data("TestData.csv")
    data_sorted, types = data_with_category(raw_data)

    # additional options to start file
    parser = OptionParser()
    parser.add_option('-t', '--types', dest='types',
                      action='store_true', default=False,
                      help="show types list")

    parser.add_option('-s', '--select', dest='select',
                      action='store_true', default=False,
                      help="show data of type")

    # Check if file has options
    options, args = parser.parse_args()
    if options.types:
        print("Current types of data:")
        for item in types:
            print(item)
        print("Type 'python3 task_2.py -s []', where [] - name of type to see reference data. If there are more than "
              "1 word in type, put it in quotes")

    if options.select:
        print("Data for type {}:".format(args[0]), end="\n")
        for item in data_sorted[args[0]]:
            print(item)





