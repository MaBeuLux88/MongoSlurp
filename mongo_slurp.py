import csv
import re
import sys
from collections import OrderedDict
from pprint import pprint
from time import time

import dateparser
from bson import ObjectId
from mergedeep import merge
from prettytable import PrettyTable
from pymongo import MongoClient


def check_params(args):
    if len(args) != 5:
        print('ERROR.')
        print('This program need 4 parameters to work.')
        print('Param 1: MongoDB URI. Example: "mongodb://localhost"')
        print('Param 2: Database name')
        print('Param 3: Collection name')
        print('Param 4: Filename to import. Example "persons_10k.csv"')
        exit(1)
    if not args[1].startswith("mongodb"):
        print('ERROR: Param 1 should be a valid MongoDB URI.')
        exit(2)
    if not args[4].endswith('.csv'):
        print('ERROR: Param 2 should be a CSV file. We only support this format at the moment.')
        exit(3)


def get_mongodb_client():
    uri = sys.argv[1]
    return MongoClient(uri)


def get_file_content():
    filename = sys.argv[4]
    with open(filename, encoding='utf-8-sig') as file:
        # todo also handle JSON and other types
        csv_file = csv.DictReader(file)
        docs = []
        for row in csv_file:
            docs.append(OrderedDict(row))
    return docs


def guess_type(value):
    # todo arrays?
    value = str(value)
    if re.match(r'^[1-9]\d*$', value):
        return 'Integer'
    if re.match(r'^\d+[\.,]\d+$', value):
        return 'Double'
    if re.match(r'^(true|false)$', value, re.IGNORECASE):
        return 'Boolean'
    if re.match(r'^ObjectId\([0-9a-f]{24}\)$', value) or re.match(r'^[0-9a-f]{24}$', value):
        return 'ObjectId'
    if re.match(r'^\d{4}[/-]\d{1,2}[/-]\d{1,2}(T.*)?$', value) or re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{4}$', value):
        return 'Date'
    else:
        return 'String'


def guess_types_and_values(doc):
    types = []
    for k, v in doc.items():
        types.append((k, guess_type(v), 'doc[\'' + k + '\']'))
    return types


def parse(value, field_type):
    try:
        if field_type == 'Integer':
            return int(value)
        if field_type == 'Double':
            return float(value)
        if field_type == 'Boolean':
            return bool(re.match(r'^true$', value, re.IGNORECASE))
        if field_type == 'ObjectId':
            return ObjectId(re.search(r'([0-9a-f]{24})', value).group(1))
        if field_type == 'Date':
            return dateparser.parse(value)
        return value
    except Exception:
        print('ERROR: Parsing', value, 'into', field_type, 'failed.', file=sys.stderr)
        return value


def create_final_documents(docs, types):
    start = time()
    mdb_docs = []
    for doc in docs:
        mdb_doc = OrderedDict()
        for t in types:
            add_value_in_mdb_doc(mdb_doc, doc, t)
        mdb_docs.append(mdb_doc)
    print('\nCreating MDB documents in', round(time() - start, 2), 's')
    return mdb_docs


def add_value_in_mdb_doc(mdb_doc, doc, value_typed):
    fname, ftype, fvalue = value_typed
    sub_doc1 = {}
    sub_doc2 = {}
    fields = fname.split('.')
    try:
        value = eval(fvalue)
    except NameError:
        value = fvalue
    sub_doc1[fields.pop()] = parse(value, ftype)
    for f in reversed(fields):
        sub_doc2[f] = sub_doc1
        sub_doc1 = sub_doc2
        sub_doc2 = {}
    merge(mdb_doc, sub_doc1)


def main():
    check_params(sys.argv)
    client = get_mongodb_client()
    collection = client.get_database(sys.argv[2]).get_collection(sys.argv[3])
    collection.drop()

    docs_csv = get_file_content()
    types = guess_types_and_values(docs_csv[0])

    x = PrettyTable(['Field Name', 'Value'])
    x.title = 'Raw document based on first line of data in CSV.'
    for i in docs_csv[0].items():
        x.add_row(i)
    print(x)
    print()

    x = PrettyTable(['Field Name', 'Type Detected', 'Value'])
    x.title = 'Types and values that have been guessed.'
    for t in types:
        x.add_row(t)
    print(x)
    print()

    print('Now is your chance to fix the table above if something wasn\'t guessed correctly.')
    print('Start from this line and send something similar.\n')
    print(types)

    types = eval(input('\nWhat will your document look like?\n'))

    mdb_docs = create_final_documents(docs_csv, types)
    start = time()
    collection.insert_many(mdb_docs)
    print('\nDocs inserted in MongoDB in', round(time() - start, 2), 's')

    doc = collection.find_one()
    print('\nOne document that has been inserted:\n')
    pprint(doc)


if __name__ == '__main__':
    main()
