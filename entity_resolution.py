from __future__ import print_function
import argparse
import multiprocessing
import csv
import re
import dedupe
from unidecode import unidecode
import os
from multiprocessing import Pool
from toolz.functoolz import compose
from functools import partial


'''Function to pre-process data before training'''


def pre_process(column):
    noise = ['\n', '-', '/', "'", ",", ":", ' +', ":"]
    column = unidecode(column)
    for n in noise:
        column = re.sub(n, ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    if not column:
        column = None
    return column

'''Read data from the input files into the dictionaries'''


def read_data(file_name, data_d):
    print(file_name)
    with open(file_name) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = dict([(k, pre_process(v)) for (k, v) in row.items()])
            if clean_row['row_id']:
                clean_row['row_id'] = str(clean_row['row_id'][1:])
            data_d[file_name + str(i)] = dict(clean_row)

    return data_d

'''Merging two values with the underscore to write into the output'''


def merge(item1, item2):
    return item1 + '_' + item2

'''Filtering linkage records from the files'''


def de_duplicates(cluster_memberships, filename):
    linkage_dict = {}
    with open(filename) as f:
        reader = csv.reader(f)
        for row_id, row in enumerate(reader):
            cluster_details = cluster_memberships.get(filename + str(row_id))
            if cluster_details is not None:
                cluster_id, score = cluster_details
                linkage_dict.setdefault('cluster_id', []).append(cluster_id)
                linkage_dict.setdefault('id', []).append(row[0])
                linkage_dict.setdefault('title', []).append(row[1])
                linkage_dict.setdefault('match', []).append(row[5])

    return linkage_dict


'''Writing output to the csv files'''


def write_output_csv(array_of_dicts, ):

    dblp_dict = array_of_dicts[0]
    scholar_dict = array_of_dicts[1]
    with open(args.output_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(('idDBLP', 'idScholar', 'DBLP_Match', 'Scholar_Match', 'Match_ID'))
        len_dblp = len(dblp_dict['cluster_id'])
        len_scholar = len(scholar_dict['cluster_id'])
        for i in range(0, len_dblp):
            for j in range(0, len_scholar):
                if dblp_dict['cluster_id'][i] == scholar_dict['cluster_id'][j]:
                    row = (dblp_dict['id'][i], scholar_dict['id'][j],
                           dblp_dict['match'][i],
                           scholar_dict['match'][j], merge(dblp_dict['match'][i], scholar_dict['match'][j]))
                    print(row)
                    writer.writerow(row)
                    break


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Perform Entity Resolution')
    parser.add_argument('-db', '--dblp_file', type=str, default='DBLP_test.csv')
    parser.add_argument('-sc', '--scholar_file', type=str, default='Scholar_test.csv')
    parser.add_argument('-tf', '--training_file', default='data_matching_training.json')
    parser.add_argument('-sf', '--setting_file', default='data_matching_learned_settings')
    parser.add_argument('-of', '--output_file', default='data_matching_output.csv')
    parser.add_argument('-p', '--number_of_processes', type=int, default=multiprocessing.cpu_count())
    parser.add_argument('-c', '--chunk_size', type=int, default=1)

    args = parser.parse_args()

    print('reading data')

    dblp_file = read_data(args.dblp_file, {})
    scholar_file = read_data(args.scholar_file, {})

    if os.path.exists(args.setting_file):
        print('reading from', args.setting_file)
        with open(args.setting_file, 'rb') as sf:
            linker = dedupe.StaticRecordLink(sf)
    else:

        print("Training Begins ...")

        fields = [
        {'field': 'title', 'type': 'String', 'has missing': True},
        {'field': 'authors', 'type': 'String', 'has missing': True},
        {'field': 'venue', 'type': 'String', 'has missing': True},
        {'field': 'year', 'type': 'DateTime', 'has missing': True}]

        linker = dedupe.RecordLink(fields)

        linker.sample(dblp_file, scholar_file, 15000)

        if os.path.exists(args.training_file):

            print("reading labeled examples from", os.path.join(args.training_file, 'training_file'))
            with open(os.path.join(args.training_file, 'training_file')) as tf:
                linker.readTraining(tf)

        print('starting active labeling...')
        dedupe.consoleLabel(linker)
        linker.train()

        # when finished, save our training away to disk

        with open(args.training_file, 'w') as tf:
            linker.writeTraining(tf)

        with open(args.setting_file, 'wb') as sf:
            linker.writeSettings(sf)

    # Clustering

    print("Clustering")

    linked_records = linker.match(dblp_file, scholar_file, threshold=0.5)

    print('# duplicate sets', len(linked_records))

    cluster_membership = {}
    cluster_id = None
    for cluster_id, (cluster, score) in enumerate(linked_records):
        for record_id in cluster:
            cluster_membership[record_id] = (cluster_id, score)

    if cluster_id:
        unique_id = cluster_id + 1
    else:
        unique_id = 0

    deduplicate = compose(partial(de_duplicates, cluster_membership))
    files = [args.dblp_file, args.scholar_file]
    p = Pool(args.number_of_processes)

    array_of_dict = []

    for index, data in enumerate(p.imap(deduplicate, files, args.chunk_size)):
        array_of_dict.append(data)

    write_output_csv(array_of_dict)

