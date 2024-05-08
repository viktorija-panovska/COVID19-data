#!/usr/bin/env python3

import os
import sys

from scripts.extract_datasets import extract_datasets
from scripts.transform_datasets import transform_datasets
from scripts.load_datasets import load_datasets


def extract(datasets_dir: str):
    extract_datasets(datasets_dir)


def transform(datasets_dir: str, tables_dir: str):
    if not os.path.exists(datasets_dir):
        extract(datasets_dir)
    transform_datasets(datasets_dir, tables_dir)


def load(datasets_dir: str, tables_dir: str, config_file: str):
    if not os.path.exists(datasets_dir):
        transform(datasets_dir, tables_dir)

    tables = {}
    for file in os.listdir(tables_dir):
        if file[-4:] == '.csv':
            tables[file[:-4]] = os.path.join(tables_dir, file)

    if len(tables) == 0:
        transform(datasets_dir, tables_dir)

    load_datasets(config_file, tables)


if __name__ == '__main__':

    current_dir = os.path.dirname(__file__)
    datasets_dir = os.path.join(current_dir, 'datasets')
    tables_dir = os.path.join(current_dir, 'tables')

    if len(sys.argv) == 1 or sys.argv[1] == 'extract':
        extract(datasets_dir)

    if len(sys.argv) == 1 or sys.argv[1] == 'transform':
        transform(datasets_dir, tables_dir)

    if len(sys.argv) == 1 or sys.argv[1] == 'load':
        config_file = os.path.join(current_dir, 'connection.json')
        load(datasets_dir, tables_dir, config_file)