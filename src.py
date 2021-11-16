#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import operator
from tqdm import tqdm

import sqlite3


class DataBase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.database = None
        self.cursor = None

    def initialize(self):
        self.database = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.database.cursor()

    def close(self):
        self.cursor.close()
        self.database.close()

    # def
    _cmd = f"CREATE TABLE submissions (title text, url text, avg_score float, scores text"


class MetaData:
    def __init__(self, data_root: str):
        self.items = []
        if data_root is not None:
            file_list = sorted(os.listdir(data_root))
            self.ttl_items = len(file_list)

            # read each item
            for i in tqdm(range(self.ttl_items)):
                with open(os.path.join(data_root, f'{i}.txt'), 'r') as f:
                    item = json.load(f)

                self.items.append(item)

    def get_all_values_by_key(self, key: str = ''):
        return list(map(operator.itemgetter(key), self.items))

    def get_by_id(self, index: int):
        return self.items[index]

    def __len__(self):
        return self.ttl_items
