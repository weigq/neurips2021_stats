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


    def initialize(self, create: bool = False):
        self.database = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.database.cursor()

        if create:
            _cmd = f"CREATE TABLE submissions " \
                   f"(id int, url text, title text, keywords text, authors text, " \
                   f"num_decision int, final_decision text, now_decision text, " \
                   f"num_rating int, rating_avg float, rating_std float, ratings text)"
            self.cursor.execute(_cmd)

    def write_item(self,
                   _id: int, url: str, title: str,
                   keywords: list, authors: str,
                   num_decision: int, final_decision: str, now_decision: str,
                   ratings: list):
        title = title.replace('\\', '').replace("\"", "'")
        num_rating = len(ratings)
        rating_avg = np.mean(ratings).item()
        rating_std = np.std(ratings).item()
        ratings = ', '.join(map(str, ratings))
        _cmd = f"insert into submissions values ( " \
               f"'{_id}', \"{url}\", \"{title}\", \"{keywords}\", \"{authors}\", " \
               f"'{num_decision}', \"{final_decision}\", \"{now_decision}\", " \
               f"'{num_rating}', '{rating_avg}', " \
               f"'{rating_std}', '{ratings}'" \
               f" )"
        self.cursor.execute(_cmd)
        self.database.commit()


    def close(self):
        self.cursor.close()
        self.database.close()
