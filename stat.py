# -*- coding: utf-8 -*-
# @Author: mithril

from __future__ import unicode_literals, print_function, absolute_import

import os
import jieba
from collections import Counter
import pandas as pd

c = Counter()


for p in os.listdir('./novel'):
    c.update(jieba.lcut(p.rsplit('.', 1)[0]))

df = pd.DataFrame.from_dict(c.most_common()[:30], orient='columns')
df.columns = ['word', 'count']
# df = pd.DataFrame.from_dict(c, orient='index')

print(df)