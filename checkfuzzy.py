# -*- coding: utf-8 -*-
import pandas as pd
import re
import numpy as np
from underthesea import word_tokenize
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import time

df_data = pd.read_csv('[Hung San]List all data from social - Crawl Data.csv', encoding = 'utf-8')
df_test = pd.read_csv('[Hung San]List all data from social - Copy of Crawl Data.csv', encoding = 'utf-8')

set_abbreviate = { 'phòng ngủ': ['pn', 'phn'],
            'phòng khách': ['pk', 'phk'],
            'phòng vệ sinh': ['wc', 'tolet', 'toilet'],
            'hợp đồng': ['hđ', 'hd'],
            'đầy đủ': ['full'],
            'nhỏ': ['mini'],
            'tầm nhìn': ['view'],
            'địa chỉ': ['đc', 'đ/c'],
            'miễn phí': ['free'],
            'vân vân' : ['vv'],
            'liên hệ' : ['lh'],
            'trung tâm thành phố': ['tttp'],
            'yêu cầu': ['order'],
            'công viên': ['cv', 'cvien'],
            'triệu /' : ['tr/', ' tr /', 'tr '],
            'phường' : [' p ', ' ph '],
            'quận' : [' q ', ' qu ']
            }
def read_txt(f_path):
    f = open(f_path, encoding="utf8")
    if f.mode == 'r':
        content = f.read()
    return content.split('\n')

stopwords = read_txt('stopwords.txt')

# Remove stopwords
def remove_stopword(text):
    tokens = word_tokenize(text)
    return " ".join(word for word in tokens if word not in stopwords)

def replace_abbreviate(s):
    for key in set_abbreviate:
        s = re.sub('|'.join(set_abbreviate[key]),' {} '.format(key), s)
    return s

def text_format(df):
    arr_description = []
    for index in range(len(df.index)):
        arr = [re.sub('[+|()]', ' ', line.lower()) for line in df.iloc[index]["Content"].split('\n')]
        arr = [re.sub('[.]', '', line) for line in arr if line != '']
        arr = [replace_abbreviate(line) for line in arr]
        arr = [re.sub('[^0-9A-Za-z ạảãàáâậầấẩẫăắằặẳẵóòọõỏôộổỗồốơờớợởỡéèẻẹẽêếềệểễúùụủũưựữửừứíìịỉĩýỳỷỵỹđ/%*-,]', ' ', line) for line in arr]
        arr = [re.sub('m2', ' m2', line) for line in arr]
        arr = [" ".join(line.split()) for line in arr]
        arr_description.append(remove_stopword(" ".join(arr)))
    return arr_description

df_data['Processed_content'] = text_format(df_data.iloc[:,:1])
df_test['Processed_content'] = text_format(df_test.iloc[:,:1])

dups = []
for i in df_test.index:
    dups.append(process.extract(df_test['Processed_content'][i], df_data['Processed_content'], limit=3, scorer=fuzz.token_sort_ratio))

df_test = df_test.drop(['Processed_content'], axis=1)
df_data = df_data.drop(['Processed_content'], axis=1)
df_test['duplicates'] = ['']*len(df_test.index)
df_data['duplicates'] = ['']*len(df_data.index)
df_test['new'] = ['1']*len(df_test.index)
df_data['new'] = ['0']*len(df_data.index)

dup_code = 0
for index in df_test.index:
    for i in range(3):
        if dups[index][i][1] > 80:
            df_test['duplicates'][index] = str(dup_code)
            df_data['duplicates'][dups[index][i][2]] = str(dup_code)
    dup_code += 1

df_data = df_data.append(df_test, ignore_index=True)

df_data.to_csv('output.csv')




