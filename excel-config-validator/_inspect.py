# -*- coding: utf-8 -*-
import json
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

import openpyxl

filepath = r'F:\QSM_TDRS\Trunk\Tools\TDR_res\Excel\[1]新物品表.xlsm'
wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
ws = wb['Thing']

rows = []
for row in ws.iter_rows(min_row=1, max_row=3, values_only=True):
    rows.append([str(c) if c is not None else '' for c in row[:60]])

wb.close()

outpath = os.path.join(os.path.dirname(__file__), 'thing_headers_out.json')
with open(outpath, 'w', encoding='utf-8') as f:
    json.dump(rows, f, ensure_ascii=False, indent=2)

print("DONE: " + outpath)
