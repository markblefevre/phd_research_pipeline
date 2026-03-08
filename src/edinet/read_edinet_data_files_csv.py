# -*- coding: utf-8 -*-
"""
Created on Wed May 15 22:34:26 2024

@author: Mark
"""
from pathlib import Path
import pandas as pd
import os

def convert_path(win_path):
    ph = str(Path.home())
    rehomed = win_path.replace('C:\\Users\\Mark', ph)
    return rehomed.replace('\\','/')

def read_edinet_data_files_csv(directory=Path.home() / 'Documents' / 'Education' / '2021 EDHEC Exec PhD' / '4 Research' / 'EDINET Information',
                      filename='edinet_data_files.csv'):
    # Construct the file path using os.path.join
    file_path = directory / filename

    # Open the file with correct error handling and read CSV
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        df = pd.read_csv(file, usecols=['File','edinet_code','fiscal_year',
                                        'fiscal_month','fiscal_period_kind',
                                        'company_name','company_name_en',
                                        'FileName'])
    # Convert path, if necessary
    if os.name == 'posix':
        df['File'] = df['File'].apply(convert_path)
    return df