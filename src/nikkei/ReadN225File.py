# -*- coding: utf-8 -*-
"""
Created on Tue May  7 00:03:44 2024

@author: Mark
"""
import pandas as pd
from pathlib import Path
import os

def read_n225_file(directory=Path.home() / 'Documents' / 'Education' / '2021 EDHEC Exec PhD' / '4 Research' / 'Reference',
                   filename='Nikkei225Constituents.txt'):
    # Construct the full file path
    filepath = os.path.join(directory, filename)
    
    # Initialize an empty list to store the data
    data = []
    current_industry = None

    # Open and read the file
    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            if '\t' not in line:
                # This line is an industry name
                current_industry = line.strip()
            elif 'Code\tCompany Name' in line:
                # This line is a header line and should be ignored
                continue
            else:
                # This line contains code and company name
                code, company_name = line.strip().split('\t')
                data.append({'Industry': current_industry, 'Code': code, 'Company Name': company_name})

    # Create a DataFrame
    df = pd.DataFrame(data)
    return df