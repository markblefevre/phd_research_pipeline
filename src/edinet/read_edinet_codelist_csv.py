# -*- coding: utf-8 -*-
"""
Created on Sun May  5 23:06:30 2024

@author: Mark

Utility Functions for Reading in EdinetcodeDlInfo.csv files
There are English and Japanese versions available for download
<insert a reference here where the files can be obtained>
"""
import pandas as pd
from pathlib import Path

def read_eng_csv_sjis(directory: Path, filename: Path) -> tuple[list[str], pd.DataFrame]:
    """
    Reads an English EDINET CSV (Shift-JIS encoded).
    Parameters
    ----------
    directory : Path
        Directory where the CSV is located.
    filename : Path
        Filename of the CSV.
    Returns
    -------
    tuple: (header list, pandas.DataFrame)
    """
    file_path = (directory / filename).expanduser().resolve()
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, 'r', encoding='shift_jis', errors='replace') as file:
        first_line = file.readline()
        first_line_values = first_line.strip().split(',')
        second_line = file.readline()
        num_columns = len(second_line.split(','))

    dtype_dict = {num_columns - 1: 'Int64'}

    with open(file_path, 'r', encoding='shift_jis', errors='replace') as file:
        df = pd.read_csv(file, skiprows=1, dtype=dtype_dict)

    return first_line_values, df


def read_jpn_csv_sjis(directory: Path, filename: Path) -> tuple[list[str], pd.DataFrame]:
    """
    Reads a Japanese EDINET CSV (Shift-JIS encoded).
    Parameters
    ----------
    directory : Path
        Directory where the CSV is located.
    filename : Path
        Filename of the CSV.
    Returns
    -------
    tuple: (header list, pandas.DataFrame)
    """
    file_path = (directory / filename).expanduser().resolve()
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, 'r', encoding='shift_jis', errors='replace') as file:
        first_line = file.readline()
        first_line_values = first_line.strip().split(',')
        second_line = file.readline()
        num_columns = len(second_line.split(','))

    dtype_dict = {num_columns - 1: 'Int64'}

    with open(file_path, 'r', encoding='shift_jis', errors='replace') as file:
        df = pd.read_csv(file, skiprows=1, dtype=dtype_dict)

    return first_line_values, df

def filter_eng_listedcompanies(df):
    """
    Filters and returns rows from the DataFrame where the company status is 'Listed company'.
    
    This function specifically checks the "Listed company / Unlisted company" column in the provided
    DataFrame and returns a new DataFrame containing only the rows where this column's value is
    "Listed company".

    Args:
        df (pandas.DataFrame): The DataFrame to filter, which must include a column named
                               "Listed company / Unlisted company".

    Returns:
        pandas.DataFrame: A DataFrame containing only the rows from the original DataFrame where
                          the "Listed company / Unlisted company" column has the value "Listed company".
                          
    Example:
        Assuming `original_df` is a pandas DataFrame with a column 'Listed company / Unlisted company':
        
        >>> filtered_df = filter_eng_listedcompanies(original_df)
        >>> print(filtered_df)
        
        This will print the DataFrame with only rows where the company is listed.
    """
    count = len(df)
    print(f'Unfiltered rows: {count}')
    df = df[df["Listed company / Unlisted company"] == "Listed company"]
    count = len(df)
    print(f'Listed companies: {count}')
    df = df[df["Type of Submitter"] == '内国法人・組合']
    count = len(df)
    print(f'Domestic corporations/unions: {count}')
    df = df[df['Securities Identification Code'].apply(str).apply(len)!=3]
    count = len(df)
    print(f'Valid SICs: {count}')
    return df

def filter_jpn_listedcompanies(df):
    """
    Filters and returns rows from the DataFrame where the company status is '上場' (listed).

    This function examines the "上場区分" column in the provided DataFrame and returns a new DataFrame
    containing only the rows where this column's value is "上場", which means 'listed' in English.
    This is useful for focusing analysis on publicly traded companies within datasets that include
    various types of company statuses.

    Args:
        df (pandas.DataFrame): The DataFrame to filter, which must include a column named
                               "上場区分" (Listing classification).

    Returns:
        pandas.DataFrame: A DataFrame containing only the rows from the original DataFrame where
                          the "上場区分" column has the value "上場" (listed).
                          
    Example:
        Assuming `original_df` is a pandas DataFrame with a column '上場区分':
        
        >>> filtered_df = filter_jpn_listedcompanies(original_df)
        >>> print(filtered_df)
        
        This will print the DataFrame with only rows where the company is listed, filtering out unlisted companies.
    """
    count = len(df)
    print(f'Unfiltered rows: {count}')
    df = df[df["上場区分"] == "上場"]
    count = len(df)
    print(f'上場: {count}')
    df = df[df['提出者種別'] == '内国法人・組合']
    count = len(df)
    print(f'内国法人・組合: {count}')
    df = df[df['証券コード'].apply(str).apply(len)!=3]
    count = len(df)
    print(f'Valid 証券コード: {count}')
    return df

def get_eng_sorted_industries():
  df = read_eng_csv_sjis()
  df = filter_eng_listedcompanies(df[1])
  industries =  df["Submitter's industry"].drop_duplicates().to_list()
  industries.sort()
  return industries

def get_jpn_sorted_industries():
  df = read_jpn_csv_sjis()
  df = filter_jpn_listedcompanies(df[1])
  industries =  df['提出者業種'].drop_duplicates().to_list()
  industries.sort()
  return industries                  