#!/usr/bin/env python3

import logging
import pandas as pd
from typing import List


def add_surrogate_key(dataframe: pd.DataFrame, ident_name: str) -> pd.DataFrame:
    try:
        dataframe[ident_name] = range(1, len(dataframe) + 1)
        return dataframe
    except Exception as e:
        logging.error(f"Error adding surrogate key: {e}")
        raise


def select_rows(dataframe: pd.DataFrame, property_name: str, property_values: List[str]) -> pd.DataFrame:
    try:
        return dataframe[dataframe[property_name].isin(property_values)]
    except Exception as e:
        logging.error(f"Error selecting rows: {e}")
        raise


def remove_rows(dataframe: pd.DataFrame, property_name: str, property_value: List[str]) -> pd.DataFrame:
    try:
        return dataframe[dataframe[property_name] != property_value]
    except Exception as e:
        logging.error(f"Error selecting rows: {e}")
        raise


def project_columns(dataframe: pd.DataFrame, columns: list) -> pd.DataFrame:
    try:
        return dataframe[columns]
    except Exception as e:
        logging.error(f"Error projecting columns: {e}")
        raise


def rename_columns(dataframe: pd.DataFrame, column_names: dict) -> pd.DataFrame:
    try:
        return dataframe.rename(columns=column_names, copy=False)
    except Exception as e:
        logging.error(f"Error renaming columns: {e}")
        raise


def format_dates(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    try:
        dataframe[column] = (dataframe[column].str.replace(r"(\d+)-(\d+)-(\d+)", r"\3.\2.\1", regex=True))
        return dataframe
    except Exception as e:
        logging.error(f"Error formatting numbers: {e}")
        raise


def join_dataframes(first: pd.DataFrame, second: pd.DataFrame, on: list, how: str) -> pd.DataFrame:
    try:
        return pd.merge(first, second, on=on, how=how)
    except Exception as e:
        logging.error(f"Error joining data: {e}")
        raise


def normalize(dataframe: pd.DataFrame, columns: list, ident_name: str) -> pd.DataFrame:
    try:
        categories = dataframe[columns].copy()
        categories.drop_duplicates(subset=columns, inplace=True)
        categories.sort_values(by=columns, inplace=True)
        categories[ident_name] = range(1, len(categories) + 1)

        dataframe = pd.merge(dataframe, categories, on=columns, how="left")
        dataframe.drop(columns, axis=1, inplace=True)
        return dataframe, categories
    except Exception as e:
        logging.error(f"Error disaggregating columns: {e}")
        raise
