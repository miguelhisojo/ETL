import os
from typing import Union
import pandas as pd
from zipfile import ZipFile

import logging

# create logger
module_logger = logging.getLogger('etl_application.transform')


class Transform:
    def __init__(self, path, file_name):
        self.path = path
        self.file = 'PPR-ALL.csv'
        self.file_name = file_name
        self.logger = logging.getLogger('etl_application.transform.Transform')
        self.logger.info('creating an instance of Transform')

    def unpack_csv(self):
        zip_file = os.path.join(self.path, self.file_name)
        try:

            with ZipFile(zip_file, 'r') as zipObj:
                self.logger.info(f'\t - Decompressing {zip_file}')
                zipObj.extractall(self.path)

            self.logger.info(f"\t -File {zip_file} ready at {self.path}")

        except (ValueError, Exception):
            self.logger.exception(" decompressing error : ", exc_info=True)

    def load_csv(self, path: str) -> Union[pd.DataFrame, bool]:
        """
        Read csv file, returns a dataframe or false
        :param path: object
        :type path: str
        :return: pd.object
        :rtype: object pandas.core.indexes.base.Index
        """
        try:
            self.logger.info("\t - Loading CSV file as Dataframe")
            df = pd.read_csv(
                path,
                sep=",",
                encoding="latin-1",
                dtype=object)

            self.logger.info("\t - Data Frame ready")
            return df
        except (ValueError, Exception):
            self.logger.exception("Exception ", exc_info=True)

    def transform_names(self, cols: object) -> object:
        """
        Clean content of parentheses on columns

        :param cols: pd.object
        :type cols: object pandas.core.indexes.base.Index
        :return: pd.object
        :rtype: object pandas.core.indexes.base.Index
        """
        self.logger.info("\t - Cleaning columns names")
        cols = cols.str.replace(r"\(.*\)", "").str.strip()
        return cols

    def transform_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename columns and applies best practices

        :param df: pd.DataFrame
        :type df:object pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        """
        self.logger.info("\t - Applying custom names and format")
        df = df.rename(columns={'Date of Sale': 'sales_date',
                                'Address': 'address',
                                'County': 'county',
                                'Price': 'sales_value',
                                'Not Full Market Price': 'not_full_market_price_ind',
                                'VAT Exclusive': 'vat_exclusive_ind'
                                })
        df.columns = df.columns.str.lower() \
            .str.replace(' ', '_') \
            .str.replace('(', '') \
            .str.replace(')', '')
        return df

    def transform_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms column to date type and creates a new column
        with first day of the month

        :param df: pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        :type df:object pd.DataFrame
        """
        self.logger.info("\t - Converting dates to date type")
        df['month_start'] = df['sales_date']
        df['sales_date'] = pd.to_datetime(df.sales_date,
                                          dayfirst=True,
                                          format='%d/%m/%Y').dt.strftime('%d-%m-%Y')
        df['month_start'] = pd.to_datetime(df.month_start,
                                           dayfirst=True,
                                           format='%d/%m/%Y').dt.strftime('01-%m-%Y')
        return df

    def transform_sv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean sales values and transforms it to integer type.

        :param df: pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        :type df:object pd.DataFrame
        """

        self.logger.info("\t - Transforming sales values")
        df['sales_value'] = df['sales_value'].apply(lambda x: x.lstrip('\x80'))
        df['sales_value'] = df['sales_value'].apply(lambda x: int(float(x.split()[0].replace(',', '')))).astype(int)
        return df

    def transform_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms column types to strings.

        :param df: pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        :type df:object pd.DataFrame
        """
        self.logger.info("\t - Applying format to address and county values")
        df['address'] = df['address'].astype('str')
        df['county'] = df['county'].astype('str')
        return df

    def created_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates new columns from information on the data frame.

        :param df: pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        :type df:object pd.DataFrame
        """

        self.logger.info("\t - Flagging repeated values, and creating new columns")
        counties = ['Carlow', 'Cavan', 'Clare', 'Cork', 'Donegal', 'Dublin',
                    'Galway', 'Kerry', 'Kildare', 'Kilkenny', 'Laois',
                    'Leitrim', 'Limerick', 'Longford', 'Louth', 'Mayo',
                    'Meath', 'Monaghan', 'Offaly', 'Roscommon', 'Sligo',
                    'Tipperary', 'Waterford', 'Westmeath', 'Wexford', 'Wicklow']

        repeated = df[df.duplicated(keep=False)]
        self.logger.info("Duplicate Rows except first occurrence based on all columns are :")
        df['quarantine_ind'] = 0
        df['quarantine_code'] = ''
        df.loc[repeated.index, 'quarantine_ind'] = 1
        df.loc[repeated.index, 'quarantine_code'] = 'not unique record'

        df['vbs'] = (~df['county'].isin(counties)).astype(int)

        df.loc[df.loc[df['vbs'] == 1].index, 'quarantine_ind'] = 1
        df.loc[repeated.index, 'quarantine_code'] = 'not irish counties'

        df['new_home_ind'] = 0
        df['new_home_ind'] = (df['description_of_property'].str.match(r'(^New.*)') == True).astype(int)

        del repeated
        return df

    def main(self):
        Transform.unpack_csv(self)
        path = os.path.join(self.path, self.file)
        df = Transform.load_csv(self, path)
        df.columns = Transform.transform_names(self, df.columns)
        df = Transform.transform_rename(self, df)
        df = Transform.transform_date(self, df)
        df = Transform.transform_sv(self, df)
        df = Transform.transform_strings(self, df)
        df = Transform.created_values(self, df)

        print(path)
        df.to_csv(path, header=True)

        return path
