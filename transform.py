import os
from typing import Union
import pandas as pd


class Transform:
    def __init__(self, path):
        self.path = path
        self.file = 'PPR-ALL.csv'

    @staticmethod
    def load_csv(path: str) -> Union[pd.DataFrame, bool]:
        """
        Read csv file, returns a dataframe or false
        :param path: object
        :type path: str
        :return: pd.object
        :rtype: object pandas.core.indexes.base.Index
        """
        try:
            df = pd.read_csv(
                path,
                sep=",",
                encoding="latin-1",
                dtype=object)

            return df
        except Exception as e:
            print("Exception %s" % e)
            return False

    @staticmethod
    def transform_names(cols: object) -> object:
        """
        Clean content of parentheses on columns

        :param cols: pd.object
        :type cols: object pandas.core.indexes.base.Index
        :return: pd.object
        :rtype: object pandas.core.indexes.base.Index
        """
        cols = cols.str.replace(r"\(.*\)", "").str.strip()
        return cols

    @staticmethod
    def transform_rename(df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename columns and applies best practices

        :param df: pd.DataFrame
        :type df:object pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        """

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

    @staticmethod
    def transform_date(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms column to date type and creates a new column
        with first day of the month

        :param df: pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        :type df:object pd.DataFrame
        """
        df['month_start'] = df['sales_date']
        df['sales_date'] = pd.to_datetime(df.sales_date,
                                          dayfirst=True,
                                          format='%d/%m/%Y').dt.strftime('%d-%m-%Y')
        df['month_start'] = pd.to_datetime(df.month_start,
                                           dayfirst=True,
                                           format='%d/%m/%Y').dt.strftime('01-%m-%Y')
        return df

    @staticmethod
    def transform_sv(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean sales values and transforms it to integer type.

        :param df: pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        :type df:object pd.DataFrame
        """
        df['sales_value'] = df['sales_value'].apply(lambda x: x.lstrip('\x80'))
        df['sales_value'] = df['sales_value'].apply(lambda x: int(float(x.split()[0].replace(',', '')))).astype(int)
        return df

    @staticmethod
    def transform_strings(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms column types to strings.

        :param df: pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        :type df:object pd.DataFrame
        """
        df['address'] = df['address'].astype('str')
        df['county'] = df['county'].astype('str')
        return df

    @staticmethod
    def created_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates new columns from information on the data frame.

        :param df: pd.DataFrame
        :return: df
        :rtype: object pd.DataFrame
        :type df:object pd.DataFrame
        """
        counties = ['Carlow', 'Cavan', 'Clare', 'Cork', 'Donegal', 'Dublin',
                    'Galway', 'Kerry', 'Kildare', 'Kilkenny', 'Laois',
                    'Leitrim', 'Limerick', 'Longford', 'Louth', 'Mayo',
                    'Meath', 'Monaghan', 'Offaly', 'Roscommon', 'Sligo',
                    'Tipperary', 'Waterford', 'Westmeath', 'Wexford', 'Wicklow']

        repeated = df[df.duplicated(keep=False)]
        # print("Duplicate Rows except first occurrence based on all columns are :")
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
        self.path = os.path.join(self.path, self.file)
        df = Transform.load_csv(self.path)
        df.columns = Transform.transform_names(df.columns)
        df = Transform.transform_rename(df)
        df = Transform.transform_date(df)
        df = Transform.transform_sv(df)
        df = Transform.transform_strings(df)
        df = Transform.created_values(df)

