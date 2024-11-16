import pandas as pd
import numpy as np
from tools import Timer, Memory
from log import Logger


class ETL:
    logger = Logger(__name__, 'app.log')
    def __init__(self, file_path, logger=logger):
        self.file_path = file_path
        self.logger = logger
    
    @Timer(logger)
    def load_excel(self):
        self.df_list = list()
        self.name_df = list()
        try: 
            for sheet_name in pd.ExcelFile(self.file_path).sheet_names:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name)
                self.name_df.append(sheet_name)
                self.df_list.append(df)
                self.logger.debug('Hoja %s cargada' %sheet_name)
            self.logger.info('Archivo cargado exitosamente')
        except:
            self.logger.warning('Error cargado el archivo')
    
    @Timer(logger)
    def load_csv(self):
        self.df_list = list()
        self.name_df = None
        try: 
            for i, file in enumerate(pd.read_csv(self.file_path, chunksize=1000)):
                self.df_list.append(file)
            self.logger.info('Archivo cargado exitosamente')
        except:
            self.logger.warning('Error cargado el archivo')
    
    def get_dataframes(self):
        return self.df_list

    def get_names(self):
        return self.name_df
    
    def get_columns(self, df):
        return df.columns.tolist()
    
    @Memory(logger)
    def nan_replace(self, df, columns):
        try:
            for col in columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                self.logger.debug('Columna %s procesada' % col)
        except:
            self.logger.warning('Se produjo un problema procesando la columna %s' %col)    
        return df
    
    @Memory(logger)
    def value_replace(self, df, columns, letter):
        try:
            for col in columns:
                df[col] = df[col].str.replace(letter, '', regex=False).fillna(df[col])
                self.logger.debug('Columna %s procesada' % col)
        except:
            self.logger.warning('Se produjo un problema procesando la columna %s' %col)    
        return df       

    @Timer(logger)
    def numeric_var_clean(self, df, columns, type='float'):
        try:
            for col in columns:
                if type == 'float':
                    df[col] = df[col].astype(str).str.replace(r'[^0-9, .]', '', regex=True)
                elif type == 'int':
                    df[col] = df[col].astype(str).str.replace(r'[^0-9]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                self.logger.debug("Se procesó la columna %s" % col)
        except: 
            self.logger.warning('Se produjo un problema haciendo la transformación')

        return df
    
    @Timer(logger)
    @Memory(logger)
    def merge_df(self, df1, df2, col_name, union='inner'):
        try:
            df_merge = pd.merge(df1, df2, on=col_name, how=union)
            self.logger.debug('Dataframes unidos')
        except:
            self.logger.warning('Se produjo un problema uniendo los dataframes')

        return df_merge

    @Timer(logger)
    def null_count(self, df, columns):
        null_list = list()
        try:
            for col in columns:
                null_index =  df[df[col].str.contains('NULL', na=False)].index.tolist()
                null_list.append(null_index)
            self.logger.debug('Valores NULL detectados')
        except:
            self.logger.warning('Las columnas deben ser únciamente con entradas str')
        return null_list
    
    @Timer(logger)
    def drop_column(self, df, column):
        try: 
            df = df.drop(columns=[column])
            self.logger.debug('Columna %s eliminada del dataframe' %column)
        except:
            self.logger.warning('La columna no pudo ser eliminada')
        
        return df
    
    @Timer(logger)
    def sum_index(self, dataframe, columna, indices):
        try:
            valores = dataframe.loc[indices, columna]
            sum = valores.sum()
            self.logger.debug('Cálculo realizado')
            return sum
        except:
            self.logger.info(f"Se presentó un error, verificar que los índices y la columna existan.")
            return None


if __name__ == "__main__":
    # Usage example
    etl = ETL('Films_2 (3).xlsx')
    etl.load_excel()  
    print('Las hojas cargadas', etl.get_names())