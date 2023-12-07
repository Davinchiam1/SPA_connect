import datetime
import sys
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, insert, select, exists, inspect, delete
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, DATETIME, Date, MetaData, Table, text, DDL, BigInteger
import pandas as pd
import sqlalchemy
import numpy as np
from report_loader import Report_loader

# creating a database connection from a file
with open('database.txt', 'r') as file:
    conn_string = file.readline().strip()

engine = create_engine(conn_string)
Session = sessionmaker(bind=engine)
metadata = MetaData()


# session = Session()


def create_table(table_name, frame):
    """
    Function for creating a Table  from dataframe or establishment of dependency with existing table from db.

    Args:
        table_name (string): name of table.
        frame (pd.Dataframe): dataframe sample for table.

    Returns:
        Table: Table object with name table_name.
    """
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        table = Table(table_name, metadata, autoload_with=engine)
    else:
        table = Table(table_name, metadata)
        columns = create_colums(frame)
        for name, data_type in columns.items():
            if name == 'id':
                table.append_column(Column(name, data_type, primary_key=True, autoincrement=True))
            else:
                table.append_column(Column(name, data_type))
        metadata.create_all(engine)
    return table


def create_colums(frame=pd.DataFrame()):
    """
    Function for transformations dataframe columns into dict with column_names: data types structure.

    Args:
        frame (pd.Dataframe): dataframe sample for table.

    Returns:
        columns_dict (dict): dict of column_names and data types.
    """
    columns_dict = {'id': BigInteger}
    for col, dtype in frame.dtypes.items():
        if dtype == 'int64':
            columns_dict[col] = Integer
        elif dtype == 'float64':
            columns_dict[col] = Float
        elif str(dtype).startswith('object'):
            columns_dict[col] = String
        else:
            columns_dict[col] = String
    return columns_dict


def update_report_table(reports_names, df_list):
    """
    Function for updating a table of the "orders" type with data from the api service from start to end page.

    Args:
        reports_names (list): names of reports,equal for table names in database.
        df_list(list): link list of dataframes with data to load to db.

    Returns:
        None

    """

    for table_name, frame in zip(reports_names, df_list):
        table = create_table(table_name=table_name, frame=frame)
        Base = sqlalchemy.orm.declarative_base()

        class Temp_table(Base):
            # base class for sqlalchemy to secure transactions
            __tablename__ = table_name
            __table__ = table

        errors_list = []
        session = Session()

        with engine.connect() as connection:
            # prepare data for uploading

            # data=data.drop('items.serial_num',axis=1)
            data1 = frame.to_dict('records')

            # update or upload data in table
            for row in data1:
                if 'sku' in row.keys():
                    existing_row = session.query(Temp_table).filter_by(date=row['date'],sku=row['sku']).first()
                else:
                    existing_row = session.query(Temp_table).filter_by(date=row['date']).first()
                if existing_row:
                    session.delete(existing_row)
                    session.commit()
                try:
                    table = Temp_table(**row)
                    session.merge(table)
                except Exception as e:
                    errors_list.append(row['id'])
                    print(e)
                    continue

        session.commit()
        session.close()

    # with engine.connect() as connection:
    #     result = connection.execute(text('SELECT * FROM ' + table_name))
    #     df_result = pd.DataFrame(result.fetchall(), columns=result.keys())
    #     df_result.to_excel('1234.xlsx')
    #     print(df_result.tail())
    #
    # # закрытие соединения
    # engine.dispose()


# update_report_table(reports_names=['test1', 'test2'], df_list=Report_loader().transform_report_document())
