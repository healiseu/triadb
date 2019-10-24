"""
This file is part of TRIADB Self-Service Data Management and Analytics Framework
(C) 2015-2019 Athanassios I. Hatzis

TRIADB is free software: you can redistribute it and/or modify it under the terms of
the GNU Affero General Public License v.3.0 as published by the Free Software Foundation.

TRIADB is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with TRIADB.
If not, see <https://www.gnu.org/licenses/>.
"""

# ===================================================================
# Package-Modules Dependencies
# ===================================================================
from time import gmtime, strftime
from IPython.display import display_html
import pandas as pd
import os.path
import tkinter as tk
import json
import petl
import psutil

from operator import itemgetter
from tkinter import filedialog

# Global variables and settings

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 300)
petl.config.look_style = 'simple'

# If sort_buffersize is set to None, this forces all sorting to be done entirely in memory.
petl.config.sort_buffersize = None


def display_dataframes(*df_stylers):
    html_repr = ''
    for styler in df_stylers:
        html_repr += styler.render() + "\xa0\xa0\xa0"

    return display_html(html_repr, raw=True)


def highlight_states(s):
    if s.P == 0:
        return ['background-color: lightgrey'] * len(s)
    elif s.S == 1:
        return ['background-color: lime'] * len(s)
    else:
        return ['background-color: white'] * len(s)


def bytes2mb(b):
    return round(b/1024**2, 1)


def sql_construct(select, frm, where=None, group_by=None, having=None, order=None, limit=None):
    sql = f'{select}\n{frm}'
    if where:
        sql += f'\n{where}'
    if group_by:
        sql += f'\n{group_by}'
    if having:
        sql += f'\n{having}'
    if order:
        sql += f'\n{order}'
    if limit:
        sql += f'\n{limit}'
    return sql


class MemStats(object):

    def __init__(self):
        self._mem = psutil.virtual_memory()
        self._cpu = psutil.cpu_percent()

    @property
    def total(self):
        return bytes2mb(self._mem.total)

    @property
    def used(self):
        return bytes2mb(self._mem.used)

    @property
    def available(self):
        return bytes2mb(self._mem.available)

    @property
    def buffers(self):
        return bytes2mb(self._mem.buffers)

    @property
    def cached(self):
        return bytes2mb(self._mem.cached)

    @property
    def free(self):
        return bytes2mb(self._mem.free)

    def __repr__(self):
        avpercent = self.available * 100 / self.total
        # usedpercent = self.used * 100 / self.total
        available_str = f'Memory(available:{self.available} MB ({round(avpercent,1)}%),'
        total_used_str = f'total used:{round(self.total-self.available, 2)} MB ({self._mem.percent}%))'
        return available_str+total_used_str

    def print_stats(self):
        print(f'System Memory ({self._mem.percent} %), CPU ({self._cpu}%))')
        print('====================================')
        print(f'Total      = {self.total} MB')
        print(f'Used       = {self.used} MB')
        print(f'Available  = {self.available} MB')
        print(f'Buffers    = {self.buffers} MB')
        print(f'Cached     = {self.cached} MB')
        print(f'Free       = {self.free} MB')


class ETL(object):

    # Left column is the clickhouse data type and right column is the name of the field in HAtom table
    # that is created when we load the data to clickhouse associative semiotic hypergraph engine

    dtype2column = {
        'String':   'str',
        'Date':     'date',
        'DateTime': 'datetime',
        'Float32':  'float32',

        'UInt8':    'uint8',
        'UInt16':   'uint16',
        'UInt32':   'uint32',
        'UInt64':   'uint64'
    }

    def __init__(self):
        pass

    @staticmethod
    def session_time():
        return strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())

    @staticmethod
    def write_json(data, fname):
        with open(fname, 'w') as outfile:
            json.dump(data, outfile, indent=3)
        return True

    @staticmethod
    def load_json(fname):
        with open(fname) as f:
            return json.load(f)

    @staticmethod
    def change_cwd(fpath):
        return os.chdir(fpath)

    @staticmethod
    def get_cwd():
        return os.getcwd()

    @staticmethod
    def drop_extention(fname):
        return os.path.splitext(fname)[0]

    @staticmethod
    def get_full_path_filename(p, f):
        return os.path.join(ETL.get_full_path(p), f)

    @staticmethod
    def get_full_path(path):
        return os.path.join(ETL.get_cwd(), path)

    @staticmethod
    def get_filenames(path, extension='json', window_title='Choose files', gui=False, select=None):
        if gui:
            root = tk.Tk()
            root.withdraw()
            root.call('wm', 'attributes', '.', '-topmost', True)

            # Get filenames with extension .ext inside a folder located at relative _path
            fullpath = ETL.get_full_path(path)
            dialogdic = {'initialdir': fullpath, 'title': window_title}

            if extension in ['json', 'JSON']:
                dialogdic['filetypes'] = (
                    ("json files", "*.json"), ("csv files", "*.csv"), ("tsv files", "*.tsv"), ("all files", "*.*"))
            elif extension in ['csv', 'CSV']:
                dialogdic['filetypes'] = (
                    ("csv files", "*.csv"), ("tsv files", "*.tsv"), ("json files", "*.json"), ("all files", "*.*"))
            elif extension in ['tsv', 'TSV']:
                dialogdic['filetypes'] = (
                    ("tsv files", "*.tsv"), ("csv files", "*.csv"), ("json files", "*.json"), ("all files", "*.*"))
            elif extension == 'all':
                dialogdic['filetypes'] = (
                    ("all files", "*.*"), ("tsv files", "*.tsv"), ("csv files", "*.csv"), ("json files", "*.json"))

            # these are full path filenames
            fullpath_filenames = filedialog.askopenfilenames(**dialogdic)
            # return only filenames
            filenames = [os.path.basename(fnam) for fnam in fullpath_filenames]
        else:
            # Get filenames with extension .ext inside a folder located at relative _path
            full_path = ETL.get_full_path(path)
            ext = '.' + extension.lower()
            filenames = [file for file in os.listdir(full_path) if file.endswith(ext)]
            if select:
                filenames = itemgetter(*select)(filenames)

        return filenames

    # ===============================================================================
    # petl table methods
    # ===============================================================================
    @staticmethod
    def get_file_header(ftype, fname):
        ext = os.path.splitext(fname)[1][1:]
        if not ftype.lower() == ext:
            raise Exception(f'Failed: Filename extension does not match < ftype={ftype} >')

        if ftype == 'CSV':
            return petl.fromcsv(fname).head(0).tol()[0]
        elif ftype == 'TSV':
            return petl.fromtsv(fname).head(0).tol()[0]

    @staticmethod
    def get_table(source=None, nrows=None, skip=None, fields=None, exclude=None, rownumbers=True, **petlargs):
        """
        :param source: full path filename of the delimited file
        :param nrows: number of rows to include in the table
        :param skip: number of rows to skip from the file
        :param fields: selected fields to extract from the file
        :param exclude: selected fields to be excluded from the file
        :param rownumbers: Add a rowID column. This is True by default
        This is similar to pandas.RangeIndex see petl.transform.basics.addrownumbers()
        Notice: skip and nrows parameters require that addrownumbers() is applied to petl table
        If `fields` is specified and `rowID` is not included in the list the column will not be included in petl table
        :param petlargs: see petl.io.csv.fromcsv and petl.io.csv.fromtsv
        :return: petl table container
        Notice: petl makes extensive use of lazy evaluation and iterators
        the file is not loaded in memory instead a container/iterator is returned

        Examples:
        etl.get_table('movies.csv', 20, 100, ['rowID', 'movie_title', 'title_year']).lookall()
        etl.get_table('movies.csv', 20, 100, exclude=['language', 'actor_1_name']).lookall()
        """

        # Get the extension of the filename
        table = None
        if source:
            ext = os.path.splitext(source)[1]

            # Read all rows from the file and create a pandas dataframe in memory
            if ext == '.csv':
                table = petl.fromcsv(source, **petlargs)
            elif ext == '.tsv':
                table = petl.fromtsv(source, **petlargs)

            if rownumbers:
                table = table.addrownumbers(start=1, step=1, field='rowID')

            if skip and rownumbers:
                if nrows:
                    table = table.select(lambda num: num.rowID > skip and num.rowID <= nrows+skip)
                else:
                    table = table.select(lambda num: num.rowID > skip)

            if not skip and nrows and rownumbers:
                table = table.select(lambda num: num.rowID <= nrows)

            if fields:
                table = table.cut(*fields)

            if exclude:
                table = table.cutout(*exclude)

        return table

    # ===============================================================================
    # pandas dataframe methods
    # ===============================================================================
    @staticmethod
    def dataframe_conversion(pdf, mapping):
        # This is a left over from TRIADB on REDIS version
        # Convert object column with a string date/time format to pandas (numpy) datetime format
        # use pd.to_datetime()
        #
        # From the mapping find attributes that have 'date', 'time', 'dt' value types
        # Do the conversion
        for col in pdf.columns.tolist():
            if mapping[col].vtype in ['date', 'time', 'dt']:
                pdf[col] = pd.to_datetime(pdf[col], infer_datetime_format=True)

    @staticmethod
    def dataframe_cardinality(df):
        # cardinality refers to the uniqueness of data values
        # contained in a particular column of a dataframe
        return dict(zip(df.nunique().keys(), df.nunique().values))

    @staticmethod
    def dataframe_selectivity(df):
        # from decimal import Decimal
        # round(Decimal(1091544/2788597), 6)
        # selectivity is a measure of how much variety there is in the values of a given dataframe column
        # in relation to the total number of rows in a given dataframe
        keys = df.nunique().keys()
        vals = [int(round(val / df.shape[0] * 100)) for val in df.nunique().values]
        return dict(zip(keys, vals))

    @staticmethod
    def load_dataframe(source, **pandasargs):
        """
        NOTICE: Currently only CSV/TSV files are supported

        :param source: full-path filename of the CSV/TSV delimited file
        :param pandasargs: Default pandas options for reading delimited text files
        :return: pandas dataframe

        Examples:
            load_dataframe(source, sep='|', index_col=False, nrows=10, skiprows=3,
                          usecols=['catsid', 'catpid', 'catcost', 'catfoo', 'catchk'],
                          dtype={'catsid':int, 'catpid':int, 'catcost':float, 'catfoo':float, 'catchk':bool},
                          parse_dates=['catdate'])
        """

        # map the file object directly onto memory and access the data directly from there.
        # Using this option can improve performance because there is no longer any I/O overhead
        pandasargs['memory_map'] = True

        # Alternatively for very large files iterate lazily
        # rather than reading the entire file into memory specify a chunksize to read_csv or read_table,
        # the return value will be an iterable object of type TextFileReader:
        #
        # chunk_reader = triadb.get_dataframe(source, chunksize=2)
        #
        # chunk_reader.get_chunk()
        # chunk_reader.get_chunk()
        # chunk_reader.get_chunk(4)
        #
        # chunk_list = []
        # for chunk in chunk_reader:
        #    chunk_list.append(chunk.get_values())

        records_df = None
        header_columns = None

        # Get the extension of the filename
        ext = os.path.splitext(source)[1]

        # Read all rows from the file and create a pandas dataframe in memory
        if ext == '.csv':
            header_columns = pd.read_csv(source, nrows=0).columns.tolist()
            records_df = pd.read_csv(source, **pandasargs)
            # if the file is too big to fit in memory we could also use:
            # self.pd.read_csv(os.path.join(path, resource_name), usecols=columns)
            # where columns are user selected columns to read from CSV flat file
            #
        elif ext == '.tsv':
            header_columns = pd.read_csv(source, nrows=0, sep='\t').columns.tolist()
            records_df = pd.read_csv(source, sep='\t', **pandasargs)

        # When Pands is skipping rows in a CSV/TSV it looses the header and replaces
        # the column names with the values of the last row of those skipped
        # in that case save the header_columns and reconstruct the dataframe
        if 'skiprows' in pandasargs:
            records_df = pd.DataFrame(data=records_df.values, columns=header_columns)

        return records_df

    @staticmethod
    def get_dataframe(iterable, columns, ndx=None):
        """
        :param iterable: e.g. list like objects
        :param columns: labels to use for the columns of the resulting frame
        :param ndx: index to use for resulting frame
        :return: pandas dataframe
        """
        df = pd.DataFrame(columns=columns, data=iterable)
        if ndx:
            df.set_index(ndx, inplace=True)
        return df

    @staticmethod
    def get_empty_dataframe():
        return pd.DataFrame()

    @staticmethod
    def concat_dataframe_columns(df1, df2):
        return pd.concat([df1, df2], axis=1)

    @staticmethod
    def dict_to_dataframe(d, labels):
        return pd.DataFrame.from_dict(d, orient='index', columns=labels)
