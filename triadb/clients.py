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


import time
from orator import DatabaseManager
from clickhouse_driver import Client
from .utils import ETL, sql_construct
from .exceptions import (InvalidCmdOperation, InvalidEngine, InvalidSourceType, PandasError)
from .exceptions import DBConnectionFailed

cmd_types = ['parts', 'mutations', 'optimize', 'query_log', 'tables', 'columns', 'create',
             'insert', 'select']

engine_types = ['MergeTree', 'ReplacingMergeTree']

source_types = ['file', 'TabSeparatedWithNames', 'CSVWithNames', 'MySQL',
                'ImportedDataResource', 'ImportedDataResourceWithRightJoin',
                'DataTypeDictionary', 'TableEngine']

# **********************************************************************
#   ******************** Classes Specifications *********************
# **********************************************************************


class MariaDB(object):
    """
    MariaDB is a facade pattern class based on Orator ORM
    """
    def __init__(self, host, port, user, password, database, trace=0):
        self._client = 'MariaDB'
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._api = None
        self._last_query = None

        # Debug messages
        self._trace = trace

        # This is to avoid problems that mysql has with localhost
        if host == 'localhost':
            self._host = '127.0.0.1'
        # Create connection with pony orm python API
        config = {
            'mysql': {
                'driver': 'mysql',
                'host': host,
                'database': database,
                'user': user,
                'password': password,
                'port': port,
                'prefix': ''
            }
        }

        self._api = DatabaseManager(config)
        result = self._api.select('SHOW TABLES')
        if not list(result):
            raise DBConnectionFailed(f'Connection to MariaDB failed. Check connection parameters')

    @property
    def last_query(self):
        return self._last_query

    @property
    def last_query_stats(self):
        return None

    def sql(self, q):
        return self._api.select(q)

    def cmd(self):
        pass

# ***************************************************************************************
# ************************** End of MariaDB Class **************************************
# ***************************************************************************************


class ClickHouse(object):
    """
    ClickHouse is a facade pattern class based on clickhouse-driver python API for ClickHouse DBMS
    It defines at a higher-level useful commands and adds to this API tracing/debug functionality and
    improved output format with Pandas dataframes.
    """
    def __init__(self, host, port, user, password, database, trace=0):
        self._client = 'ClickHouse'
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._api = None
        self._trace = trace
        self._last_query = None
        # clickhouser-driver last query execution statistics variables
        (self._lastquery_id, self._resultset_rows, self._elapsed, self._processed_rows,
         self._processed_bytes, self._total_rows) = [None, None, None, None, None, None]

        if host == 'localhost':
            self._host = '127.0.0.1'
        # Create connection with clickhouse driver API
        try:
            self._api = Client(host=host, database=database, port=port, user=user, password=password)
            self._api.execute('SHOW TABLES')
        except Exception:
            raise DBConnectionFailed(f'Connection to ClickHouse failed. Check connection parameters')

    @property
    def last_query(self):
        return self._last_query

    @property
    def last_query_stats(self):
        return [self._lastquery_id, self._resultset_rows, self._elapsed,
                self._processed_rows, self._processed_bytes, self._total_rows]

    def sql(self, sql='', cols=None, index=None, split=True, auto=False,
            params=None, columnar=False, qid=None, execute=True):
        """
        This method is calling clickhouse-driver execute() method to execute sql query
        Connection has already been established.
        :param sql: clickhouse SQL query string that will be send to server
        :param cols: pandas dataframe columns
        :param index: pandas dataframe columns
        :param split: either split the columns string argument or leave it
        :param auto: if True, it will try to extract the columns from SQL SELECT , , , FROM and pass them to `cols`
        :param params: clickhouse-client executeparameters
        :param columnar: if specified the result will be returned in column-oriented form. Defaults row-like form.
        :param qid: query identifier. If no query id specified ClickHouse server will generate it
        :param execute: execute SQL commands only if execute=True

        :return: pandas dataframe
        """
        # Initialization stage
        tuples = ()
        self._last_query = sql
        self._lastquery_id = qid
        (self._elapsed, self._resultset_rows, self._processed_rows,
         self._processed_bytes, self._total_rows) = [0, 0, 0, 0, 0]

        # clickhouse-driver execution of sql statement
        # ToDO: 1. choose the ouput format, i.e. display tuples, pandas dataframe, dictionary, etc...
        #
        # ToDO: 2. paging with a generator e.g. gen = (row for row in cql.execute('SELECT * from FloatOnSSD_SRC')
        # ToDo: 2. and clickhouse-driver streaming results, i.e. execute_iter command
        if execute:
            tuples = self._api.execute(query=sql, params=params, columnar=columnar, query_id=qid)
            self._elapsed = self._api.last_query.elapsed
            # Avoid AttributeError: 'NoneType' object has no attribute 'rows' in clickhouse-driver
            try:
                self._resultset_rows = self._api.last_query.profile_info.rows
                self._processed_rows = self._api.last_query.progress.rows
                self._processed_bytes = self._api.last_query.progress.bytes
                self._total_rows = self._api.last_query.progress.total_rows
            except AttributeError:
                pass

        # Transform tuples to pandas dataframe
        # Start measuring elapsed time for transforming python tuples to pandas dataframe
        t_start = time.perf_counter()
        if auto:
            pos1 = sql.find('SELECT') + 7
            pos2 = sql.find('FROM', pos1)
            cols = sql[pos1:pos2 - 1]

        if cols and split:
            cols = cols.split(', ')
        if index and split:
            index = index.split(', ')
        try:
            result = ETL.get_dataframe(tuples, cols, index)
        except Exception:
            print(sql)
            raise PandasError(f'Failed to construct Pandas dataframe, check query and parameters')
        # End measuring elapsed time for pandas dataframe transformation
        t_end = time.perf_counter()

        # Debug info
        if self._trace > 2:
            print(f'QueryID:{qid}\nLatency for pandas dataframe transformation : {round(t_end-t_start, 3)} sec.')
        if self._trace > 1:
            print(f'{self._last_query}\n╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌')
        if self._trace > 0:
            lqs = self.last_query_stats
            print(f'QueryID:{lqs[0]}\nElapsed: {round(lqs[2], 3)} sec',
                  f'{lqs[1]} rows in set.\nProcessed: {lqs[3]} rows, {round(lqs[4]/1048576, 3)} MB',
                  '\n___________________________________________________________________________')

        if result.empty:
            if self._trace > 0:
                print('Done.\n╚═══════════════════════════════════════════════════════════════════════╝')
            return None
        else:
            return result

    def cmd(self, cmd, dbhost=None, dbport=None, dbuser=None, dbpassword=None,
            db=None, table=None, engine=None, partkey=None, skey=None, settings=None,
            aggr=False, group_by=None, heading=None, fields=None, projection='*', where=None, hb2=None,
            source=None, ha2=None, fullpath=None, sql=None, active=True, limit=None, execute=True):
        """
        Basically this is a wrapper method that constructs sql statements,
        `sql` method executes these statements
        :param cmd: command controls the type of result to return. Default is to return the result set from query
        :param dbhost: mysql host
        :param dbport: mysql port
        :param dbuser: mysql database user
        :param dbpassword: mysql database user password
        :param db: database name
        :param table: table name
        :param engine: the type of clickhouse engine
        :param partkey: partition key
        :param skey: sorting key
        :param settings: clickhouse engine settings
        :param aggr: aggregate results
        :param group_by: SQL GROUP BY construct
        :param heading: list of field names paired with clickhouse data types
               ( 'fld1_name dtype1', 'fld2_name dtype2'... )
        :param fields: list of field names, used in pandas dataframe and in insert command
        :param projection: list of columns from a table, used in select command
        :param where: SQL WHERE construct in select command
        :param source: clickhouse file format, mysql, odbc, etc...
        :param fullpath: fullpath of the flat file used to read data from (relative to ClickHouse user_files_path)
        :param sql: SQL query
        :param active: select only active parts
        :param hb2: select parts with a specific hb2 dimension (hb2 is the dim2 of the Entity/ASET key)
               default hb2='%'
        :param ha2: attribute dimension that is used in insert command (ImportedDataResourceWithRightJoin)
        :param limit: SQL limit
        :param execute: Execute the command only if execute=True

        Each one of the following commands takes specific paramaters, `execute` is common for all of them:
        `tables`    : db, engine, table, group_by

        `columns`   : db, table, aggr

        `parts`     : db, table, hb2, active

        `optimize`  : table

        `mutations` : table, limit

        `query_log` :

        `create`    : table, heading, engine, partkey, skey, settings

        `select`    : dbhost, dbport, dbuser, dbpassword, db, table,
                      source, fullpath, heading, fields, where, projection, limit

        `insert`    : table, fields, source, ha2

        :return: query result set in a pandas dataframe
        """
        # Initialize local variables
        #

        # ------------------------------------------------------------------------------------
        # check cmd argument
        # ------------------------------------------------------------------------------------
        if cmd not in cmd_types:
            raise InvalidCmdOperation(f'Invalid command operation: failed with parameter cmd={cmd}')
        # ---------------------------------------------------------------------------------------
        # parsing cmd argument
        # ---------------------------------------------------------------------------------------
        if cmd == 'parts' and table is not None and db is not None:
            activepart = None
            # return information about parts of MergeTree tables
            if active is None:
                activepart = None
            elif active:
                activepart = 1
            elif not active:
                activepart = 0

            sel = f'\nSELECT table, database, partition_id, name, active, marks, rows,'
            sel += '\n        min_block_number as min_blk, max_block_number AS max_blk,'
            sel += '\n        level, toDecimal32(primary_key_bytes_in_memory/1024, 3) AS pk_mem'
            frm = 'FROM system.parts'
            if hb2 is None:
                wh = f'''WHERE database='{db}' AND table = '{table}' '''
            else:
                wh = f'''WHERE database='{db}' AND table = '{table}' AND partition_id LIKE '{hb2}-%' '''
            if active is not None:
                wh += f''' AND active = {activepart}'''
            ordname = f'ORDER BY name'
            # construct query
            query = sql_construct(select=sel, frm=frm, where=wh, order=ordname)
            # execute SQL query
            cols = 'table, db, PID, name, active, marks, rows, min_blk, max_blk, level, pk_mem (KB)'
            return self.sql(query, cols, index='PID', qid='Parts Command', execute=execute)
        elif cmd == 'mutations' and table is not None:
            # mutations allows changing or deleting lots of rows in a table
            # return information about mutations of MergeTree tables and their progress
            sel = f'\nSELECT table, command, create_time, block_numbers.number AS blk, parts_to_do AS parts, is_done,'
            sel += '\n        latest_failed_part AS failed_at, latest_fail_time AS failed_time'
            frm = f'FROM system.mutations'
            wh = f'WHERE table = \'{table}\''
            ordtime = f'ORDER BY create_time DESC'
            if limit:
                lim = f'LIMIT {limit}'
            else:
                lim = None
            if group_by:
                grp = f'GROUP BY {group_by}'
            else:
                grp = None
            # construct query
            query = sql_construct(select=sel, frm=frm, where=wh, group_by=grp, order=ordtime, limit=lim)
            # execute SQL query
            cols = 'table, command, created_at, blk, parts, is_done, failed_at, failed_time'
            self.sql('system flush logs', qid='flush logs', execute=execute)
            return self.sql(query, cols, qid='Mutation Information Command', execute=execute)
        elif cmd == 'optimize' and table is not None:
            # construct query
            query = f'OPTIMIZE TABLE {table} FINAL'
            # execute SQL query
            return self.sql(query, qid='Optimize Engine Command', execute=execute)
        elif cmd == 'query_log':
            # metadata for queries logged in the ClickHouse table with log_queries=1 setting
            sel = f'SELECT query_id AS id, user, client_hostname as host, client_name as client,'
            sel += '\n        result_rows AS in_set, toDecimal32(query_duration_ms / 1000, 3) AS sec,'
            sel += '\n        toDecimal32(memory_usage/1048576, 3) AS MEM_MB,'
            sel += '\n        read_rows as R_Rows, toDecimal32(read_bytes / 1048576, 3) AS R_MB,'
            sel += '\n        written_rows AS W_Rows, toDecimal32(written_bytes/1048576, 3) AS W_MB, query'
            frm = f'FROM system.query_log'
            wh = f'WHERE (type = 2) AND (query NOT LIKE \'%query_duration_ms%\')'
            ordtime = f'ORDER BY event_time DESC'
            # construct query
            query = sql_construct(select=sel, frm=frm, where=wh, order=ordtime)
            # execute SQL query
            cols = 'id, user, host, client, in_set, sec, MEM_MB, R_Rows, R_MB, W_Rows, W_MB, query'
            self.sql('system flush logs', qid='flush logs command', execute=execute)
            return self.sql(query, cols, qid='Query Log Command', index='id', execute=execute)
        elif cmd == 'tables':
            # Contains metadata of each table that the server knows about. Detached tables are not shown
            sel = f'SELECT database as db, engine, name as table, '
            sel += '\n        partition_key as partkey, sorting_key as skey, primary_key as pkey'
            frm = f'FROM system.tables'
            wh = f'WHERE db=\'{db}\' '
            if table:
                wh += f'AND table like \'%{table}%\''
            if engine:
                wh += f'AND engine=\'{engine}\''
            ordengine = f'ORDER BY db, engine'
            # construct query
            query = sql_construct(select=sel, frm=frm, where=wh, order=ordengine)
            if group_by:
                sel = f'SELECT {group_by}, groupArray(table) AS tables'
                frm = f'FROM ( \n{query} )'
                grp = f'GROUP BY {group_by}'
                query = sql_construct(select=sel, frm=frm, group_by=grp)
                cols = 'engine, tables'
            else:
                cols = 'db, engine, table, partkey, skey, pkey'
            # execute SQL query
            return self.sql(query, cols, qid='Table Engines Metadata Command', execute=execute)
        elif cmd == 'columns':
            # information about the columns in a table.
            sel = f'SELECT name, comment, type,'
            sel += '\n       toDecimal32(data_compressed_bytes/1048576, 3) as Compressed_MB,'
            sel += '\n       toDecimal32(data_uncompressed_bytes/1048576, 3) as Uncompressed_MB,'
            sel += '\n       toDecimal32(marks_bytes/1024, 3) as marks_KB'
            frm = 'FROM system.columns'
            wh = f'WHERE database=\'{db}\' AND table=\'{table}\''
            ordtype = f'ORDER BY type DESC, Compressed_MB DESC'
            # construct query
            query = sql_construct(select=sel, frm=frm, where=wh, order=ordtype)
            if aggr:
                sel = f'SELECT any(\'{db}\') as db, any(\'{table}\') as table,'
                sel += f'\n      sum(Compressed_MB) as total_compressed_MB,'
                sel += f'\n      sum(Uncompressed_MB) as total_uncompressed_MB, sum(marks_KB) as total_marks_KB,'
                sel += f'\n      argMin(name, Compressed_MB) as min_column, min(Compressed_MB) as min_MB,'
                sel += f'\n      argMax(name, Compressed_MB) as max_column, max(Compressed_MB) as max_MB,'
                sel += f'\n     avg(Compressed_MB) as avg_MB'
                frm = f'FROM ( \n{query} )'
                query = sql_construct(select=sel, frm=frm)
                cols = 'db, table, total_compressed_MB, total_uncompressed_MB, total_marks_KB, '
                cols += 'min_column, min_MB, max_column, max_MB, avg_MB'
            else:
                cols = 'name, comment, type, Compressed_MB, Uncomressed_MB, marks_KB'
            # execute SQL query
            return self.sql(query, cols, qid='Table Columns Metadata Command', execute=execute)
        elif cmd == 'create':
            # Check engine passed
            if engine not in engine_types:
                raise InvalidEngine(f'Invalid TRIADB engine: failed with parameter engine={engine}')
            # join list of strings to ', \n' separated string
            structure = ', \n'.join(heading)
            query = f'CREATE TABLE {table}'
            query += f'({structure})'
            query += f'\nENGINE = {engine}()'
            query += f'\nPARTITION BY {partkey}'
            query += f'\nORDER BY {skey}'
            query += f'\nSETTINGS {settings}'

            # execute SQL query
            self.sql(f'DROP TABLE IF EXISTS {table}', qid=f'Drop Table {table}', execute=execute)
            return self.sql(query, qid=f'Create Engine {engine} Command', execute=execute)
        elif cmd == 'select':
            if source not in source_types:
                raise InvalidSourceType(f'Invalid TRIADB source type: failed with parameter source={source}')
            query = None
            result = None

            # join list of strings to ', \n' separated string
            if source in ['CSVWithNames', 'TabSeparatedWithNames', 'MySQL']:
                sel = f'SELECT {projection}'
                frm = ''
                if source in ['CSVWithNames', 'TabSeparatedWithNames']:
                    structure = ', '.join(heading)
                    frm = f'FROM file('
                    frm += f"\n   '{fullpath}',"
                    frm += f"\n   '{source}',"
                    frm += f"\n   '{structure}'"
                    frm += f'\n)'
                elif source == 'MySQL':
                    frm = f"FROM mysql('{dbhost}:{dbport}', '{db}', '{table}', '{dbuser}', '{dbpassword}')"

                if where:
                    wh = f'WHERE {where}'
                else:
                    wh = None

                if limit:
                    lim = f'LIMIT {limit}'
                else:
                    lim = None
                # construct query
                query = sql_construct(select=sel, frm=frm, where=wh, limit=lim)
                # execute SQL query
                result = self.sql(query, qid='Select rows from flat file Command',
                                  cols=fields, split=False, execute=execute)
            elif source == 'ImportedDataResource':
                structure = ', '.join(heading)
                sel = f'SELECT {structure}'
                frm = f'FROM {table} '
                grp = 'GROUP BY val'
                hav = 'HAVING isNotNull(val)'
                ordval = 'ORDER BY val'
                # construct query
                query = sql_construct(select=sel, frm=frm, group_by=grp, having=hav, order=ordval)
                # execute SQL query
                result = self.sql(query, qid='Select HyperAtom AdjacencyLists Command',
                                  cols=fields, split=False, execute=execute)
            # if query is used in other commands use execute=false to return it
            if execute:
                return result
            else:
                return query
        elif cmd == 'insert':
            if source not in source_types:
                raise InvalidSourceType(f'Invalid TRIADB source type: failed with parameter source={source}')
            sqlid = None
            frm = None
            # Inserting the results of SELECT
            ins = f'INSERT INTO {table}'
            sel = f'SELECT ' + ', '.join(fields)
            # execute SQL query
            if source == 'file':
                # from part
                frm = f'FROM (\n{sql} \n)'
                # query id : insert from file to import data resource
                sqlid = 'InsertFromFile'
            elif source == 'ImportedDataResourceWithRightJoin':
                # from part
                frm = f'FROM {table} \nRIGHT JOIN(\n{sql} \n) AS A USING val\n WHERE ha2={ha2}'
                # query id : insert from imported data resource to load data on data type dictionary engines
                sqlid = 'InsertFromImportedDataResourceWithRightJoin'
            elif source == 'ImportedDataResource':
                # from part
                frm = f'FROM (\n{sql} \n)'
                # query id : insert from imported data resource to load data on data type dictionary engines
                sqlid = 'InsertFromImportedDataResource'
            elif source == 'TableEngine':
                # in that case sql parameter takes the name of the table engine
                # from part
                frm = f'FROM {sql}'
                # query id : insert from imported data resource to load data on data type dictionary engines
                sqlid = 'InsertFromTableEngine'
            elif source == 'DataTypeDictionary':
                # from part (sql variable here is the name of the DataTypeDictionary table)
                frm = f'FROM {sql}'
                # insert from DataTypeDictionary to load data onto HAtom and HLink
                sqlid = 'InsertFromDataTypeDictionary'
            # construct query
            query = ins + f'\n{sel}\n{frm}'
            return self.sql(query, qid=f'{sqlid} Command', execute=execute)
        else:
            raise InvalidCmdOperation(f'Invalid command operation')

    def disconnect(self):
        self._api.disconnect()


# ***************************************************************************************
# ************************** End of ClickHouse Class ************************************
# ***************************************************************************************
class ConnectionPool(object):
    """
    ConnectionPool manages connections to DBMS and provides common functionality
    e.g. `sql`, `cmd`, `qstats`.....
    """
    mysql_connections = 0
    clickhouse_connections = 0

    def __init__(self, dbms, host, port, user, password, database, trace=0):
        # Get connector, either ClickHouse or MariaDB
        self._connector = self._get_connector(dbms)
        # Create a new connection
        self._connection = self._connector(host, port, user, password, database, trace)

        self._host = host       # host connection parameter, name or IP address
        self._port = port       # the port number used by the database server
        self._user = user
        self._password = password
        self._client = self._connection._client   # the name of the DBMS, i.e. ClickHouse or MariaDB
        self._trace = trace                       # flag to display more information during execution of query

        # Methods composition
        self.sql = self._connection.sql
        self.cmd = self._connection.cmd

        if self._trace > 3:
            print(f'\nConnected to {self.__repr__()}')

    @staticmethod
    def _get_connector(dbms):
        if dbms == 'clickhouse':
            ConnectionPool.clickhouse_connections += 1
            return ClickHouse
        elif dbms == 'mariadb':
            ConnectionPool.mysql_connections += 1
            return MariaDB
        else:
            raise DBConnectionFailed(f'Connection to DBMS failed <{dbms} is not supported>')

    @property
    def api(self):
        """
        :return: the API client, orator or clickhouse-driver that is connected to DBMS
        """
        return self._connection._api

    @property
    def database(self):
        """
        :return: the name of the database in the DBMS
        """
        return self._connection._database

    @property
    def last_query(self):
        """
        :return: last sql query executed
        """
        return self._connection.last_query

    @property
    def qstats(self):
        """
        :return: statistics for the execution of last sql query
        """
        return self._connection.last_query_stats

    def __repr__(self):
        return f'{self._client}(host = {self._host}, port = {self._port}, database = {self.database})'

# ***************************************************************************************
# ************************** End of ConnectionPool Class ***********************
# ***************************************************************************************


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                   =======   End of triadb_clients Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
