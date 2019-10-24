"""
This file is part of TriaClick Associative Semiotic Hypergraph Engine
(C) 2018-2019 Athanassios I. Hatzis
Licensed under the TriaClick Open Source License Agreement (TOSLA)

You may not use this file except in compliance with TOSLA.
The files subject to TOSLA are grouped in this directory to clearly separate them from files
in the parent directory that are licensed under GNU Affero General Public License v.3.0.

You should retain this header in the file and a copy of the LICENSE_TOSLA file in the current directory
"""
import time
from collections import namedtuple
from triadb.utils import ETL
from triadb.exceptions import ASetError, OperationError
from .generative import GenerativeBase, _generative
from .hacol import HACQL, HACOL

oplist = ['Counting', 'Sum', 'Average', 'Projection', 'Filtering']
out_types = ['single', 'list', 'keys', 'tuple', 'dict', 'ids', 'set of items', 'tuple of items']
class ASET(object):
    def __init__(self, engine, dms_entity):
        if dms_entity.type != 'ENT':
            raise ASetError(f'ASET object can be created only from DMS:ENT objects')
        self._ent = dms_entity
        self._engine = engine
        self.chsql = self._engine.chsql
        self._type = 'ASET'  # type of hyper-structure
        self._last_query = None  # last query executed
        self._columns = None  # is used for pandas dataframe columns
        self._index = None  # is used for pandas dataframe index
        self._qid = None  # SQL query id
        self._alias = self._ent.alias  # Entity alias name
        self._flt_prefix = f'FLT_{self._ent.dim3}_{self._ent.dim2}'  # prefix for SQL tables in filtered state
        self._hatom_states = f'HAtom_{self._ent.dim3}States'  # HAtom table name in filtered state
        self._attributes = self._ent.get_attributes(out='objects')
        self._attributes_keyname = {(attr.dim4, attr.dim3, attr.dim2): attr.alias for attr in self._attributes}
        self._selectops = []  # List of cql.Select() operations
        self._filtered = self._is_filtered()
        self._hbonds = self.count()  # is used to store the result from count()

        class Association(self.createAssociationConstruct()):
            def __str__(self):
                return f'{self._alias}{self.key}'
            @property
            def fields(self):
                return self._fields
        self._association = Association
        self._heading = self._association.fields
    def createAssociationConstruct(self):
        assoc_name = self._alias
        assoc_fields = ['key'] + [attr.alias for attr in self._attributes]
        assoc_defaults = (None, )*len(self._attributes)
        assoc = namedtuple(assoc_name, assoc_fields, defaults=assoc_defaults)
        return assoc
    def __repr__(self):
        filtered = ''
        if self._filtered:
            filtered = ' filtered'
        return f'{self._type}{self.key}[{self._alias}] = {self._hbonds} hbonds{filtered}'
    def reset(self):
        self.chsql(f'DROP TABLE IF EXISTS {self._flt_prefix}_VW_pos', qid='Drop VW_pos')
        self.chsql(f'DROP TABLE IF EXISTS {self._flt_prefix}_VW_sel', qid='Drop VW_sel')
        self.chsql(f'DROP TABLE IF EXISTS {self._flt_prefix}_MEM_X', qid='Drop MEM_X')
        self.chsql(f'DROP TABLE IF EXISTS {self._flt_prefix}_MEM_Z', qid='Drop MEM_Z')
        self.chsql(f'DROP TABLE IF EXISTS {self._flt_prefix}_MEM_ha1', qid='Drop MEM_ha1')
        self._ent.old_set = f'{self._flt_prefix}_MEM_Z'
        self._ent.new_set = f'{self._flt_prefix}_MEM_X'
        self._selectops = []
        self._filtered = False
        self._hbonds = self.count()
        return self._filtered
    @property
    def selectops(self):
        return self._selectops
    @property
    def flt_prefix(self):
        return self._flt_prefix
    @property
    def hatom_states(self):
        return self._hatom_states
    @property
    def columns(self):
        return self._columns
    @columns.setter
    def columns(self, val):
        self._columns = val
    @property
    def qid(self):
        return self._qid
    @qid.setter
    def qid(self, val):
        self._qid = val
    @property
    def index(self):
        return self._index
    @index.setter
    def index(self, val):
        self._index = val
    @property
    def ent(self):
        return self._ent
    @property
    def alias(self):
        return self._alias
    @property
    def dbg(self):
        return self._ent.dbg
    @property
    def datadb(self):
        return self._ent.dmc.datadb
    @property
    def key(self):
        return self._ent.key[0], self._ent.key[1]
    @property
    def filtered(self):
        return self._filtered
    @filtered.setter
    def filtered(self, status):
        self._filtered = status
    @property
    def hbonds(self):
        return self._hbonds
    @hbonds.setter
    def hbonds(self, cnt):
        self._hbonds = cnt
    @property
    def last_query(self):
        if self._qid == 'Filtering':
            for update_of_state in self._last_query:
                for sql_statement in update_of_state:
                    print(sql_statement)
        else:
            print(self._last_query)
        return self._last_query
    @last_query.setter
    def last_query(self, q):
        self._last_query = q
    @property
    def last_query_info(self):
        return self._engine.get_last_query_info()
    @property
    def cql(self):
        return ASETCQL(self)
    def _is_filtered(self):
        filtered = False
        sql_views_info = self._engine.get_table_engines(engine='View', table=f'{self._flt_prefix}_VW')
        if sql_views_info is not None:
            filtered = True
        return filtered
    def clear_states_engine_columns(self, columns, exe=True):
        part_ids = self._engine.get_parts(alias='hatomStates', hb2=f'{self._ent.dim2}').index.values
        for col in columns:
            for prtid in part_ids:
                self.chsql(f'''ALTER TABLE {self._hatom_states} CLEAR COLUMN {col} in PARTITION ID '{prtid}' ''',
                           qid='Clear Column in Partition ID', execute=exe)
        return part_ids
    def count(self, coltype='val', projection=None, missing=False, order='cnt', estimate=True):
        if coltype == 'val':
            result = self.cql.Count(coltype=coltype, projection=projection,
                                    missing=missing, order=order, estimate=estimate).Exe().Value().Res
        else:
            result = self.cql.Count(coltype=coltype, projection=projection,
                                    missing=missing, order=order, estimate=estimate).Exe().Res
        return result

    def repeat_execution(self, exe=False):
        return self.chsql(self._last_query, cols=self._columns, index=self._index, qid=self._qid, execute=exe)

    def clear_selections(self):
        self._selectops = []
        return self._selectops

    def add_selections(self, selectops):
        if not selectops:
            raise ASetError(f'`selectops` must be a list of CQL Select() objects or a single CQL Select() object')
        if isinstance(selectops, HACQL):
            selectops = [selectops]
        elif isinstance(selectops, list):
            for elem in selectops:
                if not isinstance(elem, HACQL):
                    raise ASetError(f'Elements of `selectops` list must be of type HACQL')
        else:
            raise ASetError(f'`selectops` must be a list of CQL Select() objects or a single CQL Select() object')
        self._selectops.extend(selectops)

        return [elem for elem in self._selectops]

    def save_selections(self, fname):
        selection_data = [selop.seldict for selop in self._selectops]
        return ETL.write_json(selection_data, fname)

    def load_selections(self, fname):
        selection_data = ETL.load_json(fname)
        self._selectops = [self.reconstuct_selection(**seldat) for seldat in selection_data]
        return self._selectops

    def del_selections(self, *indexes):
        for ndx in indexes:
            self._selectops.remove(self._selectops[ndx])
        return self._selectops

    def get_selections(self):
        return self._selectops

    def reconstuct_selection(self, **kwargs):
        if 'where' in kwargs:
            where_cond = kwargs.pop('where')
        else:
            return 'Error'
        if 'dim3' in kwargs and 'dim2' in kwargs:
            dim3, dim2 = [kwargs.pop(d) for d in ('dim3', 'dim2')]
            select_where = getattr(HACOL(self._engine, self._ent.switch(dim3, dim2)).cql.Select(), 'Where')(where_cond)
        else:
            return 'Error'
        if 'operator' in kwargs:
            operator = kwargs.pop('operator')
            result = getattr(select_where, operator)(**kwargs)
        else:
            result = select_where
        return result

class AVIEW(object):
    def __init__(self):
        self._hstruct = 'ASET'  # type of hyper-structure

class ASETCQL(GenerativeBase):
    def __init__(self, aset, result=None):
        self.Res = result
        self._aset = aset
        self._sql = aset.chsql
        self._dbg = aset.dbg
        self._key = aset.key
        self._dim3 = aset.key[0]
        self._dim2 = aset.key[1]
        self._alias = aset.alias
        self._flt_prefix = aset.flt_prefix
        self._hatom_states = aset.hatom_states
        self._selected = None
        self._operation = 'UNDEFINED'
        self._dfcolumns = None
    def __repr__(self):
        return f'CQL{self._key}[{self._alias}]'
    @_generative
    def Count(self, coltype='val', projection=None, missing=False, order='cnt', estimate=False):
        (sel, frm, whe, grp, new_dfcolumns, count_label) = ('', '', '', '', '', '')
        if coltype == 'set' and not missing:
            new_dfcolumns = ['Attribute Collection', 'Distinct Items (domain values)']
            count_label = 'Distinct Items (domain values)'
            if self._aset.filtered:
                sel = f'SELECT ha2, count(ha2) AS cnt'
                frm = f'\nFROM {self._hatom_states}'
                whe = f'\nWHERE hb2={self._dim2} AND pos=1'
                grp = '\nGROUP BY ha2'
            else:
                sel = f'SELECT ha2, count(ha2) as cnt'
                frm = f'\nFROM HAtom_{self._dim3}'
                whe = f'\nWHERE hb2={self._dim2}'
                grp = '\nGROUP BY ha2'
        elif coltype == 'bag':
            new_dfcolumns = ['Attribute Collection', 'Items (instances)']
            count_label = 'Items (instances)'
            if missing:
                new_dfcolumns = ['Attribute Collection', 'Missing Items (NA)']
                count_label = 'Missing Items (NA)'
            if self._aset.filtered:
                sel = f'SELECT ha2, count(ha2) as cnt'
                frm = f'\nFROM HLink_{self._dim3}'
                whe = f'\nWHERE hb2={self._dim2} AND hb1 IN {self._aset.ent.old_set}'
                grp = '\nGROUP BY ha2'
            else:
                sel = f'SELECT ha2, count(ha2) as cnt'
                frm = f'\nFROM HLink_{self._dim3}'
                whe = f'\nWHERE hb2={self._dim2}'
                grp = '\nGROUP BY ha2'
        elif coltype == 'val':
            if self._aset.filtered:
                sel = f'SELECT count()'
                frm = f'\nFROM {self._aset.ent.old_set}'
            else:
                if estimate:
                    sel = f'SELECT uniq(hb1)'
                    frm = f'\nFROM HLink_{self._dim3}'
                    whe = f'\nWHERE hb2={self._dim2}'
                else:
                    sel = f'SELECT uniqExact(hb1)'
                    frm = f'\nFROM HLink_{self._dim3}'
                    whe = f'\nWHERE hb2={self._dim2}'
        else:
            raise ASetError(f'Cannot return count, check arguments')
        if projection:
            attrs = self._aset.ent.get_attributes(out='objects', csvlist=projection)
        else:
            attrs = self._aset.ent.get_attributes(out='objects')
        sql_query = sel + frm + whe + grp
        self._operation = 'Counting'
        if coltype == 'val':
            self._dfcolumns = 'HyperBonds'
            self.Res = sql_query
        else:
            self.Res = (sql_query, attrs, new_dfcolumns, count_label, missing, order)
    @_generative
    def Group(self, by, as_index=False):
        self.Res = self.Res.groupby(by=by.split(', '), sort=False, as_index=as_index).first()
    @_generative
    def Filter(self, mode='single', propagate=False):
        if mode not in ['single', 'multiple']:
            raise OperationError(f'Operation failed, unknown filtering mode')

        cnt_queries = len(self._aset.selectops)
        if mode == 'multiple' and cnt_queries < 2:
                raise OperationError(f'Filtering Operation in `multiple` mode failed.'
                                     f'\nThere must be at least two Select() operations defined '
                                     f'and added for {self._aset}')
        elif mode == 'single' and cnt_queries == 0:
                raise OperationError(f'Filtering Operation in `single` mode failed. '
                                     f'\nThere is not any cql.Select() operation defined and added for {self._aset}')
        hbset_subquery, sel_query, ndx = '', '', 1
        for elem in self._aset.selectops:
            hbsql, selsql = elem.Exe(exe=False).Res
            if mode == 'multiple':
                if ndx == 1:
                    hbset_subquery += hbsql
                else:
                    hbset_subquery = hbsql + 'AND hb1 IN' + f'\n({hbset_subquery})'
            if ndx < cnt_queries:
                sel_query += selsql + '\nUNION ALL'
            else:
                if not propagate:
                    sel_query += selsql
                else:
                    sel_query = sel_query[:-len('\nUNION ALL')]
            ndx += 1

        if mode == 'single':
            hbsql, selsql = self._aset.selectops[-1].Exe(exe=False).Res
            if self._aset.filtered:
                hbset_subquery = hbsql + 'AND hb1 IN ' + f'{self._aset.ent.old_set}'
            else:
                hbset_subquery = hbsql
        self.Res = {'update_pos': hbset_subquery, 'update_sel': sel_query}
        self._operation = 'Filtering'

    def _filtering(self, hbset_subquery, sel_query):
        updpos = []
        if self._aset.filtered:
            if self._aset.ent.new_set == f'{self._flt_prefix}_MEM_Z':
                updpos.append((f'DROP TABLE IF EXISTS {self._flt_prefix}_MEM_Z',
                               'Drop MEM_Z memory engine'))
                updpos.append((f'CREATE TABLE {self._aset.ent.new_set} ( hbz UInt32 ) ENGINE = Memory',
                               f'Create MEM_Z memory engine'))
            else:
                updpos.append((f'DROP TABLE IF EXISTS {self._flt_prefix}_MEM_X',
                               'Drop MEM_X memory engine'))
                updpos.append((f'CREATE TABLE {self._aset.ent.new_set} ( hbx UInt32 ) ENGINE = Memory',
                               f'Create MEM_X memory engine'))
        else:
            updpos.append((f'CREATE TABLE {self._flt_prefix}_MEM_X ( hbx UInt32 ) ENGINE = Memory',
                           'Create MEM_X memory engine'))
        updpos.append((f'INSERT INTO {self._aset.ent.new_set} {hbset_subquery}',
                       'Insert filtered HBonds'))
        updpos.append((f'\nDROP TABLE IF EXISTS {self._flt_prefix}_VW_pos',
                       'Drop VW_pos'))
        updpos.append((f'''
CREATE VIEW {self._flt_prefix}_VW_pos AS
SELECT any(hb2) AS hb2, groupArray(hb1) AS hb1arr, count() AS cnt, ha2, ha1, 1 AS pos, 0 AS sel  
FROM
(
SELECT *
FROM HLink_{self._dim3}
WHERE hb2={self._dim2} AND hb1 IN {self._aset.ent.new_set})
GROUP BY ha2, ha1
ORDER BY ha2, ha1
''',
                       'Create VW_pos'))
        updpos.append((f'INSERT INTO {self._hatom_states}\nSELECT * FROM {self._flt_prefix}_VW_pos',
                       'Insert VW_pos into HAtom_States'))
        updpos.append((f'\nOPTIMIZE TABLE {self._hatom_states} FINAL',
                       'Optimize HAtom_States'))
        updsel = []
        if sel_query:
            updsel.append((f'\nDROP TABLE IF EXISTS {self._flt_prefix}_VW_sel',
                           'Drop VW_sel'))
            updsel.append((f'\nCREATE VIEW {self._flt_prefix}_VW_sel AS {sel_query}',
                           'Create VW_sel'))
            updsel.append((f'\nINSERT INTO {self._hatom_states}\nSELECT * FROM {self._flt_prefix}_VW_sel',
                           'Insert VW_sel into HAtom_States'))
            updsel.append((f'\nOPTIMIZE TABLE {self._hatom_states} FINAL',
                           'Optimize HAtom_States'))
        else:
            updsel.append((f'\nSELECT 1', 'Dummy SQL statement'))

        return updpos, updsel

    @_generative
    def Exe(self, columns=None, index=None, exe=True):
        """
        :param columns: pandas dataframe columns
        :param index: pandas index
        :param exe: if true execute the sql query
        :return: result is returned with the self.Res
        This method executes the query constructed from previous generative calls
        it also modifies certain instance variables of ASET object
        """
        if self._operation not in oplist:
            raise OperationError(f'Operation failed. Unknown operation {self._operation}')

        if columns:
            self._dfcolumns = columns

        if self._operation == 'Filtering':
            sql_queries_dict = self.Res
            self.Res = self._filtering(sql_queries_dict['update_pos'], sql_queries_dict['update_sel'])
        self._aset.columns = self._dfcolumns
        self._aset.index = index
        self._aset.qid = self._operation
        self._aset.last_query = self.Res
        try:
            if self._operation == 'Filtering':
                t_start = time.time()
                self._aset.clear_states_engine_columns(['hb1arr', 'cnt', 'pos', 'sel'], exe=exe)
                for update_of_state in self.Res:
                    for sql_statement, query_id in update_of_state:
                        self._sql(sql_statement, qid=query_id, execute=exe)
                    if self._dbg > 1:
                        print('▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄')
                t_stop = time.time()
                if self._dbg > 1:
                    print(f'Filtering of ASET({self._aset.key})[{self._aset.alias}] is completed:')
                    print(f'Elapsed: {round(t_stop-t_start, 3)} sec')
                if exe:
                    self._aset.ent.old_set, self._aset.ent.new_set = self._aset.ent.new_set, self._aset.ent.old_set
                    self._aset.filtered = True
                    self._aset.hbonds = self._sql(f'SELECT count() FROM {self._aset.ent.old_set}',
                                                  qid='Counting').values[0][0]
            elif self._operation == 'Counting' and self._dfcolumns != 'HyperBonds':
                (cnt_query, attribs, new_dfcolumns, cntlbl, missing, order) = self.Res
                dfcnt = self._sql(cnt_query, cols='ha2, cnt', index='ha2', qid=self._operation, execute=exe)
                cnt_items_dict = dfcnt['cnt'].to_dict()
                if missing:
                    attr_items_dict = {obj.key: [obj.alias, (self._aset.hbonds - cnt_items_dict.get(obj.dim2, 0))]
                                       for obj in attribs}
                else:
                    attr_items_dict = {obj.key: [obj.alias, cnt_items_dict.get(obj.dim2, 0)] for obj in attribs}
                df_items_count = ETL.dict_to_dataframe(attr_items_dict, new_dfcolumns)
                if order == 'key':
                    self.Res = df_items_count.sort_index()
                elif order == 'col':
                    self.Res = df_items_count.sort_values(by='Attribute Collection', ascending=True)
                elif order == 'cnt':
                    self.Res = df_items_count.sort_values(by=cntlbl, ascending=False)
            else:
                self.Res = self._sql(self.Res, cols=self._dfcolumns, index=index, qid=self._operation, execute=exe)

        except Exception as e:
            raise print(e)

    @_generative
    def Display(self):
        print(self.Res)

    @_generative
    def Value(self):
        self.Res = self.Res.values[0][0]

