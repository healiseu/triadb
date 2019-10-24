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

from triadb.exceptions import DataResourceSystemError, MISError
from triadb.meta_models import Attribute, Field
from triadb.utils import ETL, highlight_states
from triadb.subsystems import DataModelSystem

from.hgraph import ASERD,HGPyDot
from.hacol import HACOL,HACQL
from.haset import ASET
class TriaClickEngine(object):
 def __init__(self,dmc,dms,drs,debug):
  self._mapping_pairs=[]
  self._dbg=debug 
  self._hacol=None 
  self._asets={} 
  self._aserd=None 
  self._dmc=dmc 
  self._dms=dms 
  self._drs=drs 
  self.chsql=dmc.sql
  self.chcmd=dmc.cmd
  engines_created=self.chsql(f'EXISTS table HAtom_{self._dms.key[0]}',qid='ExistsHAtom')[0][0]
  if engines_created:
   self.set_asets()
   self.set_aserd()
 def __repr__(self):
  return f'TriaClickEngine(\n\t{self._mmc}, \n\t{self._dmc}  )'
 @property
 def engines_created(self):
  res=self.chsql(f'EXISTS table HAtom_{self._dms.key[0]}',qid='ExistsHAtom')[0][0]
  if res:
   return True
  else:
   return False
 @property
 def _hltable(self):
  return self._dms.hltable
 @property
 def _dms_node(self):
  return self._dms.node
 @property
 def _dms3(self):
  return self._dms.dim3
 @property
 def _mmc(self):
  return self._dms.mmc
 @property
 def _hatable(self):
  return self._dms.hatable
 @property
 def _hatable_flt(self):
  return self._dms.hatable_flt
 @property
 def _hatable_states(self):
  return self._dms.hatable_states
 @property
 def _drs_node(self):
  return self._drs.node
 @property
 def _drs3(self):
  return self._drs.dim3
 @property
 def _table_name(self):
  return self._drs.table_name
 @property
 def _entity_key(self):
  return self._drs.entity_key
 @property
 def _imported(self):
  return self._drs.imported
 @_imported.setter
 def _imported(self,flag):
  self._drs._imported=flag
 @property
 def _loaded(self):
  return self._drs.loaded
 @property
 def hacol(self):
  return self._hacol
 @property
 def asets(self):
  d={(self._dms3,ent.dim2):self._create_aset(ent.dim2)for ent in self._dms.get_entities(out='objects')}
  return d
 @property
 def aserd(self):
  return self._aserd
 def _get_table_name(self,engine_short_name):
  tbl=None
  if engine_short_name=='hatom':
   tbl=self._hatable
  elif engine_short_name=='hatomStates':
   tbl=self._hatable_flt
  elif engine_short_name=='hlink':
   tbl=self._hltable
  elif engine_short_name in['Date','Float32','String','UInt16','UInt8']:
   tbl=f'HAtom_{self._dms3}_{engine_short_name}'
  return tbl
 def get_last_query_info(self):
  return self._dmc.get_last_query_info()
 def get_parts(self, table=None, alias=None, hb2=None, active=True, exe=True):
  if alias:
   return self._dmc.get_parts(table=self._get_table_name(alias), hb2=hb2, active=active, exe=exe)
  else:
   return self._dmc.get_parts(table=table, hb2=hb2, active=active, exe=exe)
 def get_columns(self, table=None, alias=None, aggregate=False, exe=True):
  if alias:
   return self._dmc.get_columns(table=self._get_table_name(alias), aggregate=aggregate, exe=exe)
  else:
   return self._dmc.get_columns(table=table, aggregate=aggregate, exe=exe)
 def get_table_engines(self,engine=None,table=None,exe=True):
  return self._dmc.get_tables(engine=engine,table=table,exe=exe)
 def get_aset(self,dim2=None,alias=None):
  if alias:
   result=self._asets[alias]
  elif dim2:
   result=self._asets[(self._dms3,dim2)]
  else:
   raise MISError(f'Cannot return Associative Entity Set (ASET), check parameters')
  return result
 def get_rows(self,aset_dim2=None,projection=None,pandas_columns=None,group_by=None,limit=10,offset=0,order_by=None,index=None,exe=True):
  if aset_dim2:
   filter_query=f'SELECT * FROM {self.get_aset(aset_dim2).ent.old_set}'
  else:
   filter_query=None
  if not self._imported or self._drs.type!='TBL':
   raise DataResourceSystemError(f'Failed: DataResource must be of type <TBL> and must be imported first')
  orderstr=order_by
  if not order_by:
   orderstr='rowno'
  if not projection:
   colnames=[obj.attribute.alias for obj in self._mmc.get_fields(out='objects')if obj.attribute]
   all_fields=colnames+['impdate','rowno']
   projection=', '.join([f'{tblcol}' for tblcol in all_fields])
  if filter_query:
   select_q=f'SELECT {projection} \nFROM {self._table_name} \nWHERE rowno IN ({filter_query})'
  else:
   select_q=f'SELECT {projection} \nFROM {self._table_name}'
  sql=select_q
  if group_by:
   sql+=f'\nGROUP BY {group_by}'
  if limit:
   sql+=f'\nORDER BY {orderstr} LIMIT {limit} OFFSET {offset}'
  else:
   sql+=f'\nORDER BY {orderstr}'
  if not pandas_columns:
   pandas_columns=projection
  if index:
   pandas_df=self.chsql(sql,cols=pandas_columns,index=index,qid='SelectRows',execute=exe)
  else:
   pandas_df=self.chsql(sql,cols=pandas_columns,qid='SelectRows',execute=exe)
  return pandas_df
 def get_rows_from_external_resource(self,projection=None,where=None,limit=None,exe=True):
  if self._drs.type!='TBL':
   raise DataResourceSystemError(f'Failed: DataResource must be of type <TBL>')
  if not self._entity_key:
   raise DataResourceSystemError(f'DataResource is not mapped')
  container_type,fp,structure,host,port,user,pwd,dbase,dbtable=[None]*9
  pandas_columns=[f'{obj.attribute.alias}' for obj in self._drs.get_fields(out='objects')if obj.attribute]
  if self._drs.ctype in['TSV','CSV']:
   if not projection:
    projection=', '.join(pandas_columns)
   else:
    raise DataResourceSystemError(f'Failed: <user defined projection > ' f'is not functional in the current release')
   if self._drs.ctype=='TSV':
    container_type='TabSeparatedWithNames'
   else:
    container_type='CSVWithNames'
   fp=self._drs_node.path
   structure=[f'{obj.attribute.alias} Nullable({obj.attribute.vtype})' for obj in self._drs.get_fields(out='objects')if obj.attribute]
  elif self._drs.ctype=='MYSQL':
   table_columns=[f'{obj.cname}' for obj in self._drs.get_fields(out='objects')if obj.attribute]
   if not projection:
    projection=', '.join([f'{tblcol} AS {pndcol}' for tblcol,pndcol in zip(table_columns,pandas_columns)])
   else:
    raise DataResourceSystemError(f'Failed: <user defined projection > ' f'is not functional in the current release')
   container_type='MySQL'
   host,port,user,pwd=self._mmc.host,self._mmc.port,self._mmc.user,self._mmc.pwd
   dbase,dbtable=self._drs_node.db,self._drs_node.cname
  else:
   raise DataResourceSystemError(f'Failed: DataResource must have container type  <ctype in TSV, CSV, MYSQL>')
  result=self.chcmd(cmd='select',source=container_type,dbhost=host,dbport=port,dbuser=user,dbpassword=pwd,db=dbase,table=dbtable,fullpath=fp,heading=structure,fields=pandas_columns,projection=projection,where=where,limit=limit,execute=exe)
  return result
 def get_tuples(self,*dims,aset_dim2,projection=None,group_by=None,limit=None,offset=0,order_by=None,pandas_columns=None,index=None,exe=True,hb2=False,hb1=False):
  sel_projection,sel,frm,fjn,sql_query,cnt,cntcolumns='','','','','',0,len(dims)
  for dim in dims:
   cnt+=1
   hacol=self.set_hacol(dim)
   alias=hacol.alias
   sel_projection+=f'{alias}, '
   if hacol.is_junction:
    entdim=aset_dim2
   else:
    entdim=None
   itemsq=self.get_items(dim,aset_dim2=entdim,projection='$v, $hb, $hl',limit=None,excluded=False,exe=False,highlight=False)
   hlinksq=f'SELECT hb2, hb1, val AS {alias} ' f'\nFROM\n({itemsq})\nARRAY JOIN hb1arr AS hb1' f'\nORDER BY hb1'
   if cnt==1:
    sql_query=hlinksq
   if cnt==cntcolumns:
    sel=f'SELECT {sel_projection[:-2]}'
    if hb2:
     sel+=f', {aset_dim2} AS hb2'
    if hb1:
     sel+=f', hb1'
   elif cnt>1:
    sel=f'SELECT hb1, {sel_projection[:-2]}'
   if cnt>1:
    frm=f'\nFROM\n\n({sql_query})'
    fjn=f'\n\nFULL JOIN\n\n({hlinksq})\n\nUSING hb1'
    sql_query=sel+frm+fjn
  if group_by:
   sql_query=f'SELECT {projection} \nFROM\n({sql_query}) \nGROUP BY {group_by}'
  if order_by:
   sql_query+=f'\nORDER BY {order_by}'
  if limit:
   sql_query+=f'\nLIMIT {limit} OFFSET {offset}'
  if not pandas_columns:
   pandas_columns=f'{sel_projection[:-2]}'
   if hb2:
    pandas_columns+=', hb2'
   if hb1:
    pandas_columns+=', hb1'
  if exe:
   if index:
    result=self.chsql(sql_query,cols=pandas_columns,index=index)
   else:
    result=self.chsql(sql_query,cols=pandas_columns)
  else:
   result=sql_query
  return result
 def get_items(self,dim2=None,aset_dim2=None,alias=None,projection=None,limit=None,order_by=None,excluded=None,exe=True,index=None,highlight=True,caption=None):
  hacol=None
  if aset_dim2:
   parent_entity=self.get_aset(aset_dim2).ent
  else:
   parent_entity=None
  if dim2:
   hacol=self.set_hacol(dim2=dim2,dms_entity=parent_entity)
  elif alias:
   hacol=self.set_hacol(alias=alias,dms_entity=parent_entity)
  orderstr=order_by
  if not order_by:
   orderstr='$c DESC, $v DESC'
  if not projection:
   if hacol._is_filtered():
    projection='$v, $c, $s, $p'
   else:
    projection='$v, $c'
  if limit:
   obj=hacol.cql.Over(projection,excluded=excluded).Order(orderstr).Limit(limit)
  else:
   obj=hacol.cql.Over(projection,excluded=excluded).Order(orderstr)
  if index:
   pandas_df=obj.Exe(index=index,exe=exe).Res
  else:
   pandas_df=obj.Exe(exe=exe).Res
  if not caption:
   caption=hacol.attrib.name
  if hacol._is_filtered()and highlight:
   result=pandas_df.style. apply(highlight_states,axis=1). set_table_attributes("style='display:inline'"). set_caption(caption)
  elif highlight:
   result=pandas_df.style. set_table_attributes("style='display:inline'"). set_caption(caption)
  else:
   result=pandas_df
  return result
 def get_selections(self):
  return{aset:self.get_aset(dim2=k[1]).get_selections()for k,aset in self.asets.items()}
 def count_items(self):
  return[aset.count('set')for k,aset in self.asets.items()]
 '''
    ###############################################################################################################
        <----------------- Methods for Construction of Dictionary, Hypergraph and States engines ---------------> 
    ###############################################################################################################
    ''' 
 def _create_datatype_dictionary_engines(self,exe=True):
  vtypes=set([obj.vtype for obj in self._dms.get_attributes(out='objects')])
  for vtype in vtypes:
   structure=['ha2 UInt16','ha1 UInt32',f'val {vtype}','cnt UInt32','hb2 UInt16','hb1arr Array(UInt32)']
   self.chcmd(cmd='create',table=f'HAtom_{self._dms3}_{vtype}',heading=structure,engine='ReplacingMergeTree',partkey='ha2',skey='(hb2, ha2, val)',settings='old_parts_lifetime = 30',execute=exe)
  return exe
 def _create_hypergraph_engines(self,exe=True):
  structure1=['ha2 UInt16','ha1 UInt32','hb2 UInt16','hb1 UInt32']
  self.chcmd(cmd='create',table=self._hltable,heading=structure1,engine='MergeTree',partkey='(hb2, ha2)',skey='(hb2, ha2, ha1)',settings='old_parts_lifetime = 30',execute=exe)
  structure2=['ha2 UInt16','ha1 UInt32','cnt UInt32','hb2 UInt16','hb1arr Array(UInt32)']
  self.chcmd(cmd='create',table=self._hatable,heading=structure2,engine='MergeTree',partkey='ha2',skey='(hb2, ha2, ha1)',settings='old_parts_lifetime = 30',execute=exe)
  return exe
 def _reset_states_engine(self):
  self.chsql(f'\nALTER TABLE {self._hatable_states} DROP COLUMN hb1arr',qid='Drop column hb1arr')
  self.chsql(f'ALTER TABLE {self._hatable_states} ADD COLUMN hb1arr Array(UInt32) DEFAULT [] AFTER hb2',qid='Add column hb1arr')
  self.chsql(f'\nALTER TABLE {self._hatable_states} DROP COLUMN cnt',qid='Drop column cnt')
  self.chsql(f'ALTER TABLE {self._hatable_states} ADD COLUMN cnt UInt32 DEFAULT 0 AFTER hb1arr',qid='Add column cnt')
  self.chsql(f'\nALTER TABLE {self._hatable_states} DROP COLUMN pos',qid='Drop column pos')
  self.chsql(f'ALTER TABLE {self._hatable_states} ADD COLUMN pos UInt8 DEFAULT 0 AFTER ha1',qid='Add column pos')
  self.chsql(f'ALTER TABLE {self._hatable_states} DROP COLUMN sel',qid='Drop column sel')
  self.chsql(f'ALTER TABLE {self._hatable_states} ADD COLUMN sel UInt8 DEFAULT 0 AFTER pos',qid='Add column sel')
 def create_states_engine(self,exe=True):
  hatom_heading=['hb2 UInt16','hb1arr Array(UInt32)','cnt UInt32','ha2 UInt16','ha1 UInt32']
  self.chcmd(cmd='create',table=self._hatable_flt,heading=hatom_heading,engine='ReplacingMergeTree',partkey='(hb2, ha2)',skey='(hb2, ha2, ha1)',settings='old_parts_lifetime = 30',execute=exe)
  self.chcmd(cmd='insert',source='TableEngine',table=self._hatable_flt,fields=['hb2','hb1arr','cnt','ha2','ha1'],sql=self._hatable,execute=exe)
  self.chsql(f'ALTER TABLE {self._hatable_flt} ADD COLUMN pos UInt8 DEFAULT 0 AFTER ha1',qid='AddColumn',execute=exe)
  self.chsql(f'ALTER TABLE {self._hatable_flt} ADD COLUMN sel UInt8 DEFAULT 0 AFTER ha1',qid='AddColumn',execute=exe)
  self.optimize_parts('hatomStates',exe=exe)
  return self._hatable_flt
 def _rebuild_states_engine(self,exe=True):
  t_start=time.time()
  if self._dbg>0:
   print('\n┃▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔ STARTED ▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔┃')
  hatom_table=self.create_states_engine(exe=exe)
  t_stop=time.time()
  if self._dbg>0:
   print(f'Rebuild of states engine {hatom_table} is completed:')
   print(f'Elapsed: {round(t_stop-t_start, 3)} sec')
   print('\n⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗ FINISHED REBUILD OF STATES ENGINE ⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗')
 def _create_engines(self,exe=True):
  self._create_datatype_dictionary_engines(exe=exe)
  self._create_hypergraph_engines(exe=exe)
  if self._dbg>0:
   print(f'\nBuilding stage of datatype dictionary engines and hypergraph engines is completed.')
   print('▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄')
  return exe
 def _load_hypergraph_engines(self,exe=True):
  vtypes=[]
  model_dim=None
  for field in self._drs.get_fields(out='objects'):
   attref=field.attribute
   if attref:
    vtypes.append(attref.vtype)
    model_dim=attref.key[1]
  vtypes=set(vtypes)
  for vtype in vtypes:
   hatom_structure=['ha2','ha1','hb2','arrayJoin(hb1arr) hb1']
   self.chcmd(cmd='insert',source='DataTypeDictionary',table=f'HLink_{model_dim}',fields=hatom_structure,sql=f'HAtom_{model_dim}_{vtype}',execute=exe)
  for vtype in vtypes:
   hatom_structure=['ha2','ha1','cnt','hb2','hb1arr']
   self.chcmd(cmd='insert',source='DataTypeDictionary',table=f'HAtom_{model_dim}',fields=hatom_structure,sql=f'HAtom_{model_dim}_{vtype}',execute=exe)
  '''
        SELECT [ha2, ha1] AS ha,
                groupArray([hb2, hb1]) AS hbs
        FROM HLink_200
        GROUP BY ha2, ha1 order by ha;
        '''  
  return exe
 def _get_hyperatom_adjacency_lists(self,attr_alias,exe=False):
  structure=[f'{attr_alias} val','toUInt32(count(*)) cnt',f'toUInt16({self._entity_key[2]}) hb2','groupArray(rowno) hb1arr']
  column_names=['val','cnt','hb2','hb1arr']
  result=self.chcmd(cmd='select',source='ImportedDataResource',table=self._table_name,heading=structure,fields=column_names,execute=exe)
  return result
 def _load_datatype_dictionary(self,fld,exe=True):
  modeldim=fld.attribute.dim3
  attrdim=fld.attribute.dim2
  dtype=fld.attribute.vtype
  alias=fld.attribute.alias
  attr_exists=None
  if fld.attribute.junction:
   attr_exists_query=f'''
            SELECT toUInt16({attrdim}) IN 
            (SELECT ha2 FROM HAtom_{modeldim}_{dtype} GROUP BY ha2 ) AS attr_exists
            '''   
   attr_exists=self.chsql(attr_exists_query,cols='attr_exists',qid='AttributeExists',execute=True).values[0][0]
  select_cmd=self._get_hyperatom_adjacency_lists(alias,exe=False)
  if attr_exists==1:
   colnames=['ha2','ha1','A.val','A.cnt','A.hb2','A.hb1arr']
   source_param='ImportedDataResourceWithRightJoin'
  else:
   colnames=[f'toUInt16({attrdim}) AS ha2','toUInt32(rowNumberInAllBlocks()) AS ha1','val','cnt','hb2','hb1arr']
   source_param='ImportedDataResource'
  self.chcmd(cmd='insert',source=source_param,table=f'HAtom_{modeldim}_{dtype}',fields=colnames,ha2=attrdim,sql=select_cmd,execute=exe)
  return exe
 def _load_dictionaries(self,exe=True):
  if not self._imported:
   raise DataResourceSystemError(f'DataResource DRS:{self._drs.type}:{self._drs.key} is not imported')
  if not self._loaded:
   raise DataResourceSystemError(f'Data model engines must be initialized first')
  for field in self._drs.get_fields(out='objects'):
   if field.attribute:
    self._load_datatype_dictionary(field,exe=exe)
  return exe
 def _initialize_process(self):
  if not self._drs.type=='DS' and self._drs.ctype not in['MYSQL','CSV','TSV']:
   raise MISError(f'DataResource object must be of type `DS` ' f'with a container type `MYSQL`, `CSV`, or `TSV` to import data, ')
  dsdreslist=self._drs.get_tables(out='objects')
  objlist=[]
  for obj in dsdreslist:
   if self._drs.switch(obj.key[1],obj.key[2]).check_mapping():
    objlist.append(obj)
  if not objlist:
   raise MISError(f'DataSet must have at least one data resource mapped to import data, ')
  if self._dbg>0:
   print('\n┃▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔ STARTED ▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔┃')
  return objlist
 def load_data(self,exe=True):
  t_start=time.time()
  objlist=self._initialize_process()
  self._create_engines()
  for obj in objlist:
   self._drs.switch(obj.key[1],obj.key[2])
   self._load_dictionaries(exe=exe)
   t_loading_dres=time.time()
   if self._dbg>1:
    print('Loading data from data resource into dictionaries is finished:')
    print(self._drs)
    print(f'Elapsed: {round(t_loading_dres-t_start, 3)} sec')
    print('▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄')
  self._drs.switch(self._drs3,0)
  t_loading_hypergraph_engines=time.time()
  self._load_hypergraph_engines(exe=exe)
  self.optimize_parts('hatom')
  self.optimize_parts('hlink')
  if self._dbg>1:
   print('Loading data from data type dictionaries onto the hypergraph engines is finished:')
   print(self._drs)
   print(f'Elapsed: {round(t_loading_hypergraph_engines-t_start, 3)} sec')
   print('▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄')
  t_stop=time.time()
  if self._dbg>0:
   print(f'\nLoading data from data resources is completed:')
   print(self._drs)
   print(f'Elapsed: {round(t_stop-t_start, 3)} sec')
   print('\n⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗ FINISHED LOADING ⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗')
  self._rebuild_states_engine()
 def optimize_parts(self,engine_shortname,exe=True):
  tbl_name=self._get_table_name(engine_shortname)
  return self._dmc.optimize_parts(table=tbl_name,exe=exe)
 def rebuild_hypergraph_engines(self):
  pass
 '''
    ###############################################################################################################
                <----------------- Methods for Mapping DataResources on a DataModel ---------------> 
    ###############################################################################################################
    ''' 
 def compare_fields_with_attributes(self,matching_pairs,graph=False):
  rows,left_ids,left_labels,right_ids,right_labels=[],[],[],[],[]
  for fld_id,attr_id in matching_pairs:
   pair=fld_id,attr_id
   if pair not in self._mapping_pairs:
    self._mapping_pairs.append(pair)
   fld_cname,attr_alias=Field.find(fld_id).cname,Attribute.find(attr_id).alias
   left_ids.append(fld_id)
   left_labels.append(fld_cname)
   right_ids.append(attr_id)
   right_labels.append(attr_alias)
   rows.append([fld_id,fld_cname,attr_id,attr_alias])
  columns=['fld_key','fld_nam','attr_key','attr_alias']
  if graph:
   pydot_graph=HGPyDot.pydot_bipartite(gseperation=0.2,nseperation=0.6,left_label='Fields',left_nodes=left_ids,left_nlabels=left_labels,right_label='Attributes',right_nodes=right_ids,right_nlabels=right_labels)
   result=pydot_graph.bipartite_mapping(matching_pairs).draw()
  else:
   result=ETL.get_dataframe(rows,columns=columns)
  return result
 def reset_mapping(self):
  self._mapping_pairs = []
  if self._dbg > 0:
   print(f'Mapping pairs have been reset: {self._mapping_pairs}')
 def add_mapping(self):
  if not self._mapping_pairs:
   raise MISError(f'List of enumerated key pairs has not been set')
  for fld_id,attr_id in self._mapping_pairs:
   fld,attr=Field.find(fld_id),Attribute.find(attr_id)
   fld.attribute().associate(attr)
   fld.save()
  return self._drs.check_mapping()
 '''
    ###############################################################################################################
                   <----------------- Methods for Importing DataResources ---------------> 
    ###############################################################################################################
    ''' 
 def _create_import_engine(self,exe=True):
  structure=['impdate Date','rowno UInt32']+ [f'{obj.attribute.alias} Nullable({obj.attribute.vtype})' for obj in self._drs.get_fields(out='objects')if obj.attribute]
  return self.chcmd(cmd='create',table=self._table_name,heading=structure,engine='MergeTree',partkey='impdate',skey='(impdate, rowno)',settings='old_parts_lifetime = 30',execute=exe)
 def _import(self,exe=True):
  if not self._entity_key:
   raise DataResourceSystemError(f'DataResource DRS:{self._drs.type}:{self._drs.key} is not mapped onto a data model')
  select_from_file_cmd=self.get_rows_from_external_resource(exe=False)
  self._create_import_engine(exe=exe)
  colnames=['today()','rowNumberInAllBlocks()']+ [f'{obj.attribute.alias}' for obj in self._drs.get_fields(out='objects')if obj.attribute]
  self.chcmd(cmd='insert',source='file',table=self._table_name,fields=colnames,sql=select_from_file_cmd,execute=exe)
  self._imported=True
  return self._dmc.qstats[3]
 def import_data(self,exe=True):
  t_start=time.time()
  total_rows=0 
  objlist=self._initialize_process()
  cnt=0
  for obj in objlist:
   self._drs.switch(obj.key[1],obj.key[2])
   cnt=self._import(exe=exe)
   total_rows+=cnt
   if self._dbg>1:
    t_split=time.time()
    print('Importing data from data resource finished:')
    print(self._drs)
    print(f'Imported {cnt} rows')
    print(f'Elapsed: {round(t_split-t_start, 3)} sec')
    print('▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄')
  self._drs.switch(self._drs3,0)
  if self._dbg>0:
   t_split=time.time()
   print(f'\nImporting data from data set is completed:')
   print(self._drs)
   print(f'Total rows imported: {total_rows}')
   print(f'Total time elapsed : {round(t_split-t_start, 3)} sec')
   print('\n⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗ FINISHED IMPORTING DATA ⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗')
  return cnt
 '''
    ###############################################################################################################
                <----------------- Methods for creating and setting ASETs, HACOL, etc.. ---------------> 
    ###############################################################################################################
    ''' 
 def set_aserd(self):
  self._aserd=ASERD(name=self._dms.name,alias=self._dms.alias,key=(self._dms.dim3,self._dms.dim2))
  self._aserd.add_asets(self.asets)
  return self._aserd
 def _create_aset(self,dim2=None):
  dms_entity=DataModelSystem(self._mmc,dim3=self._dms3,dim2=dim2,debug=self._dbg)
  return ASET(self,dms_entity)
 def set_asets(self):
  self._asets={(ent.dim3,ent.dim2):self._create_aset(ent.dim2)for ent in self._dms.get_entities(out='objects')}
  self._asets.update({aset.ent.alias:aset for(key,aset)in self._asets.items()})
  self.set_aserd()
  result=[self._asets[(ent.dim3,ent.dim2)]for ent in self._dms.get_entities(out='objects')]
  return result
 def restart(self):
  self._reset_states_engine()
  result=[]
  if not self._asets:
   self.set_asets()
   self.set_aserd()
  for ent in self._dms.get_entities(out='objects'):
   aset=self._asets[(ent.dim3,ent.dim2)]
   aset.reset()
   result.append(aset)
  return result
 def _create_dms_attr(self,dim2=None,alias=None,dbg=None):
  dms_attr=DataModelSystem(self._mmc,dim3=self._dms3,dim2=dim2,alias=alias,debug=dbg)
  return dms_attr
 def set_hacol(self,dim2=None,alias=None,dms_entity=None):
  dms_attribute=self._create_dms_attr(dim2,alias,self._dbg)
  if dms_entity:
   self._hacol=HACOL(self,dms_attribute,pentity=dms_entity.node)
  else:
   self._hacol=HACOL(self,dms_attribute)
  return self._hacol
 '''
    ###############################################################################################################
                <----------------- Methods for Associative Filtering ---------------> 
    ###############################################################################################################
    ''' 
 def _create_junction_attribute_set(self,flt_aset,jattr_dim2):
  ju_hacol=self.set_hacol(jattr_dim2,dms_entity=flt_aset.ent)
  ju_query=ju_hacol.cql.Over('$1',excluded=False).Exe(exe=False).Res
  junction_memory_engine=f'{flt_aset.flt_prefix}_MEM_ha1'
  self.chsql(f'\nDROP TABLE IF EXISTS {junction_memory_engine}')
  self.chsql(f'CREATE TABLE {junction_memory_engine} ( ha1 UInt32 ) ENGINE = Memory')
  self.chsql(f'INSERT INTO {junction_memory_engine}\n{ju_query}')
  return junction_memory_engine
 def _filter_successor(self,flt_aset,successor_aset,junction_attr_key,):
  attrib_dim2=junction_attr_key[1]
  junction_memory_engine=self._create_junction_attribute_set(flt_aset,attrib_dim2)
  successor_hacol=self.set_hacol(attrib_dim2,dms_entity=successor_aset.ent)
  successor_sel=successor_hacol.cql.Select().Where(f'ha1 IN {junction_memory_engine}')
  successor_aset.add_selections(successor_sel)
  successor_aset.cql.Filter(mode='single',propagate=True).Exe()
  successor_aset.del_selections(-1)
 def _get_aset_from_selection(self,selection):
  ent=selection.hacol.pentity
  aset=self.get_aset(ent.dim2)
  return aset
 def _filter(self,start_aset,mode):
  t_start=time.time()
  if self._dbg>0:
   print('\n┃▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔ STARTED ▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔┃')
  start_aset.cql.Filter(mode=mode).Exe()
  for head_node_key,tail_node_key,edge_key in self.aserd.get_bfs_edges(start_aset.key[1]):
   self._filter_successor(self._asets[head_node_key],self._asets[tail_node_key],edge_key)
  t_stop=time.time()
  if self._dbg>0:
   print('▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄')
   print('▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄')
   print(f'Filtering is completed:')
   print(f'Total Elapsed Time: {round(t_stop-t_start, 3)} sec')
   print('\n⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗  FINISHED FILTERING ⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗')
  return round(t_stop-t_start,3)
 def filter_selections(self,cql_selections,mode='single'):
  if mode not in['single','multiple']:
   raise MISError('Operation failed, unknown filtering mode')
  ellapsed_time=[]
  if isinstance(cql_selections,list):
   start_filtering_aset=None
   for selection in cql_selections:
    if not isinstance(selection,HACQL):
     raise MISError('Operation failed, list must contain only CQL Select() objects')
    elif mode=='single':
     start_aset=self._get_aset_from_selection(selection)
     start_aset.add_selections(selection)
     ellapsed_time.append(self._filter(start_aset,mode='single'))
    elif mode=='multiple':
     aset_from_selection=self._get_aset_from_selection(selection)
     if not start_filtering_aset:
      start_filtering_aset=aset_from_selection
      start_filtering_aset.add_selections(selection)
     elif start_filtering_aset==aset_from_selection:
      start_filtering_aset.add_selections(selection)
     else:
      print(f'*** WARNING *** Selection - {selection} is skipped')
   if mode=='multiple':
    ellapsed_time.append(self._filter(start_filtering_aset,mode='multiple'))
  elif isinstance(cql_selections,HACQL):
   if mode in['single','multiple']:
    start_aset=self._get_aset_from_selection(cql_selections)
    start_aset.add_selections(cql_selections)
    ellapsed_time.append(self._filter(start_aset,mode=mode))
   else:
    raise MISError("Operation failed, filtering with a single selection requires mode='single'")
  else:
   raise MISError(f'Operation failed wrong type of arguments')
  return ellapsed_time
 def filter_values(self,expr,dim2=None,alias=None,In=False,csvtype='string'):
  hacol=None
  if dim2:
   hacol=self.set_hacol(dim2)
  elif alias:
   hacol=self.set_hacol(alias=alias)
  if In:
   sel=hacol.cql.Select().Where('$v').In(expr,csvtype=csvtype)
  else:
   sel=hacol.cql.Select().Where(expr)
  return self.filter_selections(sel)


