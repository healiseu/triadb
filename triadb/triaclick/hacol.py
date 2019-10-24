"""
This file is part of TriaClick Associative Semiotic Hypergraph Engine
(C) 2018-2019 Athanassios I. Hatzis
Licensed under the TriaClick Open Source License Agreement (TOSLA)
You may not use this file except in compliance with TOSLA.
The files subject to TOSLA are grouped in this directory to clearly separate them from files
in the parent directory that are licensed under GNU Affero General Public License v.3.0.
You should retain this header in the file and a copy of the LICENSE_TOSLA file in the current directory
"""
from triadb.utils import ETL
from triadb.exceptions import HACOLError,OperationError,ClickHouseException,DataModelSystemError
from.generative import GenerativeBase,_generative
oplist=['Counting','Sum','Average','Projection','Selection']
out_types=['single','list','keys','tuple','dict','ids','set of items','tuple of items']
class HACOL(object):
 def __init__(self,engine,dms_attribute,pentity=None):
  if dms_attribute.type!='ATTR':
   raise HACOLError(f'HyperAtom collection objects (HACOL) can be create only from DMS:ATTR objects')
  self._attrib=dms_attribute
  self._engine=engine
  self.chsql=self._engine.chsql
  self._filtered=None 
  self._hatoms=None 
  if pentity:
   self._pentity=pentity 
  else:
   self._pentity=None 
  self._update_instance_vars()
  self._type='HACOL' 
  self._last_query=None 
  self._columns=None 
  self._index=None 
  self._qid=None 
 def _update_instance_vars(self):
  self._alias=self._attrib.alias 
  self._vtype=self._attrib.vtype 
  self._vcolname=ETL.dtype2column[self._attrib.vtype] 
  self._is_junction=self._attrib.node.junction
  self._seldict={'dim3':self.dim3,'dim2':self.dim2}
  self._pentities=self._attrib.parents
  if not self._pentity:
   self._pentity=self._pentities[0]
  self._filtered=self._is_filtered() 
  self._hatoms=self.count() 
 def __repr__(self):
  filtered=''
  if self._filtered:
   filtered=' <FLT>'
  return f'{self._type}{self.key}[{self._alias}]{filtered}'
 def __str__(self):
  filtered=''
  if self._filtered:
   filtered=' <FILTERED>'
  return f'{self._type}{self.key}[{self._alias}] = {self._hatoms} hatoms{filtered}'
 def _is_filtered(self):
  filtered=False
  flt_prefix=f'FLT_{self._pentity.dim3}_{self._pentity.dim2}'
  sql_views_info=self._engine.get_table_engines(engine='View',table=f'{flt_prefix}_VW')
  if sql_views_info is not None:
   filtered=True
  return filtered
 @property
 def is_junction(self):
  return self._is_junction
 @property
 def pentities(self):
  return self._pentities
 @property
 def pentity(self):
  return self._pentity
 @pentity.setter
 def pentity(self,ent):
  self._pentity=ent
 @property
 def seldict(self):
  return self._seldict
 @property
 def filtered(self):
  return self._filtered
 @property
 def str(self):
  print(self)
  return None
 @property
 def attrib(self):
  return self._attrib
 @property
 def alias(self):
  return self._alias
 @property
 def type(self):
  return self._type
 @property
 def vtype(self):
  return self._vtype
 @property
 def vcolname(self):
  return self._vcolname
 @property
 def dbg(self):
  return self._attrib.dbg
 @property
 def datadb(self):
  return self._attrib.dmc.datadb
 @property
 def dim3(self):
  return self._attrib.dim3
 @property
 def dim2(self):
  return self._attrib.dim2
 @property
 def key(self):
  return self.dim3,self.dim2
 @property
 def hatoms(self):
  return self._hatoms
 @property
 def last_query(self):
  if self._qid=='Over':
   print(self._last_query[0])
  else:
   print(self._last_query)
  return self._last_query
 @property
 def last_query_info(self):
  return self._engine.get_last_query_info()
 @property
 def cql(self):
  return HACQL(self)
 def switch(self,dim2=None,alias=None):
  try:
   self._attrib.switch(dim3=self.dim3,dim2=dim2,alias=alias)
   self._update_instance_vars()
  except DataModelSystemError:
   raise DataModelSystemError(f'Failed to switch to Attribute with ' f'dim3={self.dim3}, dim2={dim2}, alias={alias}')
  return self
 def count(self):
  self._filtered=self._is_filtered()
  if self._filtered:
   self._hatoms=self.cql.Count(coltype='set',filtered=True,total=False).Exe().Value().Res
  else:
   self._hatoms=self.cql.Count(coltype='set',filtered=False,total=False).Exe().Value().Res
  return self._hatoms
 def repeat_execution(self,exe=False):
  return self.chsql(self._last_query,cols=self._columns,index=self._index,qid=self._qid,execute=exe)
class HACQL(GenerativeBase):
 def __init__(self,hacol,result=None):
  self.Res=result
  self._hacol=hacol
  self.sql=hacol.chsql
  self._dbg=hacol.dbg
  self._key=hacol.key
  self._dim3=hacol.dim3
  self._dim2=hacol.dim2
  self._alias=hacol.alias
  self._fltred=hacol.filtered
  self.seldict=hacol.seldict
  self._selected=None
  self._operation='UNDEFINED'
  self._dfcolumns=None 
  self._vtype=self._hacol.vtype
  self._vcolname=self._hacol.vcolname
  self._repr='' 
 def __repr__(self):
  return f'CQL{self._key}[{self._alias}].{self._repr}'
 @property
 def hacol(self):
  return self._hacol
 @_generative
 def Average(self):
  select_part=f'SELECT avg(val)'
  from_part=f'\nFROM HAtom_{self._dim3}_{self._vtype}'
  where_part=f'\nWHERE ha2={self._dim2} '
  self.Res=select_part+from_part+where_part
  self._operation='Average'
  self._dfcolumns='average'
 @_generative
 def Sum(self):
  select_part=f'SELECT sum(val)'
  from_part=f'\nFROM HAtom_{self._dim3}_{self._vtype}'
  where_part=f'\nWHERE ha2={self._dim2} '
  self.Res=select_part+from_part+where_part
  self._operation='Sum'
  self._dfcolumns='sum'
 @_generative
 def Count(self,coltype='val',filtered=False,total=False):
  (select_part,from_part,where_part)=('','','')
  if total:
   select_part=f'SELECT count()'
  else:
   select_part=f'SELECT count(ha2)'
  if coltype=='set':
   from_part=f'\nFROM HAtom_{self._dim3}States'
  elif coltype=='bag':
   from_part=f'\nFROM HLink_{self._dim3}'
  elif coltype=='val':
   from_part=f'\nFROM HAtom_{self._dim3}_{self._vtype}'
  else:
   raise OperationError(f'Operation failed, check ``Count`` parameters')
  if filtered:
   where_part+='\nWHERE pos=1 '
   if not total:
    where_part+=f'AND ha2={self._dim2} '
  else:
   if not total:
    where_part+=f'\nWHERE ha2={self._dim2} '
  if(coltype=='bag' and filtered)or(coltype=='val' and filtered):
   raise OperationError(f'Count operation with these parameters is not implemented')
  self.Res=select_part+from_part+where_part
  self._operation='Counting'
  self._dfcolumns='count'
 @_generative
 def Over(self,projection=None,unfiltered=None,excluded=None):
  if not projection:
   projection='$2, $1, $v, $c'
  sql_columns,df_columns='',''
  for col in projection.split(', '):
   if col=='$2':
    sql_columns+='ha2, '
    df_columns+='HA2, '
   elif col=='$1':
    sql_columns+='ha1, '
    df_columns+='HA1, '
   elif col=='$k':
    sql_columns+='[ha2, ha1] as hatom, '
    df_columns+='HA, '
   elif col=='$fk':
    sql_columns+=f'[{self._dim3}, ha2, ha1] as hatom, '
    df_columns+='HA, '
   elif col=='$v':
    sql_columns+='val, '
    df_columns+=f'{self._alias}, '
   elif col=='$c':
    sql_columns+='cnt, '
    df_columns+='FREQ, '
   elif col=='$s':
    if self._fltred:
     sql_columns+='sel, '
     df_columns+='S, '
    else:
     raise OperationError(f'Operation failed, $s, $p are valid only in filtered state')
   elif col=='$p':
    if self._fltred:
     sql_columns+='pos, '
     df_columns+='P, '
    else:
     raise OperationError(f'Operation failed, $s, $p are valid only in filtered state')
   elif col=='$hb':
    sql_columns+='hb2, '
    df_columns+='HB2, '
   elif col=='$hl':
    sql_columns+='hb1arr, '
    df_columns+='HL, '
   else:
    raise OperationError(f'Operation failed, cannot parse `projection` argument')
  sql_columns=sql_columns.rstrip(', ')
  left_sel,left_frm,right_sel,right_frm,right_whe='','','','',''
  if unfiltered:
   in_filtered_state=not unfiltered
  else:
   in_filtered_state=self._fltred
  left_sel+='SELECT '
  left_sel+=f'{sql_columns} '
  if in_filtered_state:
   left_frm=f'\nFROM HAtom_{self._dim3}States'
  else:
   left_frm=f'\nFROM HAtom_{self._dim3}'
  left_sql=left_sel+left_frm
  if self._hacol.is_junction:
   right_sel+='SELECT ha2, ha1, val, hb2'
   right_frm+=f'\nFROM HAtom_{self._dim3}_{self._vtype}'
   right_whe+=f'\nWHERE ha2={self._dim2} AND hb2={self._hacol.pentity.dim2} '
  else:
   right_sel+='SELECT ha2, ha1, val'
   right_frm+=f'\nFROM HAtom_{self._dim3}_{self._vtype}'
   right_whe+=f'\nWHERE ha2={self._dim2} '
  right_sql=right_sel+right_frm+right_whe
  if excluded is True:
   exc_whe='\nWHERE pos=0'
  elif excluded is False:
   exc_whe='\nWHERE pos=1'
  else:
   exc_whe=''
  self.Res={'left':left_sql,'right':right_sql,'end':'','exc':exc_whe}
  self._operation='Projection'
  self._dfcolumns=df_columns.rstrip(', ')
 @_generative
 def Select(self):
  sel='\nSELECT arrayJoin(hb1arr) AS hb1'
  frm=f'\nFROM HAtom_{self._dim3}_{self._vtype}'
  if self._hacol.is_junction:
   whe=f'\nWHERE ha2={self._dim2} AND hb2={self._hacol.pentity.dim2} '
  else:
   whe=f'\nWHERE ha2={self._dim2} '
  self.Res=sel+frm+whe
  self._operation='Selection'
  self._dfcolumns='hb1'
 @_generative
 def Where(self,cond):
  cond_arg=cond
  self._repr+=f'Where({cond})' 
  if cond=='$v':
   where_part=f'AND val '
  else:
   cond=cond.replace('$v','val ')
   where_part=f'AND {cond} '
  if self._operation=='Projection':
   self.Res['right']+=where_part
  elif self._operation=='Selection':
   self.seldict['where']=f'{cond_arg}'
   self.Res+=where_part
  else:
   self.Res+=where_part
 @_generative
 def Not(self):
  self._repr+=f'.Not()' 
  if self._operation=='Projection':
   self.Res['right']+='NOT '
  else:
   self.Res+='NOT '
 @_generative
 def And(self):
  self._repr+=f'.And()' 
  if self._operation=='Projection':
   self.Res['right']+='AND '
  else:
   self.Res+='AND '
 @_generative
 def Or(self):
  self._repr+=f'.Or()' 
  if self._operation=='Projection':
   self.Res['right']+='OR '
  else:
   self.Res+='OR '
 @_generative
 def Between(self,low,high):
  self._repr+=f'.Between({low}, {high})' 
  if low and high:
   if self._operation=='Projection':
    self.Res['right']+=f'BETWEEN {low} and {high} '
   elif self._operation=='Selection':
    self.seldict.update({'operator':'Between','low':f'{low}','high':f'{high}'})
    self.Res+=f'BETWEEN {low} and {high} '
   else:
    self.Res+=f'BETWEEN {low} and {high} '
  else:
   raise OperationError(f'Operation failed, check ``Between`` parameters')
 @_generative
 def In(self,csv,csvtype='string'):
  self._repr+=f'.In({csv})' 
  lisval=csv.split(', ')
  if csvtype=='string':
   str2tuple='('+','.join(map("'{0}'".format,lisval))+')'
  elif csvtype=='numeric':
   str2tuple='('+','.join(map("{0}".format,lisval))+')'
  else:
   raise OperationError(f'Operation failed, check ``Between`` parameters')
  if self._operation=='Projection':
   self.Res['right']+=f'IN {str2tuple}'
  elif self._operation=='Selection':
   self.seldict.update({'operator':'In','csv':f'{csv}','csvtype':f'{csvtype}'})
   self.Res+=f'IN {str2tuple}'
  else:
   self.Res+=f'IN {str2tuple}'
 @_generative
 def Like(self,pattern):
  self._repr+=f'.Like({pattern})' 
  if self._operation=='Projection':
   self.Res['right']+=f"LIKE '"+pattern+"'"
  elif self._operation=='Selection':
   self.seldict.update({'operator':'Like','pattern':f'{pattern}'})
   self.Res+=f"LIKE '"+pattern+"'"
  else:
   self.Res+=f"LIKE '"+pattern+"'"
 @_generative
 def Order(self,expr):
  if '$k' in expr:
   expr=expr.replace('$k','[ha2, ha1]')
  if '$v' in expr:
   expr=expr.replace('$v','val')
  if '$c' in expr:
   expr=expr.replace('$c','cnt')
  if '$p' in expr:
   expr=expr.replace('$p','pos')
  if self._operation=='Projection':
   self.Res['end']+=f'\nORDER BY {expr}'
  else:
   self.Res+=f'\nORDER BY {expr}'
 @_generative
 def Limit(self,n,offset=0):
  if self._operation=='Projection':
   self.Res['end']+=f'\nLIMIT {n} OFFSET {offset}'
  else:
   self.Res+=f'\nLIMIT {n} OFFSET {offset}'
 @_generative
 def Display(self):
  print(self.Res)
 @_generative
 def Exe(self,columns=None,index=None,exe=True):
  hbsql,selsql,oversql='','',''
  if self._operation not in oplist:
   raise OperationError(f'Operation failed. Unknown operation {self._operation}')
  if self._operation=='Selection':
   hbsql=self.Res
   sel='\nSELECT hb2, hb1arr, cnt, ha2, ha1, pos, if (pos=1, 1, 0) as sel'
   frm=f'\nFROM HAtom_{self._dim3}States'
   whe=f'\nWHERE hb2={self._hacol.pentity.dim2} and ha2={self._dim2} and ha1 IN'
   subsel='\n(SELECT ha1'
   delimiter_ndx=hbsql.find('\n',hbsql.find('\n')+1)
   rest=hbsql[delimiter_ndx:]
   selsql=sel+frm+whe+subsel+rest+')'
  elif self._operation=='Projection':
   join_cl='\n ANY INNER JOIN \n('
   if self._hacol.is_junction:
    using_cl='\n) USING ha2, ha1, hb2'
   else:
    using_cl='\n) USING ha2, ha1'
   oversql+=self.Res['left']+join_cl+self.Res['right']+using_cl+self.Res['exc']+self.Res['end']
  if columns:
   self._dfcolumns=columns
  self._hacol._columns=self._dfcolumns
  self._hacol._index=index
  self._hacol._qid=self._operation
  if self._operation=='Selection':
   self._hacol._last_query=selsql
  elif self._operation=='Projection':
   self._hacol._last_query=oversql
  else:
   self._hacol._last_query=self.Res
  try:
   if self._operation=='Selection':
    if exe:
     self.Res=self.sql(hbsql,cols=self._dfcolumns,index=index,qid=self._operation,execute=exe)
    else:
     self.Res=(hbsql,selsql)
   elif self._operation=='Projection':
    if exe:
     self.Res=self.sql(oversql,cols=self._dfcolumns,index=index,qid=self._operation,execute=exe)
    else:
     self.Res=oversql
   else:
    self.Res=self.sql(self.Res,cols=self._dfcolumns,index=index,qid=self._operation,execute=exe)
  except Exception as e:
   raise ClickHouseException('\n'.join(str(e).split('\n')[0:3]))from None
 @_generative
 def Value(self):
  self.Res=self.Res.values[0][0]


