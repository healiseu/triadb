"""
TRIADB Modules Testing:
    write_json(), load_json() methods of triadb.utils.ETL
    loading and writing serialized data model (SDMs) in JSON format for Supplier-Part-Catalog (SPC) use case

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb.utils import ETL

dm = {'cname':'Supplier Part Catalogue',
      'alias':'SPC',
      'descr':'Model with three entities that represent relations (tables) in a relational database',
      'data': [ {'cname':'Supplier',
                 'alias':'SUP',
                 'descr':'The Supplier Entity of the data model',
                 'fields': [ {'cname':'name',     'alias':'s_name',     'vtype':'String',   'descr':'supplier name'},
                             {'cname':'address',  'alias':'s_address',  'vtype':'String',   'descr':'supplier address'},
                             {'cname':'city',     'alias':'s_city',     'vtype':'String',   'descr':'supplier city'},
                             {'cname':'country',  'alias':'s_country',  'vtype':'String',   'descr':'supplier country'},
                             {'cname':'status',   'alias':'s_status',   'vtype':'UInt8',    'descr':'supplier status'}
                           ]
                },
                {'cname':'Part',
                 'alias':'PRT',
                 'descr':'The Part Entity of the data model',
                 'fields': [ {'cname':'name',    'alias':'p_name',   'vtype':'String',   'descr':'part name'},
                             {'cname':'color',   'alias':'p_color',  'vtype':'String',   'descr':'part color'},
                             {'cname':'weight',  'alias':'p_weight', 'vtype':'Float32',  'descr':'part weight'},
                             {'cname':'uint',    'alias':'p_unit',   'vtype':'String',   'descr':'part unit'}
                           ]
                },
                {'cname':'Catalog',
                 'alias':'CAT',
                 'descr':'The Catalog Entity of the data model',
                 'fields': [ {'cname':'price',   'alias':'c_price',    'vtype':'Float32',  'descr':'catalog price'},
                             {'cname':'total',   'alias':'c_quantity', 'vtype':'UInt16', 'descr':'catalog quantity'},
                             {'cname':'date',    'alias':'c_date',     'vtype':'Date',   'descr':'catalog date'},
                             {'cname':'check',   'alias':'c_check',    'vtype':'UInt8',  'descr':'catalog check'}
                           ]
                },
                {'cname':'Cross-References',
                 'alias': ['SUP', 'CAT'],
                 'fields': {'cname':'id',  'alias':'s_id',  'vtype':'UInt16', 'descr':'supplier id'}
                },
                {'cname':'Cross-References',
                 'alias': ['PRT', 'CAT'],
                 'fields': {'cname':'id',  'alias':'p_id',  'vtype':'UInt16', 'descr':'part id'}
                }
              ]
     }

# ETL.change_cwd('/dbstore/clickhouse/user_files/athan/DataModels')
# ETL.load_json('data.json')
# ETL.write_json(dm, 'data.json')
