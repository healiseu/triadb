"""
TRIADB Modules Testing:
    write_json(), load_json() methods of triadb.utils.ETL
    loading and writing serialized data model (SDMs) in JSON format for Northwind Traders use case

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb.utils import ETL

dm = {'cname':'Northwind Traders ERD',
      'alias':'NORTHWIND',
      'descr':'Data model of 8 Entities that resemble relational tables in ER diagram',
      'data': [ {'cname':'Supplier',
                 'alias':'Sup',
                 'descr':'Suppliers of products for Northwind customers',
                 'fields': [ {'cname':'CompanyName',    'alias':'s_company',    'vtype':'String',   'descr':'company name'},
                             {'cname':'ContactName',    'alias':'s_contact',    'vtype':'String',   'descr':'contact name'},
                             {'cname':'ContactTitle',   'alias':'s_title',      'vtype':'String',   'descr':'contact title'},
                             {'cname':'Address',        'alias':'s_address',    'vtype':'String',   'descr':'supplier address'},
                             {'cname':'City',           'alias':'s_city',       'vtype':'String',   'descr':'supplier city'},
                             {'cname':'Region',         'alias':'s_region',     'vtype':'String',   'descr':'supplier region'},
                             {'cname':'PostalCode',     'alias':'s_postal',     'vtype':'String',   'descr':'supplier postal code'},
                             {'cname':'Country',        'alias':'s_country',    'vtype':'String',   'descr':'supplier country'},
                             {'cname':'Phone',          'alias':'s_phone',      'vtype':'String',   'descr':'supplier phone'},
                             {'cname':'Fax',            'alias':'s_fax',        'vtype':'String',   'descr':'supplier fax'},
                             {'cname':'HomePage',       'alias':'s_homepage',   'vtype':'String',   'descr':'supplier home page'}
                           ]
                },
                {'cname':'Category',
                 'alias':'Cat',
                 'descr':'Categories of products',
                 'fields': [ {'cname':'CategoryName',   'alias':'cat_name',         'vtype':'String',   'descr':'category name'},
                             {'cname':'Description',    'alias':'cat_description',  'vtype':'String',   'descr':'category description'}
                           ]
                },
                {'cname':'Product',
                 'alias':'Pro',
                 'descr':'Products of Supplier for Northwind Customers',
                 'fields': [
                             {'cname':'ProductName',      'alias':'p_name',         'vtype':'String',       'descr':'product name'},
                             {'cname':'QuantityPerUnit',  'alias':'p_quantity',     'vtype':'String',       'descr':'product packaging: quantity per unit'},
                             {'cname':'UnitPrice',        'alias':'p_price',        'vtype':'Float32',      'descr':'product unit price'},
                             {'cname':'UnitsInStock',     'alias':'p_stock',        'vtype':'UInt16',       'descr':'product units in stock'},
                             {'cname':'UnitsOnOrder',     'alias':'p_order',        'vtype':'UInt16',       'descr':'product units on order'},
                             {'cname':'ReorderLevel',     'alias':'p_reorder',      'vtype':'UInt16',       'descr':'product reorder level'},
                             {'cname':'Discontinued',     'alias':'p_discontinued', 'vtype':'UInt8',        'descr':'product discontinued'}
                           ]
                },
                {'cname':'OrderDetails',
                 'alias':'Odet',
                 'descr':'Items per Order of Northwind Customers',
                 'fields': [ {'cname':'UnitPrice',    'alias':'odet_price',        'vtype':'Float32',   'descr':'price of order item '},
                             {'cname':'Quantity',     'alias':'odet_quantity',     'vtype':'UInt16',    'descr':'how many items'},
                             {'cname':'Discount',     'alias':'odet_discount',     'vtype':'Float32',   'descr':'discount for ordered items'}
                           ]
                },
                {'cname':'Order',
                 'alias':'Ord',
                 'descr':'Orders of Northwind Customers',
                 'fields': [ {'cname':'OrderDate',        'alias':'o_date',     'vtype':'Date',         'descr':'date of order'},
                             {'cname':'RequiredDate',     'alias':'o_required', 'vtype':'Date',         'descr':'date required for order delivery'},
                             {'cname':'ShippedDate',      'alias':'o_shipped',  'vtype':'Date',         'descr':'date products were shipped'},
                             {'cname':'Freight',          'alias':'o_freight',  'vtype':'Float32',      'descr':'order freight'},
                             {'cname':'ShipName',         'alias':'o_name',     'vtype':'String',       'descr':'ship name'},
                             {'cname':'ShipAddress',      'alias':'o_address',  'vtype':'String',       'descr':'shipping address'},
                             {'cname':'ShipCity',         'alias':'o_city',     'vtype':'String',       'descr':'shipping city'},
                             {'cname':'ShipRegion',       'alias':'o_region',   'vtype':'String',       'descr':'shipping region'},
                             {'cname':'ShipPostalCode',   'alias':'o_postal',   'vtype':'String',       'descr':'shipping postal code'},
                             {'cname':'ShipCountry',      'alias':'o_country',  'vtype':'String',       'descr':'shipping country'}
                           ]
                },
                {'cname':'Employee',
                 'alias':'Emp',
                 'descr':'Employees of Northwind',
                 'fields': [ {'cname':'LastName',        'alias':'e_last',       'vtype':'String',       'descr':'employee last name'},
                             {'cname':'FirstName',       'alias':'e_first',      'vtype':'String',       'descr':'employee first name'},
                             {'cname':'Title',           'alias':'e_title',      'vtype':'String',       'descr':'employee title'},
                             {'cname':'TitleOfCourtesy', 'alias':'e_titlecourt', 'vtype':'String',       'descr':'employee title of courtesy'},
                             {'cname':'BirthDate',       'alias':'e_birth',      'vtype':'Date',         'descr':'employee birth date'},
                             {'cname':'HireDate',        'alias':'e_hire',       'vtype':'Date',         'descr':'employee hire date'},
                             {'cname':'Address',         'alias':'e_address',    'vtype':'String',       'descr':'employee address'},
                             {'cname':'City',            'alias':'e_city',       'vtype':'String',       'descr':'employee city'},
                             {'cname':'Region',          'alias':'e_region',     'vtype':'String',       'descr':'employee region'},
                             {'cname':'PostalCode',      'alias':'e_postal',     'vtype':'String',       'descr':'employee postal code'},
                             {'cname':'Country',         'alias':'e_country',    'vtype':'String',       'descr':'employee country'},
                             {'cname':'HomePhone',       'alias':'e_homephone',  'vtype':'String',       'descr':'employee home phone'},
                             {'cname':'Extension',       'alias':'e_extension',  'vtype':'String',       'descr':'employee extension'},
                             {'cname':'Notes',           'alias':'e_notes',      'vtype':'String',       'descr':'employee notes'},
                             {'cname':'ReportTo',        'alias':'e_reports',    'vtype':'UInt16',       'descr':'employee reports to'},
                             {'cname':'PhotoPath',       'alias':'e_photopath',  'vtype':'String',       'descr':'employee photo path'},
                             {'cname':'Salary',          'alias':'e_salary',     'vtype':'Float32',      'descr':'employee salary'}
                           ]
                },
                {'cname': 'Customer',
                 'alias': 'Cust',
                 'descr': 'Customers of Northwind Traders',
                 'fields': [ {'cname':'CompanyName',     'alias':'c_company',     'vtype':'String',   'descr':'customer company'},
                             {'cname':'ContactName',     'alias':'c_contact',     'vtype':'String',   'descr':'customer contact'},
                             {'cname':'ContactTitle',    'alias':'c_title',       'vtype':'String',   'descr':'customer contact title'},
                             {'cname':'Address',         'alias':'c_address',     'vtype':'String',   'descr':'customer address'},
                             {'cname':'City',            'alias':'c_city',        'vtype':'String',   'descr':'customer city'},
                             {'cname':'Region',          'alias':'c_region',      'vtype':'String',   'descr':'customer region'},
                             {'cname':'PostalCode',      'alias':'c_postal',      'vtype':'String',   'descr':'customer postal'},
                             {'cname':'Country',         'alias':'c_country',     'vtype':'String',   'descr':'customer country'},
                             {'cname':'Phone',           'alias':'c_phone',       'vtype':'String',   'descr':'customer phone'},
                             {'cname':'Fax',             'alias':'c_fax',         'vtype':'String',   'descr':'customer fax'}
                            ]
                 },
                {'cname': 'Shipper',
                 'alias': 'Ship',
                 'descr': 'Companies that transports products of Supplier for Northwind customers',
                 'fields': [ {'cname':'CompanyName',    'alias':'sh_company',   'vtype':'String',   'descr':'shipper company'},
                             {'cname':'Phone',          'alias':'sh_phone',     'vtype':'String',   'descr':'shipper phone'}
                            ]
                 },

                {'cname': 'Cross-References',
                 'alias': ['Sup', 'Pro'],
                 'fields': {'cname': 'SupplierID', 'alias': 's_id', 'vtype': 'UInt16', 'descr': 'supplier id'}
                 },
                {'cname': 'Cross-References',
                 'alias': ['Cat', 'Pro'],
                 'fields': {'cname': 'CategoryID', 'alias': 'cat_id', 'vtype': 'UInt16', 'descr': 'category id'}
                 },
                {'cname': 'Cross-References',
                 'alias': ['Pro', 'Odet'],
                 'fields': {'cname': 'ProductID', 'alias': 'p_id', 'vtype': 'UInt16', 'descr': 'product id'}
                 },
                {'cname': 'Cross-References',
                 'alias': ['Ord', 'Odet'],
                 'fields': {'cname': 'OrderID', 'alias': 'o_id', 'vtype': 'UInt16', 'descr': 'order id'}
                 },
                {'cname':'Cross-References',
                 'alias': ['Ord', 'Emp'],
                 'fields': {'cname':'EmployeeID', 'alias':'e_id',  'vtype':'UInt16', 'descr':'employee id'}
                },
                {'cname':'Cross-References',
                 'alias': ['Ord', 'Cust'],
                 'fields': {'cname':'CustomerID', 'alias':'c_id',  'vtype':'String', 'descr':'customer id'}
                },
                {'cname':'Cross-References',
                 'alias': ['Ord', 'Ship'],
                 'fields': {'cname':'ShipperID',  'alias':'sh_id', 'vtype':'UInt16', 'descr':'shipper id'}
                }

              ]
     }

# ETL.change_cwd('/dbstore/clickhouse/user_files/athan/DataModels')
# ETL.load_json('data.json')
# ETL.write_json(dm, 'Northwind.json')
