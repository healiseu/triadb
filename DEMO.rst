.. _demo-guide:

==================================
Demo Guide
==================================

----------------------------------
TRIADB v0.9
----------------------------------

:Author: Athanassios I. Hatzis
:date: Friday October 18 15:06:07 EEST 2019
:version: 1

In this guide we demonstrate how you can map, import, load and filter data on TRIADB. We use existing data models and data resources that we created during the testing of TRIADB modules. See the `Installation Guide` for more details.


.. contents:: Table of Contents
.. section-numbering::


Supplier - Catalog - Part
=========================

We start with a very simple many-to-many relationship data model that has two main entities ``Supplier`` and ``Part`` and an associative entity ``Catalog``.


SPC: Mapping - Importing - Loading
----------------------------------

    Get your favourite python editor and open the file ``SPC_MYSQL_mapping_loading.py``. You can review the whole procedure there which is straightforward and considerably simplified.

    * Connect to MariaDB and ClickHouse
        .. code-block:: python

            mis = MIS(debug=1, rebuild=True, what='data')

            mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                                     user='demo', password='demo', database='TRIADB', trace=0)

            mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                                     user='demo', password='demo', database='TriaDB', trace=0)


        Rebuilding ClickHouse data store is optional, set ``rebuild=False`` to maintain all data from different data models under ClickHouse TriaDB database. This is not rebuilding MariaDB meta-data store.

    * Select ``DataModel`` and ``DataResource``
        .. code-block:: python

            mis.get(what='datasets')
            mis.get(what='models')
            mis.restart(500, 242)
            print(mis)

        Use the right data resources for the SPC data model. Check the ``dim3`` column to verify that you pass sensible parameters to ``mis.restart()`` method. That is verified when you ``print(mis)``.
    * Map fields of ``DataResource`` onto attributes of the ``DataModel``
        .. code-block:: python

            mis.match_fields_with_attributes()
            mis.reset_mapping()
            mis.compare_fields_with_attributes(matching_pairs)
            mis.add_mapping()

        This needs a graphical environment but for simple cases it is manageable with a list of pairs. Use ``mis.compare_fields_with_attributes()`` method to verify that your bipartite mapping is correct. If something goes wrong, use ``reset_mapping()``.
    * Import data
        .. code-block:: python

            mis.import_data()

        Creates ClicHouse merge-tree table engines and imports data automatically from data resources. In this case MySQL tables.

    * Load data

        .. code-block:: python

            mis.load_data()

        Creates and loads TRIADB Associative Semiotic Hypergraph Engine with data from ClicHouse tables.


    After creating and loading TriaClick engine, you can fetch Associative Entity Sets (ASETs). Each one corresponds to an entity of the model.

    .. code-block:: python

        In: mis.get_asets()

        Out[13]:
        {(500, 1): ASET(500, 1)[SUP] = 4 hbonds,
         (500, 7): ASET(500, 7)[PRT] = 9 hbonds,
         (500, 12): ASET(500, 12)[CAT] = 17 hbonds}

SPC ASERD
---------

    You can also display in a Matplotlib graph the relationship between ASETs. In TRIADB this is called Associative Entity Relationship Diagram (ASERD)

    .. code-block:: python

        In: mis.engine.aserd.draw()

    .. figure:: images/spc_aserd.png
        :height: 300px
        :width: 500 px
        :alt: ASERD of Part (PRT) - Catalog (CAT) - Supplier (SUP)

        ASERD of Part (PRT) - Catalog (CAT) - Supplier (SUP)

    .. _BFS:

    For more details visit the file ``SPC_ASERD.py`` which also demonstrates `breadth-first-search` (BFS) that is used in filtering ASETs. For example if we start filtering with an attribute of `PRT` entity, it will propagate filtering through ``p_id`` attribute to `CAT` entity and then through ``s_id`` to `SUP` entity. That is the subject of the following demo exercise.

SPC Associative Filtering
-------------------------

    Open the file ``SPC_aset_opes.py``, establish connections to data/meta stores and restart `TriaClick` engine. Here you can pass an argument to reset the filtering mode.

    There are easy commands to get `Attributes`, `Entities` and `Associative Entity Sets` from `DataModelSystem` and then based on these you can define your data selections.


    .. code-block:: python

        In: mis.get_attributes(select='dim2, cname, alias', extras='fields, entities')

        Out:
            dim2    cname       alias                               fields                             entities
        0      2     name      s_name                   [FLD:(1, 242, 16)]                    [ENT:(2, 500, 1)]
        1      3  address   s_address                   [FLD:(1, 242, 17)]                    [ENT:(2, 500, 1)]
        2      4     city      s_city                   [FLD:(1, 242, 19)]                    [ENT:(2, 500, 1)]
        3      5  country   s_country                   [FLD:(1, 242, 18)]                    [ENT:(2, 500, 1)]
        4      6   status    s_status                   [FLD:(1, 242, 20)]                    [ENT:(2, 500, 1)]
        5      8     name      p_name                   [FLD:(1, 242, 11)]                    [ENT:(2, 500, 7)]
        6      9    color     p_color                   [FLD:(1, 242, 12)]                    [ENT:(2, 500, 7)]
        7     10   weight    p_weight                   [FLD:(1, 242, 13)]                    [ENT:(2, 500, 7)]
        8     11     uint      p_unit                   [FLD:(1, 242, 14)]                    [ENT:(2, 500, 7)]
        9     13    price     c_price                    [FLD:(1, 242, 6)]                   [ENT:(2, 500, 12)]
        10    14    total  c_quantity                    [FLD:(1, 242, 7)]                   [ENT:(2, 500, 12)]
        11    15     date      c_date                    [FLD:(1, 242, 8)]                   [ENT:(2, 500, 12)]
        12    16    check     c_check                    [FLD:(1, 242, 9)]                   [ENT:(2, 500, 12)]
        13    17       id        s_id  [FLD:(1, 242, 4), FLD:(1, 242, 15)]  [ENT:(2, 500, 1), ENT:(2, 500, 12)]
        14    18       id        p_id  [FLD:(1, 242, 5), FLD:(1, 242, 10)]  [ENT:(2, 500, 7), ENT:(2, 500, 12)]

    And if you want to use any of them in object form for your own development purposes:

    .. code-block:: python

        In: mis.get_attributes(out='objects', csvlist='s_id, p_id, p_color, c_price')

        Out: [ATTR:(2, 500, 13), ATTR:(2, 500, 9), ATTR:(2, 500, 18), ATTR:(2, 500, 17)]

    Now that you have an idea how to explore the `DataModelSystem` we can proceed with `selections`. Notice that this is not the kind of selections, i.e. projections, that you specify in SQL type of queries. A `selection` is simply a filter, think about SQL ``WHERE`` conditions, that you define on the domain values of an attribute.

    An Attribute domain set is defined, implemented and used explicitly in TRIADB and it is called `HyperAtom Collection` (HACOL). There are also other fundamental operations on `HACOL` sets implemented such as projection, counting, and aggregation methods. You can find many examples in the file ``SPC_hacol_ops.py``.

    .. _`SPC associative filtering example`:

    Selections in this demo example are written in *Chain Query Language* (CQL), a functional and OOP style to specify programmatically operations on `HACOL` sets. It is super easy and intuitive to learn and at the same time it saves your time from writing complex queries. For example:

    .. code-block:: python

        In: part_items_with_red_color = eng.set_hacol(alias='p_color').cql.Select().Where("$v='Red'")

    In many cases selection operations are simplified further (see `Northwind Associative Filtering`_).

    `TriaClick` engine has two filtering modes:

    * Progressive Filtering Mode is desirable when the user interactively selects values in a dashboard, BI application, and wants to receive an immediate visual feedback on the effect filtering has on all graphical representations. This type of filtering is ideal for exploratory analytics. For example, filter `Part` items that have a `Red` color:

        .. code-block:: python

           In: eng.filter_selections(part_items_with_red_color)

            ┃▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔ STARTED ▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔┃
            ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
            ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
            Filtering of ASET((500, 7))[PRT] is completed:
            Elapsed: 1.156 sec
            ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
            ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
            Filtering of ASET((500, 12))[CAT] is completed:
            Elapsed: 0.246 sec
            ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
            ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
            Filtering of ASET((500, 1))[SUP] is completed:
            Elapsed: 0.341 sec
            ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
            ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
            Filtering is completed:
            Total Elapsed Time: 2.202 sec
            ⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗  FINISHED FILTERING ⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗⫗

           Out[5]: [2.202]

        Normally you get back only the elapsed time but TRIADB has ``debug`` levels to display a more detailed output such as the one above and ``trace`` levels to display SQL queries executed in the background. That specific output of the filtering process demonstrates the `BFS` algorithm that is embedded in `ASERD` (see BFS_).

    * Multiple Filtering Mode is similar to SQL WHERE clause with multiple, complex conditions. This is useful when we know exactly what to expect as a result. In that case we don't need to apply filters one by one in a successive order. This is done by specifying the ``mode`` parameter. Notice also that in the current version of TRIADB, associative filtering in `multiple mode` works only with selections based on the same ASET (that is why you get warnings). Try the following instead:

    .. code-block:: python

        eng.restart()
        eng.filter_selections([
            eng.set_hacol(alias='c_price').cql.Select().Where('$v<20'),
            eng.set_hacol(alias='c_quantity').cql.Select().Where('$v=200'),
            eng.set_hacol(alias='c_check').cql.Select().Where('$v=0')
            ], mode='multiple')

    Finally we can inspect the effect of filtering on HACOLs of any ASET

    .. code-block:: python

        mis.dms.switch(500, 12)
        for alias in [attr.alias for attr in mis.dms.get_attributes(out='objects')]:
            print(mis.get_items(alias=alias, highlight=False, excluded=False), '\n')

           c_price  FREQ  S  P
        0    15.30     1  1  1
        1     7.95     1  1  1

           c_quantity  FREQ  S  P
        0         200     2  1  1

               c_date  FREQ  S  P
        0  2014-03-03     2  0  1

           c_check  FREQ  S  P
        0        0     2  1  1

           s_id  FREQ  S  P
        0  1082     1  0  1
        1  1081     1  0  1

           p_id  FREQ  S  P
        0   998     1  0  1
        1   993     1  0  1

    We asked TRIADB-TriaClick engine to return only those values in every HACOL of Catalog ASET that are correlated, i.e. those that exist in `Possible` (P) state. Those items selected have also active the `Selected` (S) state. For all items their frequency is also displayed. This number reveals the number of associations, think about relational tuples (rows) where the element is present.

    That can be seen clearly in the following representation of associations in the form of tuples

    .. code-block:: python

        In: mis.get_tuples(13, 14, 15, 16, 17, 18, aset_dim2=12, hb2=True, hb1=True)

        Out:
           c_price  c_quantity      c_date  c_check  s_id  p_id  hb2  hb1
        0    15.30         200  2014-03-03        0  1081   993   12    2
        1     7.95         200  2014-03-03        0  1082   998   12   11


Northwind Traders
=================

This second demo uses the classic example of Northwind Traders relational database.

    .. figure:: images/northwind_msaccess_schema.png
        :height: 600px
        :width: 800 px
        :alt: Northwind Entity-Relationship Schema

        Northwind Entity-Relationship Schema

The data model is already built during the rebuild-population procudure (see Installation Guide). Run this demo on Jupyter Lab.

    .. code-block:: bash

        cd test_cases/Northwind/Notebooks/
        jupyter lab

If this is the first time you run Jupyter lab it will ask you to build ``plotly-extension``. This step is required to view the embedded `dash` barcharts.


Northwind: Mapping - Importing - Loading
-----------------------------------------

From `JupyerLab` environment open the file ``Northwind Mapping Importing Loading.ipynb``. Start executing the cells of the same mapping-importing-loading procedure that we have already analysed in the previous example. Only this time when you run for example the command:

    .. code-block:: bash

        mis.engine.compare_fields_with_attributes([
             (260, 84),
             (261, 28),
             (262, 82),
             (263, 83),
             (264, 29),
             (265, 30),
             (266, 31),
             (267, 32),
             (268, 33),
             (269, 34),], graph=True)

    You will get a bipartite graph to visualize how fields are mapped onto attributes.

    .. figure:: images/bipartite_mapping_product_fields_attributes.png
        :height: 400px
        :width: 300 px
        :alt: Mapping fields from a data resource onto attributes of a data model

        Mapping fields from a data resource onto attributes of a data model



Northwind ASERD
---------------
When you finish loading data on TriaClick engine from the imported data resources you can fetch `ASETs`. Each one corresponds to an entity of the model. Notice that this is not necessary the case we could have created a different data model with less ASETs, this is done here for comparison purposes.

    .. code-block:: python

        In: mis.get_asets()

        {   (100, 1): ASET(100, 1)[Sup] = 29 hbonds,
            (100, 13): ASET(100, 13)[Cat] = 8 hbonds,
            (100, 16): ASET(100, 16)[Pro] = 77 hbonds,
            (100, 24): ASET(100, 24)[Odet] = 2155 hbonds,
            (100, 28): ASET(100, 28)[Ord] = 830 hbonds,
            (100, 39): ASET(100, 39)[Emp] = 9 hbonds,
            (100, 57): ASET(100, 57)[Cust] = 91 hbonds,
            (100, 68): ASET(100, 68)[Ship] = 3 hbonds
        }

    and here is the ASERD of Northwind use case

    .. code-block:: python

        eng.aserd.draw(graph_width=14, graph_height=10)


    .. figure:: images/northwind_aserd.png
        :height: 600px
        :width: 800 px
        :alt: Northwind Associative Entity Relationship Diagram (ASERD)

        Northwind Associative Entity Relationship Diagram (ASERD)

You may visit ``Northwind_ASERD.py`` file for more details.


Northwind Associative Filtering
-------------------------------
Now you can open ``Northwind Scenario 1.ipynb`` notebook to continue testing associative filtering. Here we can try higher level ``select()`` method, which simplifies `SPC associative filtering example`_:

    .. code-block:: python

        In: mis.select("$v='Sales Representative'", alias='e_title')

        Out:
        ┃▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔ STARTED ▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔┃
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering of ASET((100, 39))[Emp] is completed:
        Elapsed: 1.679 sec
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering of ASET((100, 28))[Ord] is completed:
        Elapsed: 0.656 sec
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering of ASET((100, 24))[Odet] is completed:
        Elapsed: 0.467 sec
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering of ASET((100, 68))[Ship] is completed:
        Elapsed: 0.435 sec
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering of ASET((100, 57))[Cust] is completed:
        Elapsed: 0.476 sec
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering of ASET((100, 16))[Pro] is completed:
        Elapsed: 0.48 sec
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering of ASET((100, 1))[Sup] is completed:
        Elapsed: 0.63 sec
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering of ASET((100, 13))[Cat] is completed:
        Elapsed: 1.227 sec
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        Filtering is completed:
        Total Elapsed Time: 7.47 sec

With a single command we achieve both selection and filtering on the background. The filtering order of ASETs is following BFS search on the ASERD.

    .. code-block:: python


        In: for head, tail, edge in eng.aserd.get_bfs_edges(39):
                print(f'From: {mis.set_dms(head[1]).alias} ---{eng.set_hacol(edge[1]).alias}---> '
                      f'To: {mis.set_dms(tail[1]).alias}')

        Out:
        From: Emp ---e_id---> To: Ord
        From: Ord ---o_id---> To: Odet
        From: Ord ---sh_id---> To: Ship
        From: Ord ---c_id---> To: Cust
        From: Odet ---p_id---> To: Pro
        From: Pro ---s_id---> To: Sup
        From: Pro ---cat_id---> To: Cat

Progressive filtering continues by selecting category of products and shipped year.

    .. code-block:: python

        In: mis.select("$v='Dairy Products'", alias='cat_name')
        In: mis.select('toYear($v)=1996', dim2=31)

Items of these hypercollections with their values and states can be rendered on Jupyter notebook with a graphical layout.

    .. code-block:: python

        df1 = mis.get_items(dim2=14, caption='Product Category')
        df2 = mis.get_items(dim2=40, caption='Employee Last Name')
        df3 = mis.get_items(dim2=42, caption='Employee Title')
        df4 = mis.get_items(dim2=65, limit=None, caption='Customer Country')

        display_dataframes(df1, df2, df3, df4)


    .. figure:: images/get_items_anim.gif
        :alt: Northwind Associative Entity Relationship Diagram (ASERD)

        Animated sequence of associative filtering on HACOL items

Now we can complete our first use case scenario in analytics which can be expressed in natural language as:

* **Fetch top ten subptotal by order for dairy products in the year 1996 for sales representatives**

    .. code-block:: python

        In: subtotals_by_order = mis.get_tuples(74, 73, 25, 26, 27, aset_dim2=24,
                                   projection='concat(\':\', toString(any(o_id))) AS order_id, '
                                              'round(sum(odet_price*odet_quantity*(1-odet_discount)), 1) AS subtotal, '
                                              'count(p_id) AS total_items',
                                   group_by='o_id', order_by='subtotal DESC', limit=10,
                                   pandas_columns='OrderID, SubTotal, TotalItems')
            subtotals_by_order

        Out:

                OrderID	    SubTotal    TotalItems
        0 	:11017 	    6050.0 	    1
        1 	:11030 	    4125.0 	    1
        2 	:10895 	    3400.0 	    1
        3 	:11012 	    2388.3 	    2
        4 	:10847 	    2170.0 	    2
        5 	:10941 	    2152.5 	    2
        6 	:10892 	    2090.0 	    1
        7 	:10836 	    2040.0 	    1
        8 	:10938 	    1813.9 	    2
        9 	:10894 	    1710.0 	    1


And by using ``plotly`` library we can render the result and embed a beautiful barchart in the notebook

    .. code-block:: python

        fig1 = px.bar(subtotals_by_order, x="OrderID", y="SubTotal", title='SubTotals by Order for Dairy Products')
        fig1.show()

    .. figure:: images/subtotal_by_order_dairy_products.png
        :height: 400px
        :width: 800 px
        :alt: Northwind Top Ten SubTotals by Order for Dairy Products

        Northwind Top Ten SubTotals by Order for Dairy Products


Northwind Dashboard
-------------------
In the last part of our demo you will see an example of a dashboard web application built with TriaClick and `Dash <https://dash.plot.ly/>`__ . It uses associative selections to interact with the various graphical elements. Open a shell and enter the following commands

    .. code-block:: bash

        cd test_cases/Northwind/Apps/
        python Northwind_Scenario1_dashboard.py pico

Hopefully that will start Flask web server to run our web application on http://127.0.0.1:8999/. Now open the link in a web browser. If we are already in a filtered state, it is automatically recognized and the web browser renders accordingly all the graphical elements.

    .. figure:: images/northwind_dashboard_application.png
        :height: 600px
        :width: 1024 px
        :alt: Northwind Top Ten SubTotals by Order for Dairy Products

        Northwind Top Ten SubTotals by Order for Dairy Products

There is a `RESET FILTERING` button, press it to restart TriaClick engine in unfiltered state.
    * From `Product Category` select Grains/Cereals
        That category is highlighted in green to indicate that it has been selected. You can also verify that with it's `Select State` that is 1. Upon selection, the other categories take up the grey color to indicate that are excluded, i.e. their `Possible State` is 0. This is also the state of two countries `Poland` and `Norway` in the `Customer Country` attribute list. This piece of visual information gives the end user a basic insight, Northwind company doesn't have any sales of Grains/Cereals in Poland and Norway.
    * From `Employee Title` select `Sales Representative`
        Notice that this selection filtered out those employees that do not belong in that category and the graph was updated accordingly with the new values of `Subtotals by Product` table.
    * Click on the Shipped Year drop down list and select `1994`
        In the year 1994 eight countries only had customers that bought Grains/Cereals from Northwind. The barchart of the dashboard shows that the top selling product with ID=57 was `Ravioli Angelo` but it is in the fourth place in the list of our products ordered by `Stock`.

That concludes our demo guide, there is also a screen capture demo of the Northwind dashboard that you can watch `here <https://www.youtube.com/watch?v=QSk1ldfb7ow>`_





