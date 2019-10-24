.. installation_guide:

==================================
Installation Guide
==================================

----------------------------------
TRIADB v0.9
----------------------------------

:Author: Athanassios I. Hatzis
:date: Friday October 18 18:25:34 EEST 2019
:version: 1

Follow step by step instructions on how to install and test TRIADB release. Feel free to skip any of the following steps depending on your existing hardware/software infrastructure, but make sure you read setup instructions for `ClickHouse`_ and `MariaDB`_ in order to prepare these DBMS to run TRIADB. Please also read the License agreement carefully before completing the installation process and using TRIADB - TriaClick software.

.. contents:: Table of Contents
.. section-numbering::


OS Environment
==============
Currently **TriaClick, the Associative Semiotic Hypergraph Engine of TRIADB,** is based on ClickHouse DBMS and therefore it can run on any Linux, FreeBSD or Mac OS X with x86_64 CPU architecture. This installation guide has been tested on Linux Mint (19.1, 19.2) distribution that is based on Ubuntu 18.04 (bionic).

Python Environment
==================

TRIADB is written in Python3. You can run scripts on a shell, you can create Web Browser applications or you may try a more interactive computing style in a Jupyter Notebook. For this reason we recommend you run your tests with the latest Anaconda Python 3.7 distribution. It has also become a standard practise to create a virtual environment and test Python packages there.

Anaconda Environment Setup
--------------------------

.. code-block:: bash

    conda create --name triadb python=3.7
    conda activate triadb
    python -m site

You can add command ``conda activate TRIADB`` at the end of .bashrc file

Install TRIADB package
-----------------------------

.. code-block:: bash

    pip install triadb
    cd ~/anaconda3/envs/triadb/lib/python3.7/site-packages/triadb
    conda config --add channels conda-forge
    conda config --set channel_priority strict

Install dependencies
--------------------

.. code-block:: bash

    conda install --file requirements_conda.txt
    pip install -r requirements_pip.txt

Test TRIADB Package
-------------------

.. code-block:: bash

    python -c 'from triadb import MIS'

if the command above doesn't return any error, TRIADB package and its dependencies have been succesffully installed. Now it is time to install/setup database management systems.

ClickHouse
==========

Installation
------------

Install the latest release, TRIADB has been tested successfully on 19.15.3

.. code-block:: bash

    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4
    echo "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
    sudo apt update
    sudo apt install clickhouse-server clickhouse-client


Setup
-----

(i) Add ``vagrant`` user to ``clickhouse`` group and change permissions for group users

    .. code-block:: bash

        sudo usermod -a -G clickhouse vagrant
        sudo chown -R clickhouse:clickhouse /etc/clickhouse-*
        sudo chmod -R g+w /etc/clickhouse-*
        sudo chmod -R g+rwx /var/lib/clickhouse

(ii) Replace ClickHouse configuration files with TRIADB configuration files

    .. code-block:: bash

        sudo -u clickhouse mv /etc/clickhouse-server/config.xml /etc/clickhouse-server/config.backup.xml
        sudo -u clickhouse mv /etc/clickhouse-server/users.xml /etc/clickhouse-server/users.backup.xml

        cd ~/anaconda3/envs/triadb/lib/python3.7/site-packages/triadb/data
        sudo -u clickhouse cp XMLConfig/*.xml /etc/clickhouse-server/

(iii) copy TRIADB data files under ClickHouse user_files

    .. code-block:: bash

        cd ~/anaconda3/envs/triadb/lib/python3.7/site-packages/triadb/data
        sudo -u clickhouse mkdir /var/lib/clickhouse/user_files/demo
        sudo -u clickhouse cp -r DataModels /var/lib/clickhouse/user_files/demo
        sudo -u clickhouse cp -r FlatFiles /var/lib/clickhouse/user_files/demo

(iv) Performance tips

    .. code-block:: bash

        echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
        echo 0 | sudo tee /proc/sys/vm/overcommit_memory
        echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

(v) Start clickhouse-server and test it with clichouse-client

    .. code-block:: bash

        sudo service clickhouse-server start
        sudo service clickhouse-server status
        clickhouse-client -u demo --password demo --query="SHOW DATABASES"
        clickhouse-client -u demo --password demo --query="SELECT name, value FROM system.settings WHERE changed"
        clickhouse-client -u demo --password demo -d

   
MariaDB
=======
We tested TRIADB on the latest stable release of MariaDB which, at the time of writing this guide, is 10.4.8. Installing an older version is not recommended becaused you might get errors when you try to rebuild the database tables of TRIADB metadata management system.

Installation
------------

.. code-block:: bash

    sudo apt-get install software-properties-common
    sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xF1656F24C74CD1D8
    sudo add-apt-repository "deb http://mirror.host.ag/mariadb/repo/10.4/ubuntu bionic main"
    sudo apt update
    sudo apt install mariadb-server
    mysql -V

Setup
-----

(i) Create databases

    TRIADB stores metadata and the other two (SPC, Northwind) are used for importing/loading data to ClickHouse

    .. code-block:: bash

        sudo mysql

    Enter the following SQL commands:

    .. code-block:: sql

        GRANT ALL PRIVILEGES ON *.* TO 'demo'@'localhost' IDENTIFIED BY 'demo';
        CREATE DATABASE SPC;
        CREATE DATABASE Northwind;
        CREATE DATABASE TRIADB;
        USE TRIADB;
        CREATE TABLE Nodes (id int DEFAULT NULL);
        CREATE TABLE Edges (id int DEFAULT NULL);


(ii) Restore SPC, Northwind MySQL databases from SQL dump files

    .. code-block:: bash

        cd ~/anaconda3/envs/triadb/lib/python3.7/site-packages/triadb/data/MySQLDumps
        mysql -u demo -pdemo SPC < dump-SPC-201909050814.sql
        mysql -u demo -pdemo Northwind < dump-Northwind8Entities-201909061113.sql


Testing TRIADB Modules
======================

The final step in this installation guide is to create metadata database (MySQL-TRIADB) schema and populate TRIADB with ``DataModels`` and ``DataResources``. First check that you can connect to MariaDB and ClickHouse DBMS. Execute the following scripts from the ``test_modules`` folder of TRIADB package

(i) Check DBMS Connections

    .. code-block:: bash

        conda activate TRIADB
        cd ~/anaconda3/envs/triadb/lib/python3.7/site-packages/triadb/test_modules
        python ConnectionPool.py

(ii) Rebuild TRIADB and Populate

    .. code-block:: bash

        cd ~/anaconda3/envs/triadb/lib/python3.7/site-packages/triadb/test_modules
        python MMC_rebuild.py
        python MIS_populate_systems.py

    When the second script is finished you should get a printed output at the shell console of TRIADB Meta-Management System.

Congratulations you have completed successfully the installation guide. You may continue testing the modules of TRIADB by running other scripts in the same folder. For example study how simple and powerful is to fetch metadata from TRIADB. Open and run the scripts, ``MMC_get.py``, ``MIS_DMS_get.py`` and  ``MIS_DRS_get.py`` line by line.

Alternatively you may now try to run demo applications based on simple use cases. Read the `demo guide <https://github.com/healiseu/triadb/blob/master/DEMO.rst>`_.

