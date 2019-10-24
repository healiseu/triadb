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

__version__ = '0.9'
__version_update__ = '2019-10-12'
__release_version__ = '0.9'
__release_date__ = ''
__source_url__ = ''
__python_version__ = '>=3.7.0'
__author__ = 'Athanassios I. Hatzis'
__author_email = 'hatzis@healis.eu'
__organization__ = "HEALIS (Healthy Information Systems/Services)"
__organization_url = "http://healis.eu"
__copyright__ = "Copyright (c) 2015-2019 Athanassios I. Hatzis"
__license__ = "GNU AGPL v3.0 and TriaClick Open Source License Agreement"
__distributor__ = 'Promoted and Distributed by HEALIS'
__distributor_url__ = 'http://healis.eu'
__maintainer__ = 'Athanassios I. Hatzis'
__maintainer_email__ = 'hatzis@healis.eu'
__status__ = 'Beta'

from .mis import MIS
from .utils import ETL, display_dataframes
from .clients import ConnectionPool
from .connectors import MetaManagementConnector, DataManagementConnector
from .subsystems import DataModelSystem, DataResourceSystem
from .meta_models import Node, DataModel, Entity, Attribute