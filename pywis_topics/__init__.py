###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

__version__ = '0.3.3'

import click

from pywis_topics.bundle import bundle
from pywis_topics.centre_id import centre_id
from pywis_topics.topics import topic


@click.group()
@click.version_option(version=__version__)
def cli():
    """WIS2 Topic Hierarchy utility"""

    pass


cli.add_command(bundle)
cli.add_command(centre_id)
cli.add_command(topic)
