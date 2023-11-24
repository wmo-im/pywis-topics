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

import csv
import logging
from pathlib import Path

import click

from pywis_topics.topics import TopicHierarchy, validate_baseline
from pywis_topics.util import get_cli_common_options, get_userdir, setup_logger

LOGGER = logging.getLogger(__name__)


WIS2_TOPIC_HIERARCHY_LOOKUP = Path(get_userdir()) / 'wis2-topic-hierarchy'


class CentreId:
    def __init__(self, centre_id: str, tables: str = None):
        """
        Initializer

        :param centre_id: centre-id as defined by WTH
        :param tables: location of base directory for bundle

        :returns: `pywis_topics.centre_id.CentreId`
        """

        self.centre_id = centre_id
        try:
            self.tld, self.centre = self.centre_id.split('-', 1)
        except ValueError:
            msg = 'Invalid number of centre-id tokens'
            raise ValueError(msg)

        if tables is not None:
            self.tables_dir = Path(tables) / 'wis2-topic-hierarchy'
        else:
            self.tables_dir = WIS2_TOPIC_HIERARCHY_LOOKUP

    def validate(self) -> bool:
        """
        Validates a centre-id

        :returns: `bool` of whether topic hierarchy is valid
        """

        LOGGER.debug('Validating baseline conventions')
        if not validate_baseline(self.centre_id):
            return False

        LOGGER.debug('Validating TLD component')
        tld_valid = False
        tld_upper = self.tld.upper()

        tld_file = self.tables_dir / 'tlds-alpha-by-domain.txt'
        with tld_file.open() as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                if tld_upper == row[0]:
                    tld_valid = True
                    break

        if not tld_valid:
            LOGGER.warning('Invalid TLD')
            return False

        LOGGER.debug('Checking for uniqueness')
        topics = TopicHierarchy()
        if self.centre_id in topics.topics[3]:
            LOGGER.warning('centre-id is already allocated')
            return False

        return True


@click.group('centre-id')
def centre_id():
    """Centre id utilities"""
    pass


@click.command()
@click.pass_context
@get_cli_common_options
@click.argument('centre-id')
def validate(ctx, centre_id, logfile, verbosity):
    """Validate centre-id"""

    setup_logger(verbosity, logfile)

    try:
        cid = CentreId(centre_id)
    except ValueError as err:
        raise click.ClickException(err)

    if cid.validate():
        click.echo('Valid')
    else:
        click.echo('Invalid')


centre_id.add_command(validate)
