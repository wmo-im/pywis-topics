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

import io
import logging
import os
import shutil
import zipfile

import click

from pywis_topics.util import (get_cli_common_options, get_userdir, urlopen_,
                               setup_logger)

LOGGER = logging.getLogger(__name__)

USERDIR = get_userdir()

WIS2_TOPIC_HIERARCHY_DIR = get_userdir() / 'wis2-topic-hierarchy'


@click.group()
def bundle():
    """Configuration bundle management"""
    pass


def sync_bundle() -> None:
    """
    Sync bundle locally to ~/.pywis-topics

    :returns: `None`
    """

    LOGGER.debug('Caching topic hierarchy')

    if USERDIR.exists():
        shutil.rmtree(USERDIR)

    LOGGER.debug('Downloading WIS2 topic hierarchy')
    WIS2_TOPIC_HIERARCHY_DIR.mkdir(parents=True, exist_ok=True)

    ZIPFILE_URL = 'https://wmo-im.github.io/wis2-topic-hierarchy/wth-bundle.zip'  # noqa
    FH = io.BytesIO(urlopen_(ZIPFILE_URL).read())
    with zipfile.ZipFile(FH) as z:
        LOGGER.debug(f'Processing zipfile "{z.filename}"')
        for name in z.namelist():
            LOGGER.debug(f'Processing entry "{name}"')
            filename = os.path.basename(name)

            dest_file = WIS2_TOPIC_HIERARCHY_DIR / filename
            LOGGER.debug(f'Creating "{dest_file}"')
            with z.open(name) as src, dest_file.open('wb') as dest:
                shutil.copyfileobj(src, dest)

    LOGGER.debug('Downloading IANA TLDs')
    IANA_URL = 'https://data.iana.org/TLD/tlds-alpha-by-domain.txt'
    iana_file = WIS2_TOPIC_HIERARCHY_DIR / 'tlds-alpha-by-domain.txt'
    with iana_file.open('wb') as fh:
        fh.write(urlopen_(f'{IANA_URL}').read())


@click.command()
@get_cli_common_options
@click.pass_context
def sync(ctx, logfile, verbosity):
    "Sync configuration bundle"""

    setup_logger(verbosity, logfile)
    sync_bundle()


bundle.add_command(sync)
