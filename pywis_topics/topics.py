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
from typing import List

import click

from pywis_topics.util import get_cli_common_options, get_userdir, setup_logger

LOGGER = logging.getLogger(__name__)


WIS2_TOPIC_HIERARCHY_LOOKUP = Path(get_userdir()) / 'wis2-topic-hierarchy'


class TopicHierarchy:
    def __init__(self):

        self.topics = []

        topic_levels = [
            'channel',
            'version',
            'system',
            'centre-id',
            'notification-type',
            'data-policy',
            'earth-system-discipline'
        ]

        for topic_level in topic_levels:
            filename = WIS2_TOPIC_HIERARCHY_LOOKUP / f'{topic_level}.csv'
            with filename.open() as fh:
                level_topics = []
                reader = csv.reader(fh)
                next(reader)
                for row in reader:
                    level_topics.append(row[0])

                self.topics.append(level_topics)

    def list_children(self, topic_hierarchy: str = None) -> List[str]:
        """
        Lists children at a given level of a topic hierarchy

        :param topic_hierarchy: `str` of topic hierarchy

        :returns: `list` of topic children
        """

        matches = []

        if topic_hierarchy == '/':
            LOGGER.debug('Dumping root topic children')
            return self.topics[0]

        if not self.validate(topic_hierarchy):
            msg = 'Invalid topic'
            LOGGER.info(msg)
            raise ValueError(msg)

        th_tokens = topic_hierarchy.split('/', 6)
        num_th_tokens = len(th_tokens)

        if num_th_tokens < 6:
            LOGGER.debug('Listing core topics')
            subtopics = set(self.topics[len(th_tokens)])
        elif num_th_tokens == 6:
            LOGGER.debug('Listing earth system discipline topics')
            subtopics = set([t.split('/')[0] for t in self.topics[6]])
        else:
            LOGGER.debug('Listing domain topics')
            subtopics_to_add = []
            domain_topic = th_tokens[-1]
            for subtopic in self.topics[-1]:
                if subtopic.startswith(domain_topic):
                    if subtopic != domain_topic:
                        mask = subtopic.replace(f'{domain_topic}/', '')
                        if mask:
                            subtopics_to_add.append(mask.split('/')[0])

            subtopics = set(subtopics_to_add)

        matches.extend(subtopics)

        if not matches:
            msg = f'No matching topics for {topic_hierarchy}'
            LOGGER.info(msg)
            raise ValueError(msg)

        return matches

    def validate(self, topic_hierarchy: str = None) -> bool:
        """
        Validates a topic hierarchy

        :param topic_hierarchy: `str` of topic hierarchy

        :returns: `bool` of whether topic hierarchy is valid
        """

        LOGGER.debug(f'Validating topic hierarchy {topic_hierarchy}')

        if topic_hierarchy == '/':
            msg = 'Topic hierarchy is empty'
            LOGGER.info(msg)
            raise ValueError(msg)

        th_tokens = topic_hierarchy.split('/', 6)

        for count, value in enumerate(th_tokens):
            if value not in self.topics[count]:
                return False

        return True


@click.group()
def topics():
    """Topic hierarchy utilities"""
    pass


@click.command('list')
@click.pass_context
@get_cli_common_options
@click.argument('topic-hierarchy')
def list_(ctx, topic_hierarchy, logfile, verbosity):
    """List topic hierarchies at a given level"""

    setup_logger(verbosity, logfile)

    th = TopicHierarchy()

    try:
        matching_topics = th.list_children(topic_hierarchy)
        click.echo('Matching topics')
        for matching_topic in matching_topics:
            click.echo(f'- {matching_topic}')
    except ValueError as err:
        raise click.ClickException(err)


@click.command()
@click.pass_context
@get_cli_common_options
@click.argument('topic-hierarchy')
def validate(ctx, topic_hierarchy, logfile, verbosity):
    """Validate topic hierarchy"""

    setup_logger(verbosity, logfile)

    th = TopicHierarchy()

    if th.validate(topic_hierarchy):
        click.echo('Valid')
    else:
        click.echo('Invalid')


topics.add_command(list_)
topics.add_command(validate)
