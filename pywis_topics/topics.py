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
import re
from typing import List

import click

from pywis_topics.util import get_cli_common_options, get_userdir, setup_logger

LOGGER = logging.getLogger(__name__)


WIS2_TOPIC_HIERARCHY_LOOKUP = Path(get_userdir()) / 'wis2-topic-hierarchy'


class TopicHierarchy:
    def __init__(self, tables: str = None):
        """
        Initializer

        :param tables: location of base directory for bundle

        :returns: `pywis_topics.topics_TopicHierarchy`
        """

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

        if tables is not None:
            tables_dir = Path(tables) / 'wis2-topic-hierarchy'
        else:
            tables_dir = WIS2_TOPIC_HIERARCHY_LOOKUP

        for topic_level in topic_levels:
            filename = tables_dir / f'{topic_level}.csv'
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

    def validate(self, topic_hierarchy: str = None,
                 strict: bool = True) -> bool:
        """
        Validates a topic hierarchy

        :param topic_hierarchy: `str` of topic hierarchy
        :param strict: `bool` of whether to perform strict validation,
                       including centre-id

        :returns: `bool` of whether topic hierarchy is valid
        """

        LOGGER.debug(f'Validating topic hierarchy {topic_hierarchy}')

        if topic_hierarchy in ['/', None]:
            msg = 'Topic hierarchy is empty'
            LOGGER.warning(msg)
            raise ValueError(msg)

        validate_baseline(topic_hierarchy)

        all_tokens = topic_hierarchy.split('/')
        core_tokens = all_tokens[:6]
        esd_subtopic = '/'.join(all_tokens[6:])

        LOGGER.debug(f'Core tokens: {core_tokens}')
        LOGGER.debug(f'Earth system discipline subtopic: {esd_subtopic}')
        LOGGER.debug('Validating core tokens')

        if not self._validate_core(core_tokens, strict):
            LOGGER.debug('Core tokens are invalid')
            return False

        if esd_subtopic:
            LOGGER.debug('Validating Earth system discipline subtopic')
            if not self._validate_esd_subtopic(esd_subtopic, strict):
                LOGGER.debug('Earth system discipline subtopic is invalid')
                return False
        else:
            LOGGER.debug('No Earth system discipline subtopic')

        return True

    def _validate_core(self, core_tokens: list, strict: bool = True) -> bool:
        """
        Validates core topic tokens

        :param core_tokens: `list` of core tokens
        :param strict: `bool` of whether to perform strict validation,
                       including centre-id

        :returns: `bool` of whether topic hierarchy is valid
        """

        for count, value in enumerate(core_tokens):
            if value in [None, '']:
                continue
            if not strict and count == 3:
                LOGGER.debug('Skipping centre-id validation')
                continue
            elif value in ['+', '#']:
                if not strict:
                    LOGGER.debug('Skipping wildcard')
                    continue
                else:
                    return False

            if value not in self.topics[count]:
                return False

        return True

    def _validate_esd_subtopic(self, esd_subtopic: str,
                               strict: bool = True) -> bool:
        """
        Validates Earth system discipline subtopic

        :param esd_subtopic: `str` of Earth system discipline subtopic
        :param strict: `bool` of whether to perform strict validation

        :returns: `bool` of whether topic hierarchy is valid
        """

        is_valid = False

        if strict:
            LOGGER.debug('Validating subtopic with strict mode')
            is_experimental = esd_subtopic.split('/')[1] == 'experimental'
            return esd_subtopic in self.topics[-1] or is_experimental

        tokens = esd_subtopic.split('/')
        if len(tokens) > 1 and tokens[1] == 'experimental':
            LOGGER.debug('Experimental topic found, skipping')
            return True

        regex = esd_subtopic
        if '+' in regex:
            regex = regex.replace('+', r'(\w+|\+)')
        if '#' in regex:
            regex = regex.replace('#', r'.*')

        regex = f'^{regex}$'

        LOGGER.debug(regex)

        for esd in self.topics[-1]:
            LOGGER.debug(f'Testing {esd} against {regex}')
            match = re.search(regex, esd)
            if not match:
                LOGGER.debug('No match')
            else:
                LOGGER.debug('Match')
                last_token_match = False
                if not esd.endswith('#'):
                    esd_last_token = esd.split('/')[-1]
                    regex_last_token = (regex.split('/')[-1].replace('$', '')
                                        .replace('.*', '')
                                        .replace('^', ''))
                    if regex_last_token == '':
                        last_token_match = True
                    elif esd_last_token == regex_last_token:
                        last_token_match = True

                if last_token_match:
                    is_valid = True

        return is_valid


def validate_baseline(topic_hierarchy: str = None) -> bool:
    """
    Validates a topic hierarchy baseline conventions

    :param topic_hierarchy: `str` of topic hierarchy

    :returns: `bool` of whether topic hierarchy is valid to the baseline
              conventions
    """

    if '.' in topic_hierarchy:
        msg = 'Topic cannot contain dots'
        LOGGER.warning(msg)
        return False

    if not topic_hierarchy.islower():
        msg = 'Topic must be lowercase'
        LOGGER.warning(msg)
        return False

    if not topic_hierarchy.isascii():
        msg = 'Topic must be IRA T.50'
        LOGGER.warning(msg)
        return False

    if '#' in topic_hierarchy:
        pos = topic_hierarchy.find('#')
        if pos != len(topic_hierarchy) - 1:
            LOGGER.warning('Multi-level wildcard can only be last')
            return False

    return True


@click.group()
def topic():
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
@click.option('--strict/--no-strict', default=True,
              help='Validate in strict mode')
@click.argument('topic-hierarchy')
def validate(ctx, topic_hierarchy, logfile, verbosity, strict=True):
    """Validate topic hierarchy"""

    setup_logger(verbosity, logfile)

    th = TopicHierarchy()

    if th.validate(topic_hierarchy, strict=strict):
        click.echo('Valid')
    else:
        click.echo('Invalid')


topic.add_command(list_)
topic.add_command(validate)
