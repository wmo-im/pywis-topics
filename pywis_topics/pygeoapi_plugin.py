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

# pywis-topics as a service
# -------------------------
#
# This file is intended to be used as a pygeoapi process plugin which will
# provide pywis-topics functionality via OGC API - Processes.
#
# To integrate this plugin in pygeoapi:
#
# 1. ensure pywis-topics is installed into the pygeoapi deployment environment
#
# 2. add the processes to the pygeoapi configuration as follows:
#
# pywis-topics-list:
#     type: process
#     processor:
#         name: pywis_topics.pygeoapi_plugin.WIS2TopicHierarchyListTopicsProcessor  # noqa
#
# pywis-topics-validate:
#     type: process
#     processor:
#         name: pywis_topics.pygeoapi_plugin.WIS2TopicHierarchyListTopicsProcessor  # noqa
#
# 3. (re)start pygeoapi
#
# The resulting processes will be available at the following endpoints:
#
# /processes/pywis-topics-wis2-topics-list
#
# /processes/pywis-topics-wis2-topics-validate
#
# Note that pygeoapi's OpenAPI/Swagger interface (at /openapi) will also
# provide a developer-friendly interface to test and run requests
#


import logging

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from pywis_topics.topics import TopicHierarchy

LOGGER = logging.getLogger(__name__)

WIS2_TOPIC_HIERARCHY_LINKS = [{
    'type': 'text/html',
    'rel': 'about',
    'title': 'information',
    'href': 'https://github.com/wmo-im/wis2-topic-hierarchy',
    'hreflang': 'en-US'
}]

WIS2_TOPIC_HIERARCHY_INPUT_TOPIC = {
    'title': 'Topic',
    'description': 'Topic (full or partial)',
    'schema': {
        'type': 'string'
    },
    'minOccurs': 1,
    'maxOccurs': 1,
    'metadata': None,
    'keywords': ['topic']
}


PROCESS_LIST_TOPICS = {
    'version': '0.1.0',
    'id': 'pywis-topics-list',
    'title': {
        'en': 'List WIS2 topics'
    },
    'description': {
        'en': 'Lists WIS2 topics'
    },
    'keywords': ['wis2', 'topics', 'metadata'],
    'links': WIS2_TOPIC_HIERARCHY_LINKS,
    'inputs': {
        'topic': WIS2_TOPIC_HIERARCHY_INPUT_TOPIC,
    },
    'outputs': {
        'result': {
            'title': 'List of child topics',
            'description': 'List of child topics',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json',
                'properties': {
                    'topics': {
                        'type': 'array',
                        'minOccurs': 1,
                        'items': {
                            'type': 'string',
                            'descriptiopn': 'Matching topic'
                        }
                    }
                }
            }
        }
    },
    'example': {
        'inputs': {
            'topic': 'origin/a/wis2'
        }
    }
}


PROCESS_VALIDATE_TOPIC = {
    'version': '0.1.0',
    'id': 'pywis-topics-validate',
    'title': {
        'en': 'Validate WIS2 topics'
    },
    'description': {
        'en': 'Validates WIS2 topics'
    },
    'keywords': ['wis2', 'topics', 'metadata'],
    'links': WIS2_TOPIC_HIERARCHY_LINKS,
    'inputs': {
        'topic': WIS2_TOPIC_HIERARCHY_INPUT_TOPIC,
    },
    'outputs': {
        'result': {
            'title': 'Result of topic validity',
            'description': 'Result of topic validity',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json',
                'properties': {
                    'topic_is_valid': {
                        'type': 'boolean'
                    }
                },
                'required': ['topic_is_valie']
            }
        }
    },
    'example': {
        'inputs': {
            'topic': 'origin/a/wis2'
        }
    }
}


class WIS2TopicHierarchyListTopicsProcessor(BaseProcessor):
    """WIS2 topic hierarchy list process"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pywis_topics.pygeoapi_plugin.WIS2TopicHierarchyListTopicsProcessor  # noqa
        """

        super().__init__(processor_def, PROCESS_LIST_TOPICS)

    def execute(self, data):

        response = None
        mimetype = 'application/json'
        topic = data.get('topic')

        if topic is None:
            msg = 'Missing topic'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        LOGGER.debug('Querying topic')
        th = TopicHierarchy()
        try:
            matching_topics = th.list_children(topic)
            response = {
                'topics': matching_topics
            }
        except ValueError as err:
            raise ProcessorExecuteError(err)

        return mimetype, response

    def __repr__(self):
        return '<WIS2TopicHierarchyListTopicsProcessor>'


class WIS2TopicHierarchyValidateTopicProcessor(BaseProcessor):
    """WIS2 topic hierarchy validate process"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pywis_topics.pygeoapi_plugin.WIS2TopicHierarchyValidateTopicProcessor  # noqa
        """

        super().__init__(processor_def, PROCESS_VALIDATE_TOPIC)

    def execute(self, data):

        response = None
        mimetype = 'application/json'
        topic = data.get('topic')

        if topic is None:
            msg = 'Missing topic'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        LOGGER.debug('Querying topic')
        th = TopicHierarchy()
        response = {
            'topic_is_valid': th.validate(topic)
        }
        return mimetype, response

    def __repr__(self):
        return '<WIS2TopicHierarchyValidateTopicProcessor>'
