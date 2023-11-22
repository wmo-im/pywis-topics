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

import unittest

from pywis_topics.topics import TopicHierarchy


class WIS2TopicHierarchyTest(unittest.TestCase):
    """WIS2 Topic Hierarchy tests"""

    def setUp(self):
        """setup test fixtures, etc."""
        self.th = TopicHierarchy()
        pass

    def tearDown(self):
        """return to pristine state"""
        pass

    def test_validate(self):
        value = None
        with self.assertRaises(AttributeError):
            _ = self.th.validate(value)

        value = 'invalid/topic/hierarchy'
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2'
        self.assertTrue(self.th.validate(value))

        value = 'a/wis2'
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2/fake-centre-id/data/core'
        self.assertTrue(self.th.validate(value, strict=False))

    def test_list_children(self):
        value = None
        with self.assertRaises(AttributeError):
            children = self.th.list_children(value)

        value = 'invalid.topic.hierarchy'
        with self.assertRaises(ValueError):
            _ = self.th.list_children(value)

        value = 'cache/c'
        with self.assertRaises(ValueError):
            _ = self.th.list_children(value)

        value = 'cache'
        children = self.th.list_children(value)
        self.assertEqual(children, ['a'])


if __name__ == '__main__':
    unittest.main()
