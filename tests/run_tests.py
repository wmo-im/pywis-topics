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

from pywis_topics.centre_id import CentreId
from pywis_topics.topics import TopicHierarchy


class WIS2CentreIdTest(unittest.TestCase):
    """WIS2 centre-id tests"""

    def setUp(self):
        """setup test fixtures, etc."""
        pass

    def tearDown(self):
        """return to pristine state"""
        pass

    def test_validate(self):
        value = "badcentre"
        with self.assertRaises(ValueError):
            cid = CentreId(value)

        invalid_centre_ids = [
            'MY-CENTRE',
            'my-Centre',
            'dh-some-centre'
        ]

        valid_centre_ids = [
            'int-centre123'
            'int-centre123-vaac'
            'int-my-centre-dcpc'
            'int-my_centre-dcpc'
        ]

        for invalid_centre_id in invalid_centre_ids:
            cid = CentreId(invalid_centre_id)
            self.assertFalse(cid.validate())

        for valid_centre_id in valid_centre_ids:
            cid = CentreId(valid_centre_id)
            self.assertTrue(cid.validate())


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
        with self.assertRaises(ValueError):
            _ = self.th.validate(value)

        invalid_topics = [
            'invalid.topic.hierarchy',
            'ORIGIN/A/wis2',
            'origin/a/wis2/ca-é',
            'invalid/topic/hierarchy',
            'a/wis2'
        ]

        valid_topics = [
            'cache/a/wis2',
            'cache/a/wis2/ca-eccc-msc/data/core',
        ]

        for invalid_topic in invalid_topics:
            self.assertFalse(self.th.validate(invalid_topic))

        for valid_topic in valid_topics:
            self.assertTrue(self.th.validate(valid_topic))

        value = 'cache/a/wis2/fake-centre-id/data/core'
        self.assertTrue(self.th.validate(value, strict=False))

        value = 'cache/a/+'
        self.assertTrue(self.th.validate(value, strict=False))
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/#'
        self.assertTrue(self.th.validate(value, strict=False))
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2/+/data/core/#'
        self.assertTrue(self.th.validate(value, strict=False))
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2/+/data/core/weather/#'
        self.assertTrue(self.th.validate(value, strict=False))
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2/+/data/#/weather'
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2/+/data/core/weather/surface-based-observations'
        self.assertTrue(self.th.validate(value, strict=False))
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2/ca-eccc-msc/data/core/weather/surface-based-observations'  # noqa
        self.assertTrue(self.th.validate(value))

        value = 'cache/a/wis2/ca-eccc-msc/data/core/weather/surface-based-observations1'  # noqa
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2/ca-eccc-msc/data/core/weather/surface-based-observations/'  # noqa
        self.assertFalse(self.th.validate(value))

        value = 'cache/a/wis2/ca-eccc-msc/data/core/weather/experimental/surface-based-observations'  # noqa
        self.assertTrue(self.th.validate(value))

        value = 'origin/a/wis2/ca-eccc-msc/data/core/ocean'
        self.assertTrue(self.th.validate(value))

        value = 'cache/a/wis2/ca-eccc-msc/data/core/ocean'
        self.assertTrue(self.th.validate(value))

        value = 'cache/a/wis2/io-wis2dev-11-test/data/core/ocean'
        self.assertTrue(self.th.validate(value))

        value = 'cache/a/wis2/ca-eccc-msc/data/core/weather/surface-based-observations/#'  # noqa
        self.assertTrue(self.th.validate(value, strict=False))
        self.assertFalse(self.th.validate(value))

    def test_list_children(self):
        value = None
        with self.assertRaises(ValueError):
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
