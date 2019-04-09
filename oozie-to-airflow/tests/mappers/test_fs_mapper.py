# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import ast
import unittest
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from converter.relation import Relation
from mappers import pig_mapper, fs_mapper
from mappers.fs_mapper import (
    bool_value,
    FS_OP_TOUCHZ,
    FS_OP_CHMOD,
    FS_OP_MKDIR,
    FS_OP_CHGRP,
    FS_OP_SETREP,
    FS_OP_MOVE,
    FS_OP_DELETE,
    FS_TAG_PATH,
    FS_TAG_SOURCE,
    FS_TAG_TARGET,
    FS_TAG_GROUP,
    FS_TAG_REPLFAC,
    FS_TAG_RECURSIVE,
    FS_TAG_PERMISSIONS,
    FS_TAG_DIRFILES,
    FS_TAG_SKIPTRASH,
)


class bool_valueTest(unittest.TestCase):
    def test_results(self):
        self.assertTrue(bool_value(ET.Element("op", {"test_attrib": "true"}), "test_attrib"))
        self.assertFalse(bool_value(ET.Element("op", {"test_attrib": "false"}), "test_attrib"))
        self.assertFalse(bool_value(ET.Element("op"), "test_attrib"))


class ChainTestCase(unittest.TestCase):
    def test_empty(self):
        relations = fs_mapper.chain([])
        self.assertEqual(relations, [])

    def test_one(self):
        relations = fs_mapper.chain([fs_mapper.SubOperator(task_id="A", rendered_template="")])
        self.assertEqual(relations, [])

    def test_multiple(self):
        relations = fs_mapper.chain(
            [
                fs_mapper.SubOperator(task_id="task_1", rendered_template=""),
                fs_mapper.SubOperator(task_id="task_2", rendered_template=""),
                fs_mapper.SubOperator(task_id="task_3", rendered_template=""),
                fs_mapper.SubOperator(task_id="task_4", rendered_template=""),
            ]
        )
        self.assertEqual(
            relations,
            [
                Relation(from_name="task_1", to_name="task_2"),
                Relation(from_name="task_2", to_name="task_3"),
                Relation(from_name="task_3", to_name="task_4"),
            ],
        )


class FsMapperTestCase(unittest.TestCase):
    def test_empty(self):
        pig = ET.Element("fs")
        self.et = ET.ElementTree(pig)
        self.mapper = fs_mapper.FsMapper(
            oozie_node=self.et.getroot(), name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        with self.subTest("test_convert_to_text"):
            # Throws a syntax error if doesn't parse correctly
            ast.parse(self.mapper.convert_to_text())

        with self.subTest("test_required_imports"):
            imps = pig_mapper.PigMapper.required_imports()
            imp_str = "\n".join(imps)
            ast.parse(imp_str)

        with self.subTest("test_get_first_task_id"):
            self.assertEqual(self.mapper.get_first_task_id(), "test_id")

        with self.subTest("test_get_last_task_id"):
            self.assertEqual(self.mapper.get_last_task_id(), "test_id")

    def test_complex(self):
        pig = ET.Element("fs")
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-delete-1"})
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-delete-1"})
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-delete-2"})
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-delete-3"})
        ET.SubElement(pig, FS_OP_DELETE, {FS_TAG_PATH: "/home/pig/test-delete-1", FS_TAG_SKIPTRASH: "true"})
        ET.SubElement(pig, FS_OP_DELETE, {FS_TAG_PATH: "/home/pig/test-delete-2", FS_TAG_SKIPTRASH: "false"})
        ET.SubElement(pig, FS_OP_DELETE, {FS_TAG_PATH: "/home/pig/test-delete-3"})
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-delete-1"})
        ET.SubElement(
            pig,
            FS_OP_MOVE,
            {FS_TAG_SOURCE: "/home/pig/test-chmod-1", FS_TAG_TARGET: "/home/pig/test-chmod-2"},
        )
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-chmod-1"})
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-chmod-2"})
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-chmod-3"})
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-chmod-4"})
        ET.SubElement(
            pig,
            FS_OP_CHMOD,
            {
                FS_TAG_PATH: "/home/pig/test-chmod-1",
                FS_TAG_PERMISSIONS: "-rwxrw-rw-",
                FS_TAG_DIRFILES: "false",
            },
        )
        ET.SubElement(
            pig,
            FS_OP_CHMOD,
            {
                FS_TAG_PATH: "/home/pig/test-chmod-2",
                FS_TAG_PERMISSIONS: "-rwxrw-rw-",
                FS_TAG_DIRFILES: "true",
            },
        )
        ET.SubElement(
            pig, FS_OP_CHMOD, {FS_TAG_PATH: "/home/pig/test-chmod-3", FS_TAG_PERMISSIONS: "-rwxrw-rw-"}
        )
        recursive_chmod = ET.SubElement(
            pig,
            FS_OP_CHMOD,
            {
                FS_TAG_PATH: "/home/pig/test-chmod-4",
                FS_TAG_PERMISSIONS: "-rwxrw-rw-",
                FS_TAG_DIRFILES: "false",
            },
        )
        ET.SubElement(recursive_chmod, FS_TAG_RECURSIVE)

        ET.SubElement(pig, FS_OP_TOUCHZ, {FS_TAG_PATH: "/home/pig/test-touchz-1"})
        ET.SubElement(pig, FS_OP_CHGRP, {FS_TAG_PATH: "/home/pig/test-touchz-1", FS_TAG_GROUP: "pig"})
        ET.SubElement(pig, FS_OP_MKDIR, {FS_TAG_PATH: "/home/pig/test-setrep-1"})
        ET.SubElement(pig, FS_OP_SETREP, {FS_TAG_PATH: "/home/pig/test-setrep-1", FS_TAG_REPLFAC: "2"})

        self.et = ET.ElementTree(pig)
        self.mapper = fs_mapper.FsMapper(
            oozie_node=self.et.getroot(), name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        with self.subTest("test_convert_to_text"):
            # Throws a syntax error if doesn't parse correctly
            ast.parse(self.mapper.convert_to_text())

        with self.subTest("test_required_imports"):
            imps = pig_mapper.PigMapper.required_imports()
            imp_str = "\n".join(imps)
            ast.parse(imp_str)

        with self.subTest("test_get_first_task_id"):
            self.assertEqual(self.mapper.get_first_task_id(), "test_id_fs_0")

        with self.subTest("test_get_last_task_id"):
            self.assertEqual(self.mapper.get_last_task_id(), "test_id_fs_20")
