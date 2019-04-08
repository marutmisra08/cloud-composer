# Copyright 2018 Google LLC
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
import os
import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from converter import parser
from converter import parsed_node
from converter.mappers import ACTION_MAP, CONTROL_MAP
from converter.relation import Relation
from definitions import ROOT_DIR
from mappers import dummy_mapper
from mappers import ssh_mapper
from tests.utils.test_paths import EXAMPLE_DEMO_PATH


class TestOozieParser(unittest.TestCase):
    def setUp(self):
        params = {}
        self.parser = parser.OozieParser(
            input_directory_path=EXAMPLE_DEMO_PATH,
            output_directory_path="/tmp",
            params=params,
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
        )

    def test_parse_kill_node(self):
        node_name = "kill_name"
        kill = ET.Element("kill", attrib={"name": node_name})
        message = ET.SubElement(kill, "message")
        message.text = "kill-text-to-log"

        self.parser._parse_kill_node(kill)

        self.assertIn(node_name, self.parser.OPERATORS)
        for depend in self.parser.OPERATORS[node_name].mapper.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_end_node(self):
        node_name = "end_name"
        end = ET.Element("end", attrib={"name": node_name})

        self.parser._parse_end_node(end)

        self.assertIn(node_name, self.parser.OPERATORS)
        for depend in self.parser.OPERATORS[node_name].mapper.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    @mock.patch("converter.parser.OozieParser.parse_node")
    def test_parse_fork_node(self, parse_node_mock):
        node_name = "fork_name"
        root = ET.Element("root")
        fork = ET.SubElement(root, "fork", attrib={"name": node_name})
        path1 = ET.SubElement(fork, "path", attrib={"start": "task1"})
        path2 = ET.SubElement(fork, "path", attrib={"start": "task2"})
        node1 = ET.SubElement(root, "action", attrib={"name": "task1"})
        node2 = ET.SubElement(root, "action", attrib={"name": "task2"})
        join = ET.SubElement(root, "join", attrib={"name": "join", "to": "end_node"})
        end = ET.SubElement(root, "end", attrib={"name": "end_node"})

        self.parser._parse_fork_node(root, fork)

        p_op = self.parser.OPERATORS[node_name]
        self.assertIn("task1", p_op.get_downstreams())
        self.assertIn("task2", p_op.get_downstreams())
        self.assertIn(node_name, self.parser.OPERATORS)
        parse_node_mock.assert_any_call(root, node1)
        parse_node_mock.assert_any_call(root, node2)
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_join_node(self):
        node_name = "join_name"
        end_name = "end_name"
        join = ET.Element("join", attrib={"name": node_name, "to": end_name})

        self.parser._parse_join_node(join)

        p_op = self.parser.OPERATORS[node_name]
        self.assertIn(node_name, self.parser.OPERATORS)
        self.assertIn(end_name, p_op.get_downstreams())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_decision_node(self):
        node_name = "decision_node"
        decision = ET.Element("decision", attrib={"name": node_name})
        switch = ET.SubElement(decision, "switch")
        case1 = ET.SubElement(switch, "case", attrib={"to": "down1"})
        case1.text = "${fs:fileSize(secondjobOutputDir) gt 10 * GB}"
        case2 = ET.SubElement(switch, "case", attrib={"to": "down2"})
        case2.text = "${fs:filSize(secondjobOutputDir) lt 100 * MB}"
        default = ET.SubElement(switch, "switch", attrib={"to": "end1"})

        self.parser._parse_decision_node(decision)

        p_op = self.parser.OPERATORS[node_name]
        self.assertIn(node_name, self.parser.OPERATORS)
        self.assertIn("down1", p_op.get_downstreams())
        self.assertIn("down2", p_op.get_downstreams())
        self.assertIn("end1", p_op.get_downstreams())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    @mock.patch("uuid.uuid4")
    def test_parse_start_node(self, uuid_mock):
        uuid_mock.return_value = "1234"
        node_name = "start_node_1234"
        end_name = "end_name"
        start = ET.Element("start", attrib={"to": end_name})

        self.parser._parse_start_node(start)

        p_op = self.parser.OPERATORS[node_name]
        self.assertIn(node_name, self.parser.OPERATORS)
        self.assertIn(end_name, p_op.get_downstreams())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_action_node_ssh(self):
        self.parser.ACTION_MAP = {"ssh": ssh_mapper.SSHMapper}
        node_name = "action_name"
        # Set up XML to mimic Oozie
        action = ET.Element("action", attrib={"name": node_name})
        ssh = ET.SubElement(action, "ssh")
        ok = ET.SubElement(action, "ok", attrib={"to": "end1"})
        error = ET.SubElement(action, "error", attrib={"to": "fail1"})
        host = ET.SubElement(ssh, "host")
        command = ET.SubElement(ssh, "command")
        args1 = ET.SubElement(ssh, "args")
        args2 = ET.SubElement(ssh, "args")
        cap_out = ET.SubElement(ssh, "capture-output")

        host.text = "user@apache.org"
        command.text = "ls"
        args1.text = "-l"
        args2.text = "-a"
        # default does not have text

        self.parser._parse_action_node(action)

        p_op = self.parser.OPERATORS[node_name]
        self.assertIn(node_name, self.parser.OPERATORS)
        self.assertIn("end1", p_op.get_downstreams())
        self.assertEqual("fail1", p_op.get_error_downstream_name())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_action_node_unknown(self):
        self.parser.ACTION_MAP = {"unknown": dummy_mapper.DummyMapper}
        NODE_NAME = "action_name"
        # Set up XML to mimic Oozie
        action = ET.Element("action", attrib={"name": NODE_NAME})
        ssh = ET.SubElement(action, "ssh")
        ET.SubElement(action, "ok", attrib={"to": "end1"})
        ET.SubElement(action, "error", attrib={"to": "fail1"})
        ET.SubElement(ssh, "host")
        ET.SubElement(ssh, "command")
        ET.SubElement(ssh, "args")
        ET.SubElement(ssh, "args")
        ET.SubElement(ssh, "capture-output")

        self.parser._parse_action_node(action)

        p_op = self.parser.OPERATORS[NODE_NAME]
        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        self.assertIn("end1", p_op.get_downstreams())
        self.assertEqual("fail1", p_op.get_error_downstream_name())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    @mock.patch("converter.parser.OozieParser._parse_action_node")
    def test_parse_node_action(self, action_mock):
        root = ET.Element("root")
        action = ET.SubElement(root, "action", attrib={"name": "test_name"})
        self.parser.parse_node(root, action)
        action_mock.assert_called_once_with(action)

    @mock.patch("converter.parser.OozieParser._parse_start_node")
    def test_parse_node_start(self, start_mock):
        root = ET.Element("root")
        start = ET.SubElement(root, "start", attrib={"name": "test_name"})
        self.parser.parse_node(root, start)
        start_mock.assert_called_once_with(start)

    @mock.patch("converter.parser.OozieParser._parse_kill_node")
    def test_parse_node_kill(self, kill_mock):
        root = ET.Element("root")
        kill = ET.SubElement(root, "kill", attrib={"name": "test_name"})
        self.parser.parse_node(root, kill)
        kill_mock.assert_called_once_with(kill)

    @mock.patch("converter.parser.OozieParser._parse_end_node")
    def test_parse_node_end(self, end_mock):
        root = ET.Element("root")
        end = ET.SubElement(root, "end", attrib={"name": "test_name"})
        self.parser.parse_node(root, end)
        end_mock.assert_called_once_with(end)

    @mock.patch("converter.parser.OozieParser._parse_fork_node")
    def test_parse_node_fork(self, fork_mock):
        root = ET.Element("root")
        fork = ET.SubElement(root, "fork", attrib={"name": "test_name"})
        self.parser.parse_node(root, fork)
        fork_mock.assert_called_once_with(root, fork)

    @mock.patch("converter.parser.OozieParser._parse_join_node")
    def test_parse_node_join(self, join_mock):
        root = ET.Element("root")
        join = ET.SubElement(root, "join", attrib={"name": "test_name"})
        self.parser.parse_node(root, join)
        join_mock.assert_called_once_with(join)

    @mock.patch("converter.parser.OozieParser._parse_decision_node")
    def test_parse_node_decision(self, decision_mock):
        root = ET.Element("root")
        decision = ET.SubElement(root, "decision", attrib={"name": "test_name"})
        self.parser.parse_node(root, decision)
        decision_mock.assert_called_once_with(decision)

    def test_create_relations(self):
        op1 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="task1"))
        op1.downstream_names = ["task2", "task3"]
        op1.error_xml = "fail1"
        op2 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="task2"))
        op2.downstream_names = ["task3"]
        op2.error_xml = "fail1"
        op3 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="task3"))
        op3.downstream_names = ["end1"]
        op3.error_xml = "fail1"
        end = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="end1"))
        fail = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="fail1"))
        op_dict = {"task1": op1, "task2": op2, "task3": op3, "end1": end, "fail1": fail}
        self.parser.OPERATORS.update(op_dict)
        self.parser.create_relations()

        self.assertIn(Relation("task3", "fail1"), self.parser.relations)
        self.assertIn(Relation("task3", "end1"), self.parser.relations)
        self.assertIn(Relation("task1", "task2"), self.parser.relations)
        self.assertIn(Relation("task1", "task3"), self.parser.relations)
        self.assertIn(Relation("task2", "task3"), self.parser.relations)
        self.assertIn(Relation("task2", "fail1"), self.parser.relations)
        self.assertIn(Relation("task1", "fail1"), self.parser.relations)

    def test_update_trigger_rules(self):
        op1 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="task1"))
        op1.downstream_names = ["task2", "task3"]
        op1.error_xml = "fail1"
        op2 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="task2"))
        op2.downstream_names = ["task3"]
        op2.error_xml = "fail1"
        op3 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="task3"))
        op3.downstream_names = ["end1"]
        op3.error_xml = "fail1"
        end = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="end1"))
        fail = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, name="fail1"))
        op_dict = {"task1": op1, "task2": op2, "task3": op3, "end1": end, "fail1": fail}

        self.parser.OPERATORS.update(op_dict)
        self.parser.create_relations()
        self.parser.update_trigger_rules()

        self.assertFalse(op1.is_ok)
        self.assertFalse(op1.is_error)
        self.assertTrue(op2.is_ok)
        self.assertFalse(op2.is_error)
        self.assertTrue(op3.is_ok)
        self.assertFalse(op2.is_error)
        self.assertTrue(end.is_ok)
        self.assertFalse(end.is_error)
        self.assertFalse(fail.is_ok)
        self.assertTrue(fail.is_error)

    def test_parse_workflow(self):
        filename = os.path.join(ROOT_DIR, "examples/demo/workflow.xml")
        self.parser.workflow = filename
        self.parser.parse_workflow()
        # Checking if names were changed to the Python syntax
        self.assertIn("cleanup_node", self.parser.OPERATORS)
        self.assertIn("fork_node", self.parser.OPERATORS)
        self.assertIn("pig_node", self.parser.OPERATORS)
        self.assertIn("fail", self.parser.OPERATORS)
