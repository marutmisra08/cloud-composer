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
import shlex
from typing import Set, NamedTuple

from converter.parser import Relation
from mappers.action_mapper import ActionMapper
from utils.template_utils import render_template
from xml.etree.ElementTree import Element

ACTION_TYPE = "fs"

FS_OP_MKDIR = "mkdir"
FS_OP_DELETE = "delete"
FS_OP_MOVE = "move"
FS_OP_CHMOD = "chmod"
FS_OP_TOUCHZ = "touchz"
FS_OP_CHGRP = "chgrp"
FS_OP_SETREP = "setrep"

FS_TAG_PATH = "path"
FS_TAG_SOURCE = "source"
FS_TAG_TARGET = "target"
FS_TAG_RECURSIVE = "recursive"
FS_TAG_DIRFILES = "dir-files"
FS_TAG_SKIPTRASH = "skip-trash"
FS_TAG_PERMISSIONS = "permissions"
FS_TAG_GROUP = "group"
FS_TAG_REPLFAC = "replication-factor"


def bool_value(node, attr_name, default="false"):
    value = node.attrib.get(attr_name, default)
    return value is not None and value != "false"


def prepare_mkdir_command(node: Element):
    path = node.attrib[FS_TAG_PATH]
    command = "fs -mkdir {path}".format(path=shlex.quote(path))
    return command


def prepare_delete_command(node: Element):
    path = node.attrib[FS_TAG_PATH]
    command = "fs -rm -r {path}".format(path=shlex.quote(path))
    if bool_value(node, FS_TAG_SKIPTRASH):
        command += " -skipTrash"
    return command


def prepare_move_command(node: Element):
    source = node.attrib[FS_TAG_SOURCE]
    target = node.attrib[FS_TAG_TARGET]

    command = "fs -mv {source} {target}".format(source=source, target=target)
    return command


def prepare_chmod_command(node: Element):
    path = node.attrib[FS_TAG_PATH]
    permission = node.attrib[FS_TAG_PERMISSIONS]
    # TODO: Add support for dirFiles
    # dirFiles = bool_value(node, FS_TAG_DIRFILES)
    recursive = node.find(FS_TAG_RECURSIVE) is not None
    extra_param = "-R" if recursive else ""

    command = "fs -chmod {extra} {path} '{permission}'".format(
        extra=extra_param, path=shlex.quote(path), permission=shlex.quote(permission)
    )
    return command


def prepare_touchz_command(node: Element):
    path = node.attrib[FS_TAG_PATH]

    command = "fs -touchz {path}".format(path=shlex.quote(path))
    return command


def prepare_chgrp_command(node: Element):
    path = node.attrib[FS_TAG_PATH]
    group = node.attrib[FS_TAG_GROUP]

    recursive = node.find(FS_TAG_RECURSIVE) is not None
    extra_param = "-R" if recursive else ""

    command = "fs -chgrp {extra} {path} {group}".format(
        extra=extra_param, path=shlex.quote(path), group=shlex.quote(group)
    )
    return command


def prepare_setrep_command(node: Element):
    path = node.attrib[FS_TAG_PATH]
    fac = node.attrib[FS_TAG_REPLFAC]

    command = "fs -setrep {fac} {path}".format(fac=shlex.quote(fac), path=shlex.quote(path))
    return command


FS_OPERATION_MAPPERS = {
    FS_OP_MKDIR: prepare_mkdir_command,
    FS_OP_DELETE: prepare_delete_command,
    FS_OP_MOVE: prepare_move_command,
    FS_OP_CHMOD: prepare_chmod_command,
    FS_OP_TOUCHZ: prepare_touchz_command,
    FS_OP_CHGRP: prepare_chgrp_command,
    FS_OP_SETREP: prepare_setrep_command,
}


class BaseFsActionMapper:
    template_name = None


class SubOperator(NamedTuple):
    task_id: str
    rendered_template: str


def chain(ops):
    return [Relation(from_name=a.task_id, to_name=b.task_id) for a, b in zip(ops, ops[1::])]


class FsMapper(ActionMapper):
    def get_sub_operators(self):
        if len(self.oozie_node) == 0:
            return [
                SubOperator(
                    task_id=self.name,
                    rendered_template=render_template(
                        "dummy.tpl", task_id=self.name, trigger_rule=self.trigger_rule
                    ),
                )
            ]
        return [self.parse_fs_action(i, node) for i, node in enumerate(self.oozie_node)]

    def convert_to_text(self):
        sub_ops = self.get_sub_operators()

        return render_template(
            template_name="fs.tpl",
            task_id=self.name,
            trigger_rule=self.trigger_rule,
            sub_ops=sub_ops,
            relations=chain(sub_ops),
        )

    @staticmethod
    def required_imports() -> Set[str]:
        return {
            "from airflow.operators import dummy_operator",
            "from airflow.operators import bash_operator",
            "import shlex",
        }

    def get_first_task_id(self):
        return self.get_sub_operators()[0].task_id

    def get_last_task_id(self):
        return self.get_sub_operators()[-1].task_id

    def parse_fs_action(self, index: int, node: Element):
        task_id = "{}_fs_{}".format(self.name, index)

        tag_name = node.tag
        mapper_fn = FS_OPERATION_MAPPERS.get(tag_name)

        if not mapper_fn:
            raise Exception("Unknown FS operation: {}".format(tag_name))

        pig_command = mapper_fn(node)
        rendered_template = render_template("fs_op.tpl", task_id=task_id, pig_command=pig_command)

        return SubOperator(task_id=task_id, rendered_template=rendered_template)
