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
"""Mappers defined for the converter.

This module contains mappings between Oozie actions and corresponding mappers that handle
particular actions.

"""

from typing import Type, Dict

from mappers.action_mapper import ActionMapper
from mappers.base_mapper import BaseMapper
from mappers.decision_mapper import DecisionMapper
from mappers.dummy_mapper import DummyMapper
from mappers.kill_mapper import KillMapper
from mappers.mapreduce_mapper import MapReduceMapper
from mappers.pig_mapper import PigMapper
from mappers.shell_mapper import ShellMapper
from mappers.spark_mapper import SparkMapper
from mappers.ssh_mapper import SSHMapper
from mappers.subworkflow_mapper import SubworkflowMapper


CONTROL_MAP: Dict[str, Type[BaseMapper]] = {
    "decision": DecisionMapper,
    "end": DummyMapper,
    "kill": KillMapper,
    "fork": DummyMapper,
    "join": DummyMapper,
    "start": DummyMapper,
}

ACTION_MAP: Dict[str, Type[ActionMapper]] = {
    "unknown": DummyMapper,
    "ssh": SSHMapper,
    "spark": SparkMapper,
    "pig": PigMapper,
    "sub-workflow": SubworkflowMapper,
    "shell": ShellMapper,
    "map-reduce": MapReduceMapper,
}
