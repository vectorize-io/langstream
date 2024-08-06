#!/bin/bash
#
# Copyright DataStax, Inc.
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
#

poetry install
grpc_proto_dir=../../../../../langstream-agents/langstream-agent-grpc/src/main/proto/langstream_grpc/proto
out_dir=./langstream_grpc/proto
poetry run python -m grpc_tools.protoc \
  -I${grpc_proto_dir} \
  --python_out=${out_dir} \
  --pyi_out=${out_dir} \
  --grpc_python_out=${out_dir} \
  ${grpc_proto_dir}/agent.proto