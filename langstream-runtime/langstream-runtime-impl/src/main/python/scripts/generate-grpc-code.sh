#!/bin/bash
poetry install
grpc_proto_dir=../../../../../langstream-agents/langstream-agent-grpc/src/main/proto/langstream_grpc/proto
out_dir=./langstream_grpc/proto
poetry run python -m grpc_tools.protoc \
  -I${grpc_proto_dir} \
  --python_out=${out_dir} \
  --pyi_out=${out_dir} \
  --grpc_python_out=${out_dir} \
  ${grpc_proto_dir}/agent.proto