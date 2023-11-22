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

# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from langstream_grpc.proto import agent_pb2 as langstream__grpc_dot_proto_dot_agent__pb2


class AgentServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.agent_info = channel.unary_unary(
            "/AgentService/agent_info",
            request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            response_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.InfoResponse.FromString,
        )
        self.read = channel.stream_stream(
            "/AgentService/read",
            request_serializer=langstream__grpc_dot_proto_dot_agent__pb2.SourceRequest.SerializeToString,
            response_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.SourceResponse.FromString,
        )
        self.process = channel.stream_stream(
            "/AgentService/process",
            request_serializer=langstream__grpc_dot_proto_dot_agent__pb2.ProcessorRequest.SerializeToString,
            response_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.ProcessorResponse.FromString,
        )
        self.write = channel.stream_stream(
            "/AgentService/write",
            request_serializer=langstream__grpc_dot_proto_dot_agent__pb2.SinkRequest.SerializeToString,
            response_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.SinkResponse.FromString,
        )
        self.get_topic_producer_records = channel.stream_stream(
            "/AgentService/get_topic_producer_records",
            request_serializer=langstream__grpc_dot_proto_dot_agent__pb2.TopicProducerWriteResult.SerializeToString,
            response_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.TopicProducerResponse.FromString,
        )


class AgentServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def agent_info(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def read(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def process(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def write(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def get_topic_producer_records(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_AgentServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "agent_info": grpc.unary_unary_rpc_method_handler(
            servicer.agent_info,
            request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            response_serializer=langstream__grpc_dot_proto_dot_agent__pb2.InfoResponse.SerializeToString,
        ),
        "read": grpc.stream_stream_rpc_method_handler(
            servicer.read,
            request_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.SourceRequest.FromString,
            response_serializer=langstream__grpc_dot_proto_dot_agent__pb2.SourceResponse.SerializeToString,
        ),
        "process": grpc.stream_stream_rpc_method_handler(
            servicer.process,
            request_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.ProcessorRequest.FromString,
            response_serializer=langstream__grpc_dot_proto_dot_agent__pb2.ProcessorResponse.SerializeToString,
        ),
        "write": grpc.stream_stream_rpc_method_handler(
            servicer.write,
            request_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.SinkRequest.FromString,
            response_serializer=langstream__grpc_dot_proto_dot_agent__pb2.SinkResponse.SerializeToString,
        ),
        "get_topic_producer_records": grpc.stream_stream_rpc_method_handler(
            servicer.get_topic_producer_records,
            request_deserializer=langstream__grpc_dot_proto_dot_agent__pb2.TopicProducerWriteResult.FromString,
            response_serializer=langstream__grpc_dot_proto_dot_agent__pb2.TopicProducerResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "AgentService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class AgentService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def agent_info(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/AgentService/agent_info",
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            langstream__grpc_dot_proto_dot_agent__pb2.InfoResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def read(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/AgentService/read",
            langstream__grpc_dot_proto_dot_agent__pb2.SourceRequest.SerializeToString,
            langstream__grpc_dot_proto_dot_agent__pb2.SourceResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def process(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/AgentService/process",
            langstream__grpc_dot_proto_dot_agent__pb2.ProcessorRequest.SerializeToString,
            langstream__grpc_dot_proto_dot_agent__pb2.ProcessorResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def write(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/AgentService/write",
            langstream__grpc_dot_proto_dot_agent__pb2.SinkRequest.SerializeToString,
            langstream__grpc_dot_proto_dot_agent__pb2.SinkResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def get_topic_producer_records(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/AgentService/get_topic_producer_records",
            langstream__grpc_dot_proto_dot_agent__pb2.TopicProducerWriteResult.SerializeToString,
            langstream__grpc_dot_proto_dot_agent__pb2.TopicProducerResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
