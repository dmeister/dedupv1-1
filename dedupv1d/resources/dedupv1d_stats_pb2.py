# Generated by the protocol buffer compiler.  DO NOT EDIT!

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)


DESCRIPTOR = descriptor.FileDescriptor(
  name='dedupv1d_stats.proto',
  package='',
  serialized_pb='\n\x14\x64\x65\x64upv1d_stats.proto\"\xa6\x02\n\x17\x43ommandHandlerStatsData\x12\x1a\n\x12scsi_command_count\x18\x01 \x01(\x04\x12\x34\n\x0copcode_stats\x18\x02 \x03(\x0b\x32\x1e.CommandHandlerOpcodeStatsData\x12\x39\n\x0ftask_mgmt_stats\x18\x03 \x03(\x0b\x32 .CommandHandlerTaskMgmtStatsData\x12\x32\n\x0b\x65rror_stats\x18\x04 \x03(\x0b\x32\x1d.CommandHandlerErrorStatsData\x12\x19\n\x11sector_read_count\x18\x05 \x01(\x04\x12\x1a\n\x12sector_write_count\x18\x06 \x01(\x04\x12\x13\n\x0bretry_count\x18\x07 \x01(\x04\">\n\x1d\x43ommandHandlerOpcodeStatsData\x12\x0e\n\x06opcode\x18\x01 \x01(\x05\x12\r\n\x05\x63ount\x18\x02 \x01(\x04\"@\n\x1f\x43ommandHandlerTaskMgmtStatsData\x12\x0e\n\x06tmcode\x18\x01 \x01(\x05\x12\r\n\x05\x63ount\x18\x02 \x01(\x04\"=\n\x1c\x43ommandHandlerErrorStatsData\x12\x0e\n\x06opcode\x18\x01 \x01(\x05\x12\r\n\x05\x63ount\x18\x02 \x01(\x04\")\n\x11\x44\x65\x64upv1dStatsData\x12\x14\n\x0cservice_time\x18\x01 \x01(\x01')




_COMMANDHANDLERSTATSDATA = descriptor.Descriptor(
  name='CommandHandlerStatsData',
  full_name='CommandHandlerStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='scsi_command_count', full_name='CommandHandlerStatsData.scsi_command_count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='opcode_stats', full_name='CommandHandlerStatsData.opcode_stats', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='task_mgmt_stats', full_name='CommandHandlerStatsData.task_mgmt_stats', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='error_stats', full_name='CommandHandlerStatsData.error_stats', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='sector_read_count', full_name='CommandHandlerStatsData.sector_read_count', index=4,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='sector_write_count', full_name='CommandHandlerStatsData.sector_write_count', index=5,
      number=6, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='retry_count', full_name='CommandHandlerStatsData.retry_count', index=6,
      number=7, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=25,
  serialized_end=319,
)


_COMMANDHANDLEROPCODESTATSDATA = descriptor.Descriptor(
  name='CommandHandlerOpcodeStatsData',
  full_name='CommandHandlerOpcodeStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='opcode', full_name='CommandHandlerOpcodeStatsData.opcode', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='count', full_name='CommandHandlerOpcodeStatsData.count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=321,
  serialized_end=383,
)


_COMMANDHANDLERTASKMGMTSTATSDATA = descriptor.Descriptor(
  name='CommandHandlerTaskMgmtStatsData',
  full_name='CommandHandlerTaskMgmtStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='tmcode', full_name='CommandHandlerTaskMgmtStatsData.tmcode', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='count', full_name='CommandHandlerTaskMgmtStatsData.count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=385,
  serialized_end=449,
)


_COMMANDHANDLERERRORSTATSDATA = descriptor.Descriptor(
  name='CommandHandlerErrorStatsData',
  full_name='CommandHandlerErrorStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='opcode', full_name='CommandHandlerErrorStatsData.opcode', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='count', full_name='CommandHandlerErrorStatsData.count', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=451,
  serialized_end=512,
)


_DEDUPV1DSTATSDATA = descriptor.Descriptor(
  name='Dedupv1dStatsData',
  full_name='Dedupv1dStatsData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='service_time', full_name='Dedupv1dStatsData.service_time', index=0,
      number=1, type=1, cpp_type=5, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=514,
  serialized_end=555,
)


_COMMANDHANDLERSTATSDATA.fields_by_name['opcode_stats'].message_type = _COMMANDHANDLEROPCODESTATSDATA
_COMMANDHANDLERSTATSDATA.fields_by_name['task_mgmt_stats'].message_type = _COMMANDHANDLERTASKMGMTSTATSDATA
_COMMANDHANDLERSTATSDATA.fields_by_name['error_stats'].message_type = _COMMANDHANDLERERRORSTATSDATA

class CommandHandlerStatsData(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _COMMANDHANDLERSTATSDATA
  
  # @@protoc_insertion_point(class_scope:CommandHandlerStatsData)

class CommandHandlerOpcodeStatsData(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _COMMANDHANDLEROPCODESTATSDATA
  
  # @@protoc_insertion_point(class_scope:CommandHandlerOpcodeStatsData)

class CommandHandlerTaskMgmtStatsData(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _COMMANDHANDLERTASKMGMTSTATSDATA
  
  # @@protoc_insertion_point(class_scope:CommandHandlerTaskMgmtStatsData)

class CommandHandlerErrorStatsData(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _COMMANDHANDLERERRORSTATSDATA
  
  # @@protoc_insertion_point(class_scope:CommandHandlerErrorStatsData)

class Dedupv1dStatsData(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _DEDUPV1DSTATSDATA
  
  # @@protoc_insertion_point(class_scope:Dedupv1dStatsData)

# @@protoc_insertion_point(module_scope)
