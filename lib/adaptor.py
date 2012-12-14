# Copyright [2012] [Triv.io, Scott Robertson]
# 
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
# 
#       http://www.apache.org/licenses/LICENSE-2.0
# 
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# 

import re
import struct


from triv.io import datasources
from .pbtree import PBTreeDictWriter, DataWriter, IndexWriter, DataBlockReader

BLOCK_SIZE   = 2**16
VALUE_FORMAT = "<QQIQ"
VALUE_SIZE   = struct.calcsize(VALUE_FORMAT)

ITEM_KEYS=(
  'arcSourceSegmentId',
  'arcFileDate',
  'arcFileParition',
  'arcFileOffset'
)


@datasources.write_mimetype('application/vnd.commoncrawl.org.url_index')
def output_stream(stream):
  return PBTreeDictWriter(stream, 
    value_format=VALUE_FORMAT,
    block_size =BLOCK_SIZE,
    item_keys=ITEM_KEYS
  )
  
  
@datasources.write_mimetype('application/vnd.commoncrawl.org.data_segment')
def output_stream(stream):
  class DictPacker(object):
    def __init__(self, item_keys, value_format):
      self.item_keys = item_keys
      self.value_format = value_format
      self.block_count = 0

    def pack_value(self, dict):
      value = [dict[k] for k in self.item_keys]      
      return struct.pack(self.value_format, *value)
    
    def on_new_block(self, key):
      self.block_count += 1
        
    def on_item_exceeds_block_size(self, key,value):
      print ""
  
  return DataWriter(stream, BLOCK_SIZE, '\0', DictPacker(ITEM_KEYS, VALUE_FORMAT))
  
@datasources.write_mimetype('application/vnd.commoncrawl.org.index_segment')
def output_stream(stream):
  return IndexWriter(stream, block_size=BLOCK_SIZE, terminator='\0')



@datasources.read_mimetype('application/vnd.commoncrawl.org.data_segment')
def data_block_reader(fd, size, url, params):
  part_number = "%04d" % int(re.search(':reduce:(\d+)\-',url).group(1))

  # read each block in the part, yielding the first key and part number
  
  params.last_key = None
  
  while True:
    block_buffer = fd.read(BLOCK_SIZE)
    if block_buffer == '':
      break
    block = iter(DataBlockReader(block_buffer, VALUE_SIZE))
    yield part_number, block
  