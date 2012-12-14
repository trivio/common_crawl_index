# Copyright 2012 Triv.io, Scott Robertson]
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

from unittest import TestCase

from nose.tools import eq_

from .pbtree import PBTreeWriter, PBTreeReader, IndexWriter
from tempfile import TemporaryFile

class TestPBTree(TestCase):
  def test_btree_index(self):
    t = TemporaryFile()
    pbtree = PBTreeWriter(t)
    pbtree.add("blah", 1)
    #pbtree.commit()
    
    #t.seek(0)
    packet = pbtree.data_segment.write_buffer #t.read()
    
    eq_(packet, 'blah\x00\x01\x00\x00\x00\x00\x00\x00\x00')
    
  def test_one_key_per_block_writer(self):
    # 2 pointers and a 1 byte string null terminated string = 10 bytes
    stream = TemporaryFile()
    
    i = IndexWriter(stream, block_size=10, terminator='\0')
    i.add(0, 'b')
    eq_(len(i.indexes), 1)
    
    i.add(0, 'c')
    eq_(len(i.indexes), 2)
    i.finish()


    stream.seek(0)
    packet = stream.read()
    eq_(len(packet), 30)
    

    root_block = packet[:10]
    eq_(root_block, '\x01\x00\x00\x00c\x00\x02\x00\x00\x00')
    
    block_1 = packet[10:20]
    eq_(block_1, '\x03\x00\x00\x00b\x00\x04\x00\x00\x00')
    
    block_2 = packet[20:]
    eq_(block_2, '\x04\x00\x00\x00c\x00\x05\x00\x00\x00')