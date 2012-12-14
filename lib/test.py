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

from unittest import TestCase
from tempfile import NamedTemporaryFile
from functools import partial
from nose.tools import eq_
import mmap


class TestIndex(TestCase):
  def test_btree_index(self):
    from .pbtree import PBTreeWriter, PBTreeReader
    
    def data():
      """Returns an iterator of (url, linepos)"""
      return ((url.strip(), pos) for pos, url in enumerate(open('sorted_urls')))

    self.validate(
      PBTreeWriter,
      PBTreeReader,
      data(),
      prefix = 'http://natebeaty.com/',
      known_keys = [
        'http://natebeaty.com/illustration/4452349850',
        'http://natebeaty.com/illustration/4573016166',
        'http://natebeaty.com/illustration/4747271212',
        'http://natebeaty.com/illustration/4752986875',
      ],
      known_values =[
        1891,
        1892,
        1893,
        1894
      ]
    )
    
  def test_btree_dict_index(self):
    from .pbtree import PBTreeDictWriter, PBTreeDictReader
    
    writer = partial(PBTreeDictWriter, item_keys=("key1", "key2"), value_format="<QI")
    reader = partial(PBTreeDictReader, item_keys=("key1", "key2"), value_format="<QI")

    
    def data():
      for pos, url in enumerate(open('sorted_urls')):
        yield url.strip(), {'key1': pos, 'key2': pos}


    self.validate(
      writer,
      reader,
      data(),
      prefix = 'http://natebeaty.com/',
      known_keys = [
        'http://natebeaty.com/illustration/4452349850',
        'http://natebeaty.com/illustration/4573016166',
        'http://natebeaty.com/illustration/4747271212',
        'http://natebeaty.com/illustration/4752986875',
      ],
      known_values =[
        {'key1': 1891, 'key2': 1891},
        {'key1': 1892, 'key2': 1892},
        {'key1': 1893, 'key2': 1893},
        {'key1': 1894, 'key2': 1894}
      ]
    )
  
  
  def validate(self, writer, reader, data, prefix, known_keys, known_values):
    """
    Verify  given writer produces content that can be read by the given
    reader and  returns known keys and values for the given prefix
    
    """
  
    temp = NamedTemporaryFile(delete=False)
    index = writer(temp)

    keys_written = []
    for key, value in data:
      index.add(key, value)
      keys_written.append(key)
    index.close()

    ii = open(temp.name, 'r+')
    map = mmap.mmap(ii.fileno(),0)
    index = reader(map)
    keys = index.keys(prefix)

    self.assertListEqual(keys,known_keys)
    
    values = index.values(prefix)
    self.assertListEqual(values,known_values)
    
    items = index.items(prefix)

    self.assertListEqual(items, zip(known_keys, known_values))

    self.assertListEqual(index.keys(), keys_written)
  