# <data segment> :: = <item> (<item> | ( {<null>} <null>))
# <item> ::= <key> <null> <location pointer>

# 
#

from unittest import TestCase
from .prefix import signifigant


class TestIndexMapCase(TestCase):
  def test(self):
    class P():
      pass
    params = P()
  
    results =[]
    for partition_number, input in enumerate(inputs):
      params.last_key = None
      params.partition_number = partition_number
      for block in input:
        for item in map_block(iter(block), params):
          results.append(item)

    self.assertSequenceEqual(results, final)  



file1 = [
  [
    'key01',
    'key02',
    'key03a',
    'key03ac',
  ],
  [
    'key03bc'
    'key06',
    'key07',
    'key08z',
  ],
  [
    'key08zafz'
    'key10',
    'key11',
    'key12',
  ],
]

file2= [
  [
    'key13feee',
    'key14',
    'key16',
    'key16a',
  ],
  [
    'key16b'
    'key18',
    'key19',
    'key20',
  ]
]


final=(
  (0,"key01"),
  (0,"key03b"),
  (0,"key08za"),
  (1,"key13feee"),
  (1,"key16b")
)

inputs = [file1, file2]

def map_block(block, params):
  # yield first item and last

  first_key = block.next()
  assert first_key.find('\0') == -1
  
  if params.last_key is None:
    yield params.partition_number, first_key
  else:
    yield params.partition_number, signifigant(params.last_key, first_key)

  second_to_last = None
  for key in block:
    if not key.startswith('\0'):
      second_to_last = key

  if second_to_last is not None:
    params.last_key = second_to_last
  else:
    params.last_key = first_key
  
  
  
  
  