# Copyright 2012 Triv.io, Scott Robertson
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

from  itertools import izip_longest, dropwhile

def commonlen(s1,s2):
  """
  Returns the length of the common prefix
  """
      
  # given "hi", "hip"
  # izip_longest("hi", "hip") -> ('h','h'), ('i','i'), (None, 'p')
  # enumerate -> (0,('h','h')), (1,('i','i')), (2,(None, 'p'))
  # dropwhile(lambda (i,(x,y)): x == 5 -> (2,(None,'p')) ...
  
  try:  
    return dropwhile(lambda (i,(x,y)): x == y,enumerate(zip(s1, s2))).next()[0]
  except StopIteration:
    # strings are identical return the len of one of them
    return len(s1)

def common(s1,s2):
  """
  Returns the  common prefix
  """
  cl = commonlen(s1,s2)
  return s2[:cl]

def signifigant(s1,s2):
  """
  Given two strings s1 and s2, and assuming s2 > s1 returns the character
  that make s2 gerater.
  """  
  cl = commonlen(s1,s2)
  return s2[:cl+1]
