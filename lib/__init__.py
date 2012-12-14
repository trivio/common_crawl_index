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

import urlparse

def reversehost(url):
# reverse netlocation http://www.example.com/foo -> com.example.www/foo:http
  url = urlparse.urlsplit(str(url))
  
  netloc = url.netloc.split(':')
  host = netloc[0]
  if len(netloc) == 2:
    port = ':' + netloc[1]
  else:
    port = ''
  

  # reverse the host
  host = '.'.join(reversed(host.split('.')))

  return (
    host + 
    url.path + 
    (('?' + url.query) if url.query else '' ) +
    port +
    ':' + url.scheme
  )

