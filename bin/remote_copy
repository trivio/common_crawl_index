#!/usr/bin/env python
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

import argparse
import boto
import os
import sys
import struct
import tempfile
import time
import traceback

from datetime import timedelta
from itertools import chain
from multiprocessing import Pool, Queue

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.pbtree import IndexBlockReader, PBTreeDictReader

class BotoMap(object):
    def __init__(self, s3, bucket, key_name):
        self.key = bucket.get_key(key_name)
        self.block_size = 2**16
        self.cached_block = -1

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = i.start
            end = i.stop - 1
        else:
            start = i
            end = start + 1
        
        return self.fetch(start,end)

    def fetch(self, start, end):
        try:
            return self.key.get_contents_as_string(
              headers={'Range' : 'bytes={}-{}'.format(start, end)}
            )
        except boto.exception.S3ResponseError, e:
            # invalid range, we've reached the end of the file
            if e.status == 416:
                return ''

# See copy_arc_files_init below
# Arguments except for index_results are passed using copy_arc_files_init
# so that copy_arc_files can be used with multiprocessing pool.map()
def copy_arc_files(index_results):
    try:
        s3_anon = boto.connect_s3(anon=True)
        s3_user = boto.connect_s3(copy_arc_files.access_key, copy_arc_files.secret_key)

        src_bucket_name = 'aws-publicdatasets'
        src_bucket = s3_anon.lookup(src_bucket_name)

        dest_bucket = s3_user.lookup(copy_arc_files.dest_bucket_name)

        src_keystem = '/common-crawl/parse-output/segment/{arcSourceSegmentId}/{arcFileDate}_{arcFilePartition}.arc.gz'
        
        chunk = tempfile.NamedTemporaryFile('ab+')
        src_key_cache = {}

        for i, key_info in enumerate(index_results):
            src_keyname = src_keystem.format(**key_info)

            src_key = None
            if src_keyname in src_key_cache:
                src_key = src_key_cache[src_keyname]
            else:
                src_key = src_bucket.lookup(src_keyname)
                src_key_cache[src_keyname] = src_key

            start = key_info['arcFileOffset']
            end = start + key_info['compressedSize'] - 1
            headers={'Range' : 'bytes={}-{}'.format(start, end)}

            if src_key:
                src_key.get_contents_to_file(chunk, headers=headers)
            else:
                copy_arc_files.progress_queue.put(("warning", "WARNING: could not find key " + src_keyname))
            copy_arc_files.progress_queue.put(("download", key_info['compressedSize']))

        dest_keyname = '/' + copy_arc_files.dest_keystem + '/' + str(os.getpid()) + ".gz"
        dest_key = dest_bucket.new_key(dest_keyname)

        upload_bytes = chunk.tell()
        chunk.seek(0, 0)
        dest_key.set_contents_from_file(chunk, replace=True)
        chunk.close()
        copy_arc_files.progress_queue.put(("upload", upload_bytes))
    except:
        print ""
        print "ERROR, pid=" + str(os.getpid())
        print traceback.format_exc()
        copy_arc_files.progress_queue.put(("error", None))

def copy_arc_files_init(access_key, secret_key, bucket_name, keystem, queue):
    copy_arc_files.access_key = access_key
    copy_arc_files.secret_key = secret_key
    copy_arc_files.dest_bucket_name = bucket_name
    copy_arc_files.dest_keystem = keystem
    copy_arc_files.progress_queue = queue

def partition(li, n):
    division = len(li) / float(n)
    for i in xrange(n):
        yield li[int(round(division * i)): int(round(division * (i + 1)))]

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Copy common-crawl webpages from a given domain or comma-delimited list of domains to an s3 location specified by the user.")
    argparser.add_argument('command', choices=['check', 'copy'], help='check to list # of webpages in index for given domain; copy to copy those webpages to an s3 location')
    argparser.add_argument('domains', help='domain or comma-delimited list of domains to check/copy from the index')
    argparser.add_argument('-b', '--bucket', help='webpages stored in s3://<bucket>/<key>/<process-id>.gz (multiple files if parallel > 1)')
    argparser.add_argument('-k', '--key', help='webpages stored in s3://<bucket>/<key>/<process-id>.gz (multiple files if parallel > 1)')
    argparser.add_argument('-p', '--parallel', type=int, default=4, help='how many parallel processes to run (default = 4)')
    argparser.add_argument('-O', '--aws-access-key', default=os.environ.get('AWS_ACCESS_KEY', None), help='AWS Access Key ID. Defaults to the value of the AWS_ACCESS_KEY environment variable (if set).')
    argparser.add_argument('-W', '--aws-secret-key', default=os.environ.get('AWS_SECRET_KEY', None), help='AWS Secret Access Key. Defaults to the value of the AWS_SECRET_KEY environment variable (if set).')
    args = argparser.parse_args()

    if not args.aws_access_key:
        argparser.error("Error: neither --aws-access-key option nor AWS_ACCESS_KEY environment variable set")
    if not args.aws_secret_key:
        argparser.error("Error: neither --aws-secret-key option nor AWS_SECRET_KEY environment variable set")

    if args.command == "copy":
        if not args.bucket:
            argparser.error("Error: --bucket option is required for copy operation")
        if not args.key:
            argparser.error("Error: --key option is required for copy operation")

    s3_anon = boto.connect_s3(anon=True)

    src_bucket_name = 'aws-publicdatasets'
    src_bucket = s3_anon.lookup(src_bucket_name)

    src_keystem = '/common-crawl/parse-output/segment/{arcSourceSegmentId}/{arcFileDate}_{arcFilePartition}.arc.gz'

    mmap = BotoMap(s3_anon, src_bucket, '/common-crawl/projects/url-index/url-index.1356128792')

    reader = PBTreeDictReader(
        mmap,
        value_format="<QQIQI", 
        item_keys=(
            'arcSourceSegmentId',
            'arcFileDate',
            'arcFilePartition',
            'arcFileOffset',
            'compressedSize'
        )
    )

    src_keys = set()
    index_results = []

    domains = [reader.itemsiter(s.strip()) for s in args.domains.split(',')]
    for url, index_data in chain.from_iterable(domains):
        src_keys.add(src_keystem.format(**index_data))
        index_results.append(index_data)

    num_files = len(src_keys)
    num_webpages = len(index_results)
    dest_compressed_size = sum([data['compressedSize'] for data in index_results])
    dest_compressed_size_mb = dest_compressed_size / 1000000

    print ""
    print "# files: " + str(num_files)
    print "# webpages: " + str(num_webpages)
    print ""
    print "Source compressed file size (MB): " + str(num_files * 100)
    print "Destination compressed file size (MB): " + str(dest_compressed_size_mb)
    print ""

    if args.command == "copy":
        if len(index_results) == 0:
            print "No webpages found for domains \"" + str(args.domains) + "\""
            print ""
            exit()

        print "Starting copy..."
        copy_start_time = time.time()
            
        progress_queue = Queue()
        pool = Pool(args.parallel, copy_arc_files_init, [args.aws_access_key, args.aws_secret_key, args.bucket, args.key, progress_queue])
        index_results = partition(index_results, args.parallel)

        result = pool.map_async(copy_arc_files, index_results)
        pool.close()

        bytes_downloaded = bytes_uploaded = 0
        while(True):
            if (result.ready()): 
                sys.stdout.write("\rDownload: 100%\tUpload: 100%")
                sys.stdout.flush()
                break

            while not progress_queue.empty():
                obj = progress_queue.get()
                if obj[0] == "download":
                    bytes_downloaded += obj[1]
                elif obj[0] == "upload":
                    bytes_uploaded += obj[1]
                elif obj[0] == "warning":
                    print "\n" + obj[1]
                elif obj[0] == "error":
                    # error logged in child process
                    pool.terminate()
                    pool.join()
                    exit()

            sys.stdout.write("\rDownload: %d%%\tUpload: %d%%" % (100 * bytes_downloaded / dest_compressed_size, 100 * bytes_uploaded / dest_compressed_size))
            sys.stdout.flush()
            time.sleep(1) 

        pool.join()

        copy_elapsed_time = int(round(time.time() - copy_start_time))
        copy_timedelta = timedelta(seconds=copy_elapsed_time)

        print ""
        print "Copy complete!"
        print "Took " + str(copy_timedelta)
        print ""
