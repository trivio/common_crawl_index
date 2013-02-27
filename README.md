![Trivio Logo](/docs/project_header.png?raw=true)

==================

The Common Crawl data  is composed of billions of  pages randomly crawled from the internet. Each page is archived with thousands of other pages into an archive file, commonly known as an ARC file. There are hundreds of thousands of these archives and the pages are stored essentially randomly distributed and unordered within these archives. 


Without an index, even if you know a pages URL, you're forced to download, uncompress and each of the archives until you locate the pages you're are interested in.

Using the index described here, you can find all the archive files that contain pages for a give URL prefix, subdomain or top level domain with no more than 3 small network requests.



Challenges
---------
To understand the design of the index let's first look at the challenges unique to the Common Crawl Project. 

Specifically:


1. It's huge
  * 5 Billion unique URLs
  * Average URL is 66 bytes
  * A pointer to an individual page requires an additional 28 bytes 
   (see file format for details)
   
  Just storing this information  uncompressed, requires a file greater
  than 437 GB in size. (5x 10^9 x (66+28)).
  
2. The size alone prevents constructing the entire index in memory

3. Or.. even on one modest machine

4. There's a large community with a variety of tools interested in accessing the index.

5. Common Crawl is a non profit, it's especially important to keep processing and hosting costs down while ensuring the data is highly available.

Since 2012, the Common Crawl data has been hosted for free by Amazon on the Amazon Public Data Sets. The Amazon Public Data Set program is a boon to everyone who uses data - it lowers the overhead costs for organizations that want to share data, lowers the cost for users to download open data, and makes valuable data sets more discoverable.

Putting this altogether leads to the following goals.


Goals:
------

  * Store and share the entire index from S3 without any other services
  * The entire index should be utilizable without downloading the entire thing.
  * The number of network calls needed should be minimized.
  * You can search for any URL, URL prefix, subdomain or top-level domain.
  * Once you've determined the approximate location of the URL you're
  interested in you can sequentially read other similar urls.
  * It should be easy to access the index from any programming language *This the main reason we opted to roll our own format rather than rely on a third party library


File Format
------------

The file format is based on a [Prefixed B-tree](http://ict.pue.udlap.mx/people/carlos/is215/papers/p11-bayer.pdf). The proceeding link will take you to a paper that gives you an in depth overview of this data structure, however, hopefully, the information we provide here will be enough for your to utilize the index without requiring you to read the whole paper. 

Conceptually the index is organized into a b+ tree like this.

![Tree](/docs/tree.png?raw=true)

To access any given URL in the index, you  start by reading the root block in the tree and then follow the pointers to zero or more other index blocks and finally to a data block. The urls is the data block are stored in lexicographic order. So for  a url of `http://example.com/page1.html` will come before `http://example.com/page2.html`. Because of this property you
can find all the pages that share a common prefix by subsequently reading
each url in the data portion of the file.


###File overview
The entire index plus data are stored in one file that has 3 major parts. **The header**, **index blocks** and **data blocks** as depicted below.

![File Overview](/docs/file_overview.png?raw=true)


###Header
The header is exactly 8 bytes long. The first 4 bytes represents the **block size** used in the file and the second 4 bytes represent the number of index blocks or **block count** contained in the file.

All numbers are encoded in little-endian order

![Header](/docs/header.png?raw=true)

Once you have the block size and block count you can randomly access any block by following the instructions in the Operations section of this guide.

Interpreting a block depends on whether it is a **Index block** or a **Data block**.


###Index block
Any block number that is less than the block count in the header is considered an index block. It is interpreted as follows:

![Index Block](/docs/index_block.png?raw=true)

An index block always starts with 4 byte &lt;block number&gt; then one or more &lt;prefix&gt; &lt;null&gt; &lt;block number&gt; triplets until the next triplet can not fit within the block, at which point it is padded with additional &lt;null&gt; bytes.
  
As a result should you ever encounter a &lt;null&gt; immediately after reading a &lt;block number&gt; you know you've reached the end of the block.

See the section on searching the index to see how to use the index blocks to find the first data block appropriate for your search.

###Data block

Data blocks consist of oner or more &lt;url&gt; &lt;null&gt; &lt;location pointer&gt; collectively known as an **item**. There are a variable number of items in a block based on the length of the url. 

Just like index blocks, data blocks are padded with &lt;null&gt; bytes, when during construction, the next item  can not fit within the given block size.

![Data Block](/docs/data_block.png?raw=true)


A &lt;url&gt; consists of one or more characters terminated by the null byte. 
  
The location pointer is 32 bytes long and can be interpreted as follows. The first 8 bytes represents the segment id, the next 8 bytes represents the ARC file creation date, followed by 4 bytes that represent the ARC file partition, followed by 8 bytes that represent the offset within the ARC file and then
finally the last 4 bytes represent the size of compressed data stored inside the ARC file.

See the the section on retrieving a page, to put all this information together to access a page.


Operations
----------

###Read a block

Once you've read the header you can use this information to randomly read any block given it's block number. Keep in mind the size of the header is exactly 8 bytes long.

First determine the offset of the block in the file

```
block offset = (block number * block size) + header size
```

Then read the range of bytes starting from the *block offset* plus the size of a single block.

For example imagine the block size is 65536 bytes long and you wanted to read  block number 2. You'd first calculate tho block offset which is (2 x 65536) + 8 = 131080. Then you would read the block. 

If you were making an http request to S3 this would be done using the HTTP Range header as follows

```
Range: bytes=131080-196616
```

If you have download the index you can simply seek to position 131080 in the file and then read the next 65536 bytes.

###Jumping directly to the start of the data block

Any block number that is larger than the index block count in the header, is  a data block. You can skip over all the index blocks to the very first data block by following the instructions in the Reading a Block  and using the block count as the block number.

###Searching the Index

To find a URL in the index or find all the URLs that start with a common prefix, we'll call this a "target", start by reading block 0 also known as the **root block**

1. Read through each prefix found in the index block until you find the first prefix that is lexicographically greater than or equal to the given target.

2. The next block number you need to read is the number that was found immediately to the left of the prefix that was greater than or equal to your target. If your target is bigger than every prefix in the block use the last block number in the block.

3. Repeat steps 1 through 3 until the block number is greater than the block count that is stored in the header. You've now found the first data block that could possibly contain your target.


Once you've found the first data block.

1. Retrieve the given block number
2. Read all characters up to the first &lt;null&gt; byte
3. Read the  32 byte location pointer
4. Repeat steps 1-3; as long as the URL is greater than the target or you reach the end of the block. If you reach the end of the block without finding the target, it is not in the index.
5. Now read each item out of the data block, until each item url no longer starts with the target string.


###Retrieving a page

The location pointer represents 5 numbers:

* segment id
* file date
* partition
* file offset
* compressed sized

Using the first 3 numbers you can construct the URL of the arc file that contains the page you are interested in.

```
 s3://aws-publicdatasets/common-crawl/parse-output/segment/[segment id]/[file date]_[partition].arc.gz
```

The file offset and compressed size can be used to fetch the compressed chunk from the arc file, without
downloading the entire arc file.

Here is an example using the boto library in python to retreive and uncompress the chunk.



```python
def arc_file(s3, bucket, info):

  bucket = s3.lookup(bucket)
  keyname = "/common-crawl/parse-output/segment/{arcSourceSegmentId}/{arcFileDate}_{arcFileParition}.
arc.gz".format(**info)
  key = bucket.lookup(keyname)
  
  start = info['arcFileOffset']
  end = start + info['compressedSize'] - 1
  
  headers={'Range' : 'bytes={}-{}'.format(start, end)}
  
  chunk = StringIO(
    key.get_contents_as_string(headers=headers)
  )
  
  return GzipFile(fileobj=chunk).read()
```
 

