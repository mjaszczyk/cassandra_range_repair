range_repair.py
======================

A Python script to repair the primary range of a Cassandra node in N discrete steps [using best practices](http://www.datastax.com/dev/blog/advanced-repair-techniques).

### Background
When Cassandra begins the repair process it constructs a [merkle tree](http://en.wikipedia.org/wiki/Merkle_tree), which is a tree of hashes over segments of data that the node is responsible for. The node compares it's tree to that of the replicas, if there is a difference in the hash values for any of the nodes then the segment for that hash is requested from the replica and is re-inserted.

By default the Merkle tree for Cassandra represents 15 discreet segments, which means that the data on the node is broken into 15 pieces. If the data for one of these 15 pieces is different from that of the replicas then it will result in 1/15th of the data being streamed and re-inserted. This can cause problems for two use cases: dense nodes, and DSE Solr nodes running at or near capacity.

#### Solr Nodes
For DSE Solr nodes when the data transferred as the result of a repair is re-inserted it is also re-indexed because each host maintains it's own independent set of indexes. If the node is already at or near capacity then the additional strain caused by the repair/re-index can push it over the edge. As a last result Cassandra will shed load by dropping mutations. If a mutation is dropped the data will at some point need to be brought over from the replicas through the repair process which unfortunately begins a never ending cycle of re-index/repair.

#### Dense Nodes
For clusters that have a large amount of data per node the repair process could require an unacceptably large amount of data to be streamed and re-inserted.

### How the script works
The script works by figuring out the primary range for the node that it's being executed on, and instead of running repair on the entire range, run the repair on only a smaller sub-range. When a repair is initiated on a sub-range Cassandra constructs a merkle tree only for the range specified, which in turn divides the much smaller range into 15 segments. If there is disagreement in any of the hash values then a much smaller portion of data needs to be transferred which lessens load on the system.

### Options

```
Usage: range_repair.py [options]

Options:
  -h, --help            show this help message and exit
  -k KEYSPACE, --keyspace=KEYSPACE
                        Keyspace to repair
  -c COLUMNFAMILY, --columnfamily=COLUMNFAMILY
                        ColumnFamily to repair
  -H HOST, --host=HOST  Hostname to repair
  -s STEPS, --steps=STEPS
                        Number of discrete ranges
  --nodetool=NODETOOL   Nodetool bin location
  --repair-concurrency=REPAIRCONCURRENCY
                        How many repairs can run in parallel. Default: 10
  --all-dcs             Do not limit repair for local DC
  -v, --verbose         Verbose mode
```

### Sample

```
$ LOG_LEVEL="DEBUG" ./range_repair.py -k demo_keyspace
INFO       2014-05-09 17:31:33,503    get_ring_tokens                 66  : running nodetool ring, this will take a little bit of time
DEBUG      2014-05-09 17:31:39,057    get_ring_tokens                 72  : ring tokens found, creating ring token list...
DEBUG      2014-05-09 17:31:40,207    get_host_tokens                 86  : host tokens found, creating host token list...
DEBUG      2014-05-09 17:31:40,208    repair_keyspace                 170 : repair over range (-2974082934175371230, -2971948823734978979] with 100 steps for keyspace demo_keyspace
DEBUG      2014-05-09 17:31:40,208    repair_keyspace                 176 : step 0100 repairing range (-2974082934175371230, -2974061593070967308] for keyspace demo_keyspace ...
DEBUG      2014-05-09 17:32:47,508    repair_keyspace                 182 : SUCCESS
DEBUG      2014-05-09 17:32:47,509    repair_keyspace                 176 : step 0099 repairing range (-2974061593070967308, -2974040251966563386] for keyspace demo_keyspace ...
DEBUG      2014-05-09 17:33:54,904    repair_keyspace                 182 : SUCCESS
...
```

### Dependencies
-   Python 2.6
-   Cassandra ```nodetool``` must exist in the ```PATH```

### History
- Originally by [Matt Stump](https://github.com/mstump)
- Converted to work with vnodes by [Brian Gallew](https://github.com/BrianGallew)
- Additional functionality by [Eric Lubow](http://github.com/elubow)
- Running repair steps in parallel and minor changes by [Michal Jaszczyk @Scandit](https://github.com/mjaszczyk)
