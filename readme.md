# PullQueue
PullQueue Project provide the [PullQueue](src/main/java/org/name/zicat/queue/PullQueue.java) interface, it's implement [FilePullQueue](src/main/java/org/name/zicat/queue/FilePullQueue.java) and [PartitionFilePullQueue](src/main/java/org/name/zicat/queue/PartitionFilePullQueue.java).

# Usage
The design of PullQueue is most likely to kafka log segment supporting repeat read data from PullQueue by FileOffset and GroupId.

In delivery system, FileQueue play an important role as channel like flume.

# Main Concepts

## [Segment](src/main/java/org/name/zicat/queue/Segment.java)

A segment is a set of data, write block to page cache, and flush cache to one file. Detail design:
![image](/docs/segment_struct.png)

## [FilePullQueue](src/main/java/org/name/zicat/queue/FilePullQueue.java)

A FilePullQueue manage a set of segments include create new segments, delete expired segments, auto flush writeable segment etc.

Compare with kafka concepts, an instance of FilePullQueue equals to one partition of a topic in kafka.

A FilePullQueue provide the management of consumer group include commit file offset and group id, get file offset by group id, restore group id on disk for persistence.

## [PartitionFilePullQueue](src/main/java/org/name/zicat/queue/PartitionFilePullQueue.java)

A PartitionFilePullQueue manage a set of FilePullQueue with partitions.

Compare with kafka concepts, an instance of FilePullQueue equals to a topic.

# Demo

Go To [FilePullQueueTest](src/test/java/org/name/zicat/queue/test/FilePullQueueTest.java)