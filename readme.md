# PullQueue
PullQueue Project provide the [PullQueue](/src/main/java/org/name/zicat/queue/PullQueue.java) interface, it's implement [FilePullQueue](/src/main/java/org/name/zicat/queue/FilePullQueue.java) and [PartitionFilePullQueue](/src/main/java/org/name/zicat/queue/PartitionFilePullQueue.java)

# Usage
The design of PullQueue is most likely to kafka log segment supporting repeat read data from PullQueue by FileOffset and GroupId.

In delivery system like flume, FileQueue play an important role as channel like flume.

# Demo

Go To [FilePullQueueTest](/src/test/java/org/name/zicat/queue/test/FilePullQueueTest.java)