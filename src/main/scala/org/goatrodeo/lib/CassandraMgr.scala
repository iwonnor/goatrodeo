/*
 * CassandraMgr.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.goatrodeo.lib

/*
import org.apache.cassandra.service.CassandraDaemon

import java.io._

class CassandraMgr(workingDir: String) {
  private def init() {
    System.setProperty("storage-config", workingDir)
    (new File(workingDir+"/commitlog")).mkdir
    (new File(workingDir+"/data")).mkdir
    (new File(workingDir+"/callouts")).mkdir
    (new File(workingDir+"/bootstrap")).mkdir
    (new File(workingDir+"/staging")).mkdir
    (new File(workingDir+"/logs")).mkdir

    val xml =
    <Storage>
    <!--======================================================================-->
    <!-- Basic Configuration                                                  -->
    <!--======================================================================-->

    <!-- The name of this cluster. This is mainly used to prevent machines in
         one logical cluster from joining any other cluster. -->
    <ClusterName>Test Cluster</ClusterName>

    <!-- Tables and ColumnFamilies
         Think of a table as a namespace, not a relational table.
         (ColumnFamilies are closer in meaning to those.)

         There is an implicit table named 'system' for Cassandra internals.

         The default ColumnSort is Time for standard column families.
         For super column families, specifying ColumnSort is not supported;
         the supercolumns themselves are always name-sorted and their subcolumns
         are always time-sorted.
    -->
    <Tables>
        <Table Name="Table1">
            <!-- The fraction of keys per sstable whose locations we
                 keep in memory in "mostly LRU" order.  (JUST the key
                 locations, NOT any column values.)

                 The amount of memory used by the default setting of
                 0.01 is comparable to the amount used by the internal
                 per-sstable key index. Consider increasing this is
                 fine if you have fewer, wider rows.  Set to 0 to
                 disable entirely.
            -->
            <KeysCachedFraction>0.01</KeysCachedFraction>
            <!--
                 The CompareWith attribute tells Cassandra how to sort the columns
                 for slicing operations.  For backwards compatibility, the default
                 is to use AsciiType, which is probably NOT what you want.
                 Other options are BytesType, UTF8Type, UUIDType, and LongType.
                 You can also specify the fully-qualified class name to a class
                 of your choice implementing org.apache.cassandra.db.marshal.IType.

                 SuperColumns have a similar CompareSubcolumnsWith attribute.

                 (So to get the closest approximation to 0.3-style supercolumns,
                 you would use CompareWith=UTF8Type CompareSubcolumnsWith=LongType.)

                 if FlushPeriodInMinutes is configured and positive, it will be
                 flushed to disk with that period whether it is dirty or not.
                 This is intended for lightly-used columnfamilies so that they
                 do not prevent commitlog segments from being purged.

            -->
            <ColumnFamily CompareWith="UTF8Type" Name="Standard1" FlushPeriodInMinutes="60"/>
            <ColumnFamily CompareWith="UTF8Type" Name="Standard2"/>
            <ColumnFamily CompareWith="UUIDType" Name="StandardByUUID1"/>
            <ColumnFamily ColumnType="Super" CompareWith="UTF8Type" CompareSubcolumnsWith="UTF8Type" Name="Super1"/>
        </Table>
    </Tables>

    <!-- Partitioner: any IPartitioner may be used, including your own
         as long as it is on the classpath.  Out of the box,
         Cassandra provides
         org.apache.cassandra.dht.RandomPartitioner and
         org.apache.cassandra.dht.OrderPreservingPartitioner.
         Range queries require using OrderPreservingPartitioner or a subclass.

         Achtung!  Changing this parameter requires wiping your data directories,
         since the partitioner can modify the sstable on-disk format.
    -->
    <Partitioner>org.apache.cassandra.dht.RandomPartitioner</Partitioner>

    <!-- If you are using the OrderPreservingPartitioner and you know your key
         distribution, you can specify the token for this node to use.
         (Keys are sent to the node with the "closest" token, so distributing
         your tokens equally along the key distribution space will spread
         keys evenly across your cluster.)  This setting is only checked the
         first time a node is started.

         This can also be useful with RandomPartitioner to force equal
         spacing of tokens around the hash space, especially for
         clusters with a small number of nodes. -->
    <InitialToken></InitialToken>

    <!-- RackAware: Setting this to true instructs Cassandra to try
         and place one replica in a different datacenter, and the
         others on different racks in the same one.  If you haven't
         looked at the code for RackAwareStrategy, leave this off.
    -->
    <RackAware>false</RackAware>

    <!-- Number of replicas of the data-->
    <ReplicationFactor>1</ReplicationFactor>

    <!-- Directories: Specify where Cassandra should store different data on disk
         Keep the data disks and the CommitLog disks separate for best performance
    -->
    <CommitLogDirectory>{workingDir}commitlog</CommitLogDirectory>
    <DataFileDirectories>
        <DataFileDirectory>{workingDir}data</DataFileDirectory>
    </DataFileDirectories>
    <CalloutLocation>{workingDir}callouts</CalloutLocation>
    <BootstrapFileDirectory>{workingDir}bootstrap</BootstrapFileDirectory>
    <StagingFileDirectory>{workingDir}staging</StagingFileDirectory>


    <!-- Addresses of hosts that are deemed contact points. Cassandra nodes use
         this list of hosts to find each other and learn the topology of the ring.
         You must change this if you are running multiple nodes!
    -->
    <Seeds>
        <Seed>127.0.0.1</Seed>
    </Seeds>


    <!-- Miscellaneous -->

    <!-- time to wait for a reply from other nodes before failing the command -->
    <RpcTimeoutInMillis>5000</RpcTimeoutInMillis>
    <!-- size to allow commitlog to grow to before creating a new segment -->
    <CommitLogRotationThresholdInMB>128</CommitLogRotationThresholdInMB>


    <!-- Local hosts and ports -->

    <!-- Address to bind to and tell other nodes to connect to.
         You _must_ change this if you want multiple nodes to be able
         to communicate!

         Leaving it blank leaves it up to InetAddress.getLocalHost().
         This will always do the Right Thing *if* the node is properly
         configured (hostname, name resolution, etc), and the Right
         Thing is to use the address associated with the hostname (it
         might not be). -->
    <ListenAddress>localhost</ListenAddress>
    <!-- TCP port, for commands and data -->
    <StoragePort>7000</StoragePort>
    <!-- UDP port, for membership communications (gossip) -->
    <ControlPort>7001</ControlPort>

    <!-- The address to bind the Thrift RPC service to. Unlike
         ListenAddress above, you *can* specify 0.0.0.0 here if you want
         Thrift to listen on all interfaces.

         Leaving this blank has the same effect it does for ListenAddress,
         (i.e. it will be based on the configured hostname of the node).
    -->
    <ThriftAddress>localhost</ThriftAddress>
    <!-- Thrift RPC port (the port clients connect to). -->
    <ThriftPort>9160</ThriftPort>


    <!--======================================================================-->
    <!-- Memory, Disk, and Performance                                        -->
    <!--======================================================================-->

    <!-- Add column indexes to a row after its contents reach this size -->
    <ColumnIndexSizeInKB>256</ColumnIndexSizeInKB>

    <!--
      The maximum amount of data to store in memory before flushing to
      disk. Note: There is one memtable per column family, and this threshold
      is based solely on the amount of data stored, not actual heap memory
      usage (there is some overhead in indexing the columns).
    -->
    <MemtableSizeInMB>32</MemtableSizeInMB>

    <!--
      The maximum number of columns in millions to store in memory
      before flushing to disk.  This is also a per-memtable setting.
      Use with MemtableSizeInMB to tune memory usage.
    -->
    <MemtableObjectCountInMillions>0.02</MemtableObjectCountInMillions>

    <!-- Time to wait before garbage-collection deletion markers.
         Set this to a large enough value that you are confident
         that the deletion marker will be propagated to all replicas
         by the time this many seconds has elapsed, even in the
         face of hardware failures.  The default value is ten days.
    -->
    <GCGraceSeconds>864000</GCGraceSeconds>
</Storage>

    val log4j =
"""# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# for production, you should probably set the root to INFO
# and the pattern to %c instead of %l.  (%l is slower.)

# output messages into a rolling log file as well as stdout
log4j.rootLogger=DEBUG,stdout,R

# stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.SimpleLayout

# rolling log file ("system.log
log4j.appender.R=org.apache.log4j.DailyRollingFileAppender
log4j.appender.R.DatePattern='.'yyyy-MM-dd-HH
log4j.appender.R.layout=org.apache.log4j.PatternLayout
log4j.appender.R.layout.ConversionPattern=%5p [%t] %d{ISO8601} %F (line %L) %m%n
# Edit the next line to point to your logs directory
log4j.appender.R.File="""+workingDir+"""/logs/system.log

# Application logging options
#log4j.logger.com.facebook=DEBUG
#log4j.logger.com.facebook.infrastructure.gms=DEBUG
#log4j.logger.com.facebook.infrastructure.db=DEBUG
"""

    val fos = new FileOutputStream(workingDir+"/storage-conf.xml")
    fos.write(xml.toString.getBytes("UTF-8"))
    fos.flush
    fos.close

    val f2 = new FileOutputStream(workingDir+"/log4j.properties")
    f2.write(log4j.getBytes("UTF-8"))
    f2.flush
    f2.close

  }

  def startCassandra() {
    (new Thread(new Runnable{ def run() {
            try {
    CassandraDaemon.main(Array())
            } catch {
              case e => println("**** Sigh")
                e.printStackTrace
            }
    }})).start
  }

  init()
}
*/