/*
 * Cassandra.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.goatrodeo.lib

/*
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.NotFoundException;
import org.apache.cassandra.service.UnavailableException;
import org.apache.cassandra.service.column_t;
import org.apache.cassandra.service.Cassandra.Client;
import scala.collection.jcl.Conversions._

class CClient {
  private class AB(in: Array[Byte]) {
    def asString = new String(in, "UTF-8")
  }
  private implicit def btoAb(in: Array[Byte]) = new AB(in)

  def doIt() {
    val tr = new TSocket("localhost", 9160)
    val proto = new TBinaryProtocol(tr)
    try {
      val client = new Client(proto)
      tr.open()
      val key_user_id = "1"
      val timestamp = System.currentTimeMillis()
      client.insert("users", key_user_id, "base_attributes:name",
                    "Chris Goffinet".getBytes("UTF-8"), timestamp, 0)

      client.insert("users", key_user_id, "base_attributes:age",
                    "24".getBytes("UTF-8"), timestamp, 0)

      println("Column inform: "+client.get_column("users", key_user_id,
                                      "base_attributes:name").getValue.asString)

            println("Column inform: "+client.get_column("users", key_user_id,
                                      "base_attributes:age").getValue.asString)

      /*
      val results =  client.get_slice("users", key_user_id,
                                      "base_attributes", true, 100)

      for (col <- results) {
        System.out.println(col.getColumnName() + " -> " +
                           new String(col.getValue(),"UTF-8"));
      }
      */

    } finally {
      tr.close();
    }
  }


}
*/