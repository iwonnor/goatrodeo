/*
 * ZKMaster.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.goatrodeo.lib

import org.apache.zookeeper.server.quorum._

class ZKMaster(workingDir: String) extends QuorumPeerMain {
  def init {
    (new Thread(new Runnable {
          def run {
            ZKMaster.this.initializeAndRun(Array("9822", workingDir))
          }
        }, "Zookeeper")).start()

    Thread.sleep(500)
  }
}
