/*
 * Rodeo Lib
 *
 * Copyright 2009 WorldWide Conferencing, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.goatrodeo.lib

import net.liftweb.util._
import Helpers._

import org.apache.zookeeper._
import data._
import java.io._
import scala.reflect._

@serializable
trait QBase extends Serializable {
  lazy val serialize: String = {
    val ret = new StringBuilder
    serialize(ret)
    ret.toString
  }
  def serialize(pw: StringBuilder): Unit
}

trait QBaseT[T] extends QBase {
  def is: T
}

object QBaseT {
  implicit def unbox[T](in: QBaseT[T]): T = in.is
}

object QBase {
  implicit def sToQ(in: String) = QString(in)
  implicit def bToQ(in: Boolean) = QBoolean(in)
  implicit def lToQ(in: Long) = QLong(in)
  implicit def iToQ(in: Int) = QInt(in)

  implicit def serialize(in: QBase): Array[Byte] = {
    val bas = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(bas)

    oos.writeObject(in)

    oos.flush

    bas.toByteArray
  }

  implicit def deserialize(in: Array[Byte]): Box[QBase] = {
    try {
      val bas = new ByteArrayInputStream(in)
      val oos = new ObjectInputStream(bas)

      val b = Box !! oos.readObject
      b.asA[QBase]
    } catch {
      case e: IOException => Failure(e.getMessage, Full(e), Empty)
    }
  }
}

case class QString(is: String) extends QBaseT[String] {
  def serialize(pw: StringBuilder): Unit = {
    pw.append(is.encJs)
  }
}
case class QInt(is: Int) extends QBaseT[Int]{
  def serialize(pw: StringBuilder): Unit = {
    pw.append(is)
  }
}
case class QLong(is: Long) extends QBaseT[Long]{
  def serialize(pw: StringBuilder): Unit = {
    pw.append(is)
  }
}
case class QBoolean(is: Boolean) extends QBaseT[Boolean]{
  def serialize(pw: StringBuilder): Unit = {
    pw.append(is)
  }
}
case class QByte(is: Byte) extends QBaseT[Byte]{
  def serialize(pw: StringBuilder): Unit = {
    pw.append(is)
  }
}
case class QChar(is: Char) extends QBaseT[Char]{
  def serialize(pw: StringBuilder): Unit = {
    pw.append('\'')
    pw.append(is)
    pw.append('\'')
  }
}
case class QFloat(is: Float) extends QBaseT[Float]{
  def serialize(pw: StringBuilder): Unit = {
    pw.append(is)
  }
}
case class QDouble(is: Double) extends QBaseT[Double]{
  def serialize(pw: StringBuilder): Unit = {
    pw.append(is)
  }
}
//trait QMap[K <: QBase, V <: QBase] extends Map[K, V] with QBase
//trait QList[T <: QBase] extends QBase

sealed trait TRef[T <: QBase] {
  def value: T = is

  def value_=(in: T): Unit = set(in)

  def is: T

  def set(in: T): Unit = set(Some(in))
  def set(in: Option[T]): Unit

  def get: T = is
  def apply(): T = is
  def apply(in: T) = set(in)
  def update(in: T) = set(in)
  def version: Long
}

private final class TRefImpl[T <: QBase](ref: Ref[T])(implicit mt: Manifest[T]) extends TRef[T] {
  private var _value: Option[T] = Some(Transaction.read(ref))

  def is: T = {
   _value match {
      case Some(r) => r
      case _ => val rv = ref.default
        _value = Some(rv)
        rv
    }
  }
  def set(in: Option[T]): Unit = {
    _value = in
    Transaction.write(ref.name, in)
  }

  def version: Long = Transaction.version(ref.name)
}

import net.liftweb.util._

class Ref[T <: QBase](_default: => T)(implicit mt: Manifest[T]) {
  def foreach(f: TRef[T] => Unit): Unit = {
    Transaction {
      () => f(new TRefImpl(this))
    }
  }

  def map[R](f: TRef[T] => R): Box[R] = {
    Transaction {
      () => f(new TRefImpl(this))
    }
  }
  def flatMap[R](f: TRef[T] => Box[R]): Box[R] = {
    Transaction {
      () => f(new TRefImpl(this))
    } match {
      case Empty => Empty
      case f: Failure => f
      case Full(x) => x
      case _ => Empty
    }
  }

  def prefix: Box[String] = None
  def name: String = prefix match {
    case Full(n) => "/"+n+"/"+_calcName
    case _ => "/"+_calcName
  }
  private lazy val _calcName = getClass.getName

  def default: T = _default
}

class OutsideTransactionError(msg: String) extends Error()

object Transaction extends Watcher {
  private final val baseNode = "/goat_roedo"
  private final val prepend = baseNode + "/"
  import scala.collection.mutable.HashMap
  import java.io.ObjectOutputStream
  import java.io.ByteArrayInputStream
  import java.io.ObjectInputStream
  import java.io.IOException
  //private var data: Map[String, (Long, Any)] = Map()
  private val localStore: HashMap[String, QBase] = new HashMap

  private val zkServer = {val ret = new ZKMaster; ret.init; ret}

  private val zk = try {
    val ret = new ZooKeeper("localhost:9822", 5000, this)
    
    // make sure a /goat_rodeo node exists
    try {
      ret.create(baseNode, Array(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    } catch {
      case e: KeeperException =>
    }

    ret
  } catch {case e => e.printStackTrace; throw e}

  try {
    (new CClient).doIt()
  } catch {
    case e: Exception => e.printStackTrace
  }

  private val xactDepth: ThreadGlobal[Int] = new ThreadGlobal
  // private val xaCache: ThreadGlobal[HashMap[String, Long]] = new ThreadGlobal
  private val touched: ThreadGlobal[HashMap[String, (Long, String)]] = new ThreadGlobal
  private val writeCache: ThreadGlobal[HashMap[String, (Long, Option[QBase])]] = new ThreadGlobal

  def apply[T](what: () => T): Box[T] = {
    val d = depth
    val top = d == 0
    if (top) {
      touched.set(new HashMap)
      writeCache.set(new HashMap)
    }

    val (ret, redo) =
    try {
      val r = xactDepth.doWith(d + 1){
        Full(what())
      }
      val local_redo = if (!top) false
      else !performCommit()
      /*{
       synchronized {
       val good = touched.value.elements.forall{case (n, v) =>
       data.getOrElse(n, (0L, ""))._1 == v
       }

       if (good) {
       data = xaData.value
       }
       !good
       }
       }*/

      (r, local_redo)
    } finally {
      if (top) {
        touched.set(null)
        writeCache.set(null)
      }
    }

    if (!redo) ret
    else apply(what)
  }

  def depth: Int = xactDepth.value

  //private def dataSnapshot = synchronized {data}

  def process(evt: WatchedEvent) {
    println("Got a watched event: "+evt)
  }

  import scala.collection.jcl.Conversions._

  private def getXAVersion[T <: QBase](name: String): Long = touched.value.get(name) match {
    case Some((ret, _)) => ret
    case _ => val nn = normalizeName(name)
      val baseNode = prepend+nn
      if (zk.exists(baseNode, false) eq null) {
        try {
          zk.create(baseNode, Array() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.PERSISTENT)
        } catch {
          case e: KeeperException.NodeExistsException =>
        }
      }

      import scala.collection.jcl.Conversions._

      val verNode = baseNode + "/version"

      val readNode = baseNode+"/reading-"

      val myReadNode = zk.create(readNode, Array() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL)

      def readVersion(): Long = {
        val qb: Box[QBase] = zk.getData(verNode, false, null)
        qb match {
          case Full(QLong(lng)) => lng
        }
      }

      val ret: Long = if (zk.exists(verNode, false) eq null) {
        val fetch = baseNode + "/version_fetch-"
        val myNode = zk.create(fetch, Array() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL)
        zk.getChildren(baseNode, false).filter(_.indexOf("version_fetch-") >= 0).toList.sort(_ < _) match {
          case x :: _ if myNode.endsWith(x) =>
            val data: Long = readData(nn+"-ver") match {
              case Some(QLong(x)) => x
              case _ => 0L
            }
            
            val qdata: QBase =  QLong(data)
            zk.create(verNode, qdata, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
            zk.delete(myNode, -1)
            data

            

          case n =>
            val obj = new Object
            obj.synchronized {
              zk.exists(verNode, new Watcher{
                  def process(e: WatchedEvent) {
                    obj.synchronized(obj.notify)
                  }
                })
              obj.wait
            }
            zk.delete(myNode, -1)
            readVersion()
        }
      } else readVersion()

      touched.value(name) = (ret, myReadNode)
      ret
  }

  private def normalizeName(in: String): String =
  "d"+hexEncode(in.getBytes("UTF-8"))

  private def performCommit(): Boolean = {
    var keepOn = true

    val lockList: List[String] =
    for {
      (name, _) <- touched.value.toList if keepOn
    } yield {
      val nn = normalizeName(name)
      val baseNode = prepend+nn

      val commitNode = baseNode+"/committing-"
      val cn = zk.create(commitNode, Array(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL)

      zk.getChildren(baseNode, false).filter(_.indexOf("committing-") >= 0).toList.sort(_ < _) match {
        case x :: _ if cn.endsWith(x) =>
        case _ => keepOn = false
      }

      cn
    }

    // test to make sure we've got the latest versions
    for {
      (name, (ver, _)) <- touched.value.toList if keepOn
    } yield {
      val nn = normalizeName(name)
      val baseNode = prepend+nn

      val verNode = baseNode+"/version"

      val qb: Box[QBase] = zk.getData(verNode, false, null)
      qb match {
        case Full(QLong(lng)) if ver == lng =>
        case _ => keepOn = false
      }
    }

    // it's safe to commit because we own the commit flag on all the nodes we care about
    // and we've got the latest versions
    if (keepOn) {
      for {
        (name, (ver, data)) <- writeCache.value
      } {
        val nn = normalizeName(name)
        writeData(buildName(name, ver), data)
        writeData(nn+"-ver", Some(QLong(ver)))


        val baseNode = prepend+nn

        val verNode = baseNode+"/version"

        val verQ: QBase = QLong(ver)
        zk.setData(verNode, verQ, -1)
      }
    }

    lockList.foreach(name => zk.delete(name, -1))
    touched.value.foreach{case (_, (_, name)) => zk.delete(name, -1)}
    keepOn
  }

  private def readData(name: String): Option[QBase] = synchronized {
    localStore.get(name)
  }

  private def writeData(name: String, data: Option[QBase]): Unit = synchronized {
    data match {
      case Some(x) => localStore(name) = x
      case _ => localStore -= name
    }
  }

  private def buildName(name: String, ver: Long): String =
  "d"+hexEncode(name.getBytes("UTF-8"))+"-"+ver

  // def inXAction_? = false
  private[lib] def read[T <: QBase](what: Ref[T])(implicit mt: Manifest[T]): T = depth match {
    case n if n > 0 => 
      val ver = getXAVersion(what.name)

      readData(buildName(what.name, ver)) match {
        case Some(x) => x.asInstanceOf[T]
        case _ => what.default
      }
      
    case _ => throw new OutsideTransactionError("Outside of transaction")
  }

  private[lib] def write[T <: QBase](name: String, newVal: Option[T]) {
    val ver = getXAVersion(name)
    writeCache.value(name) = (ver + 1, newVal)
  }
  private[lib] def version(what: String): Long = 0L
}
