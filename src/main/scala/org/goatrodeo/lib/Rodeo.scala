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

  def set(in: T): Unit

  def get: T = is
  def apply(): T = is
  def apply(in: T) = set(in)
  def update(in: T) = set(in)
  def version: Long
}

private final class TRefImpl[T <: QBase](ref: Ref[T])(implicit mt: Manifest[T]) extends TRef[T] {
  private var valSet = false
  private var _value: T = _

  def is: T = {
    if (valSet) _value
    else {
      _value = Transaction.read(ref)
      _value
    }
  }
  def set(in: T): Unit = {
    _value = in
    Transaction.write(ref.name, this, in)
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
  private val localStore: HashMap[String, (Long, Option[QBase])] = new HashMap

  println("Hello")
  private val zkServer = {val ret = new ZKMaster; ret.init; ret}

  println("Dog")

  private val zk = try {
    val ret = new ZooKeeper("localhost:9822", 5000, this)
    try {
      ret.create(baseNode, Array(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    } catch {
      case e: KeeperException =>
    }
    ret
  } catch {case e => e.printStackTrace; throw e}

  println("Howdy")

  try {
    val toSer: QBase = 88

    println("Added: "+zk.create("/hello_world-", toSer , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL))

    val ser: Array[Byte] = toSer
    val in: Box[QBase] = ser
    println("In is "+in)
  } catch {
    case e => e.printStackTrace
  }

  private val xactDepth: ThreadGlobal[Int] = new ThreadGlobal
  private val xaCache: ThreadGlobal[HashMap[String, (Long, Option[QBase])]] = new ThreadGlobal
  private val touched: ThreadGlobal[HashMap[String, Long]] = new ThreadGlobal

  def apply[T](what: () => T): Box[T] = {
    val d = depth
    val top = d == 0
    if (top) {
      xaCache.set(new HashMap)
      touched.set(new HashMap)
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
        xaCache.set(null)
        touched.set(null)
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

  private def getXAVersion[T <: QBase](name: String, default: () => T): (Long, T) = xaCache.value.get(name) match {
    case Some(ret) => ret
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

      println("verNode "+verNode)

      def printRes[T](in: T): T = {
        println("PrintRes "+in)
        in
      }

      if (printRes(zk.exists(verNode, false)) eq null) {
        println("We're fetching")
        val fetch = baseNode + "/version_fetch-"
        val myNode = zk.create(fetch, Array() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL)
        zk.getChildren(baseNode, false).filter(_.indexOf("version_fetch-") >= 0).toList.sort(_ > _) match {
          case x :: _ if myNode.endsWith(x) => println("Fetching "+myNode) // we fetch
            val data = readData(name)
          case n => println("N is "+n+" and myNode is "+myNode) // we wait
            System.exit(0)
        }

        ()
      }

    try {
      zk.getData(prepend+nn, false, null)
    } catch {
      case e: KeeperException.NoNodeException => try {
          // zk.create(prepend+nn, array, list, createmode, stringcallback, any)
      }
    }
    null
  }

  private def normalizeName(in: String): String =
  "d"+hexEncode(in.getBytes("UTF-8"))

  private def performCommit(): Boolean = false

  private def readData(name: String): Box[(Long, Option[QBase])] = synchronized {
    localStore.get(name) match {
      case Some(x: QBase) => Full(x)
      case _ => Empty
    }
  }

  private def writeData(name: String, data: (Long, Option[QBase])): Unit = synchronized {
    localStore(name) = data
  }

  // def inXAction_? = false
  private[lib] def read[T <: QBase](what: Ref[T])(implicit mt: Manifest[T]): T = depth match {
    case n if n > 0 => getXAVersion(what.name) match {
        /*case None =>
          val ret = what.default
          // xaData.set(xaData.value + what.name -> (0, ret))
          touched.value(what.name) = 0
          ret
*/
        case (ver, Some(value)) =>
          touched.value(what.name) = ver
          value.asInstanceOf[T]

         case (ver, _) =>
           touched.value(what.name) = ver

      }
    case _ => throw new OutsideTransactionError("Outside of transaction")
  }

  private[lib] def write(what: String, who: TRef[_], newVal: Any) {
    if (!touched.value.contains(what)) {
      touched.value(what) = getXAVersion(what).map(_._1) getOrElse 0L // xaData.value.getOrElse(what, (0L, ""))._1
    }

    //xaData.set(xaData.value + (what -> (touched.value(what) + 1, newVal)))
  }
  private[lib] def version(what: String): Long = 0L
}
