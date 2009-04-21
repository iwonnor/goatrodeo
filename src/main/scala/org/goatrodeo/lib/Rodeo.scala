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

// import java.io.PrintWriter

trait QBase {
  // def serialize(pw: PrintWriter): Unit
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
}

case class QString(is: String) extends QBaseT[String]
case class QInt(is: Int) extends QBaseT[Int]
case class QLong(is: Long) extends QBaseT[Long]
case class QBoolean(is: Boolean) extends QBaseT[Boolean]
case class QByte(is: Byte) extends QBaseT[Byte]
case class QChar(is: Char) extends QBaseT[Char]
case class QFloat(is: Float) extends QBaseT[Float]
case class QDouble(is: Double) extends QBaseT[Double]
trait QMap[K <: QBase, V <: QBase] extends Map[K, V] with QBase
trait QList[T <: QBase] extends QBase

class TRef[T <: QBase](ref: Ref[T]) {
  private var valSet = false
  private var _value: T = _

  def value: T = is
  
  def value_=(in: T): Unit = set(in)

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
  def get: T = is
  def apply(): T = is
  def apply(in: T) = set(in)
  def update(in: T) = set(in)
  def version: Long = Transaction.version(ref.name)
}

import net.liftweb.util._

class Ref[T <: QBase](_default: => T) {
  def foreach(f: TRef[T] => Unit): Unit = {
    Transaction {
      () => f(new TRef(this))
    }
  }
  
  def map[R](f: TRef[T] => R): Box[R] = {
    Transaction {
      () => f(new TRef(this))
    }
  }
  def flatMap[R](f: TRef[T] => Box[R]): Box[R] = {
    Transaction {
      () => f(new TRef(this))
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

object Transaction {
  import scala.collection.mutable.HashMap
  private var data: Map[String, (Long, Any)] = Map()

  private val xactDepth: ThreadGlobal[Int] = new ThreadGlobal
  private val xaData: ThreadGlobal[Map[String, (Long, Any)]] = new ThreadGlobal
  private val touched: ThreadGlobal[HashMap[String, Long]] = new ThreadGlobal

  def apply[T](what: () => T): Box[T] = {
    val d = depth
    val top = d == 0
    if (top) {
      xaData.set(dataSnapshot)
      touched.set(new HashMap)
    }

    val (ret, redo) =
    try {
      xactDepth.doWith(d + 1){
        Full(what())
      }
    } finally {
      if (top) {
        synchronized {

          data = xaData.value
        }
        xaData.set(null)
        touched.set(null)
      }
    }

  }

  private def depth: Int = xactDepth.value

  private def dataSnapshot = synchronized {data}


  // def inXAction_? = false
  private[lib] def read[T <: QBase](what: Ref[T]): T = depth match {
    case n if n > 0 => xaData.value.get(what.name) match {
        case None => 
          val ret = what.default
          xaData.set(xaData.value + what.name -> (0, ret))
          touched.value(what.name) = 0
          ret
          
        case Some((ver, value)) => 
          touched.value(what.name) = ver
          value.asInstanceOf[T]
      }
    case _ => throw new OutsideTransactionError("Outside of transaction")
  }

  private[lib] def write(what: String, who: TRef[_], newVal: Any) {
    xaData.set(xaData.value + (what -> (touched.value(what) + 1, newVal)))
  }
  private[lib] def version(what: String): Long = 0L
}
