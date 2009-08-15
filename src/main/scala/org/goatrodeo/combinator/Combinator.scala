/*
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


package org.goatrodeo.combinator

import util._
import HLists._

import java.util.concurrent._

object CombinatorRules {
  var threadPoolCreator: () => Executor = () => Executors.newCachedThreadPool()

  lazy val executor = threadPoolCreator()

  var executeStrategy: Function1[Function0[Unit], Unit] = 
  f => executor.execute(new Runnable{def run() {f.apply()}})
}

/**
 *  Describe complex business flow via a Combinator
 */
object Combinator {

  trait Execable {
    protected def execute(f: () => Unit): Unit
  }

  trait Block[In, Out] extends Execable {
    
    def %>[NewOut](that: Block[Out, NewOut]): Block[In, NewOut] = {
      val self = this
      new Block[In, NewOut] {
        protected def apply(in: In): Nothing = throw new Exception("Huh?")
        override def managedApply(param: In, andThen: NewOut => Unit): Unit = {
          self.execute{
            () => {
              val res: Out = self.apply(param)
              that.managedApply(res, andThen)
            }
          }
        }
      }
    }

    protected def apply(in: In): Out

    protected def execute(f: () => Unit): Unit = CombinatorRules.executeStrategy(f)

    def managedApply(param: In, andThen: Out => Unit): Unit = {
      execute{
        () => andThen(apply(param))

      }
    }

  }

  trait LocalExecBlock extends Execable {
    self: Block[_, _] =>

    abstract override protected def execute(f: () => Unit): Unit = f.apply()
  }


  trait Strict {
    self: Block[_, _] =>
  }

  trait Lazy {
    self: Block[_, _] =>
  }

  object Transform {
    def apply[In, Out](f: In => Out): Block[In, Out] = new Block[In, Out] {
      protected def apply(in: In): Out = f(in)
    }
  }

  object TransformLocal {
    def apply[In, Out](f: In => Out): Block[In, Out] = new Block[In, Out] with LocalExecBlock {
      protected def apply(in: In): Out = f(in)
    }
  }

  trait Distributor[T, In <: T :: HList, Out, Distributee] extends Block[In, Seq[Out]] {
    protected def findDistributees(in: T): Seq[Distributee]

    protected def apply(in: In): Nothing = throw new Exception("Huh?")
    override def managedApply(param: In, andThen: Seq[Out] => Unit): Unit = {
      execute{
        () => {
          val toInvoke = findDistributees(param.head)
          val cnt = toInvoke.length
          var answered = 0
          val ret: Array[Out] = new Array(cnt)
          val sync = new Object

          toInvoke.toList.zipWithIndex.foreach {
            case (d, idx) => block.managedApply(HCons(d, param), answer => {
                  val send = sync.synchronized{
                    ret(idx) = answer
                    answered += 1
                    answered >= cnt
                  }
                  if (send) andThen(ret)
                })
          }

        }
      }
    }
    
    protected def block: Block[Distributee :: In, Out]

    def to(blk: Block[Distributee :: In, Out]): Distributor[T, In, Out, Distributee] = {
      val self = this

      new Distributor[T, In, Out,  Distributee] {
        protected def findDistributees(in: T): Seq[Distributee] = self.findDistributees(in)
        protected def block: Block[Distributee :: In, Out] = blk
      }
    }

  }

  trait Persister[In] extends Block[In, Unit]

  trait Fork[In, Out1, Out2] extends Block[In, (Out1, Out2)] {

  }

  class Tee[In, Out](left: Block[In, Out], right: Block[In, Out]) extends Block[In, Out] {
    protected def apply(in: In): Nothing = throw new Exception("Huh?")
    override def managedApply(param: In, andThen: Out => Unit): Unit = {
      execute {
        () => left.managedApply(param, _ => ())
      }
      execute{
        () => right.managedApply(param, andThen)

      }
    }
  }


}
