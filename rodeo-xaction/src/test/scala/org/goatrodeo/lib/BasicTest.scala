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
package org.goatrodeo.lib

import _root_.org.specs._
import _root_.org.specs.runner._
import _root_.org.specs.Sugar._

import net.liftweb.util._
import net.liftweb.common._

class BasicTest extends Runner(BasicSpec) with JUnit with Console

object LongRef extends Ref[QLong](0L)

object AnotherRef extends Ref[QInt](55)

object BasicSpec extends Specification {
  "A Ref" can {
    "Contain a value" in {
      for (lr <- LongRef) {

        lr.value mustBe 0L
      }
    }

    "Be changable" in {
      for (lr <- LongRef) {
        lr.value = 4L
      }

      for (lr <- LongRef) {
        lr.value mustBe 4L
      }
    }

    "be nestable" in {

      val res = for {
        lr <- LongRef
        ir <- AnotherRef
      } yield {
        val old: Long = lr.value
        val oldInt: Int = ir.value
        ir.value = 33
        lr.value = 18L
        old
      }

      res must_== Full(4L)

      for {
        lr <- LongRef
        ir <- AnotherRef
      } {
        ir.value mustBe 33
        lr.value mustBe 18L
      }

    }

    "Must retry if another thread changes things" in {
      var cnt = 0
      val foo = "Hello"
      var done = false

      for (lr <- LongRef) {
        // Note this is a side effect
        // This is *very bad*
        if (cnt == 0) {
          (new Thread(new Runnable {
                def run() {
                  for (lr2 <- LongRef) {
                    lr2.value = 97L
                  }

                  foo.synchronized {
                    done = true
                    foo.notifyAll
                  }
                }
              })).start
          foo.synchronized{
            while (!done) foo.wait
          }
        }

        cnt = cnt + 1
        lr.value = 89L
      }

      import net.liftweb.util._

      val res: Box[Long] = for (lr <- LongRef) yield lr.value

      res.open_! mustBe 89L
      cnt must be_>=(2)
    }
  }
}
