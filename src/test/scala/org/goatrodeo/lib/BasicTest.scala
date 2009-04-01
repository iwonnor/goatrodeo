/*
 * Copyright 2007-2008 WorldWide Conferencing, LLC
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

class BasicTest extends Runner(BasicSpec) with JUnit with Console

object LongRef extends Ref[QLong](0L)

object BasicSpec extends Specification {
  "A Ref" can {
    "Contain a value" in {
      for (lr <- LongRef) {
      println("lr is "+lr.is)

      lr.is.is mustBe 0L
      }
    }
  }
}
