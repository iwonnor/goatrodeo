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

import _root_.org.specs._
import _root_.org.specs.runner._
import _root_.org.specs.Sugar._

import net.liftweb.util._

class ComboTest extends Runner(ComboSpec) with JUnit with Console

import Combinator._


object ComboSpec extends Specification {
  "A Combo" can {
    "Perform a calculation" in {
      var result = -1
      val mult = Combinator.TransformLocal[Int, Int](_ * 2)
      mult.managedApply(4, result = _)
      result must_== 8
    }

    "Perform a chained calculation" in {
      var result = -1
      val mult = Combinator.TransformLocal[Int, Int](_ * 2)
      val byFour = mult %> mult
      byFour.managedApply(4, result = _)
      result must_== 16
    }

    "Perform a multi-threaded chained calculation" in {
      var result = -1
      val sync = new Object
      var done = false
      val mult = Combinator.Transform[Int, Int](_ * 2)
      val byFour = mult %> mult
      byFour.managedApply(4, v => {result = v; sync.synchronized{done = true; sync.notify}})
      val earlyResult = result
      sync.synchronized {
        while (!done) {
          sync.wait
        }
      }
      earlyResult must_== -1
      result must_== 16
    }
  }
}
