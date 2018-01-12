/*
 * Copyright (c) 2014-2018 by The Monix Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.eval

import minitest.SimpleTestSuite
import monix.execution.Scheduler

object TaskLocalSuite extends SimpleTestSuite {
  implicit val ec: Scheduler = monix.execution.Scheduler.Implicits.global
  implicit val opts = Task.defaultOptions.enableLocalContextPropagation

  testAsync("Local.apply") {
    val local = TaskLocal(0)
    val test =
      for {
        v1 <- local.read
        _ <- Task.now(assertEquals(v1, 0))
        _ <- local.write(100)
        _ <- Task.shift
        v2 <- local.read
        _ <- Task.now(assertEquals(v2, 100))
        _ <- local.clear
        _ <- Task.shift
        v3 <- local.read
        _ <- Task.now(assertEquals(v3, 0))
      } yield ()

    test.runAsyncOpt
  }

  testAsync("Local.defaultLazy") {
    var i = 0
    val local = TaskLocal.lazyDefault(Coeval { i += 1; i })

    val test =
      for {
        v1 <- local.read
        _ <- Task.now(assertEquals(v1, 1))
        _ <- local.write(100)
        _ <- Task.shift
        v2 <- local.read
        _ <- Task.now(assertEquals(v2, 100))
        _ <- local.clear
        _ <- Task.shift
        v3 <- local.read
        _ <- Task.now(assertEquals(v3, 2))
      } yield ()

    test.runAsyncOpt
  }


  testAsync("TaskLocal!.bind") {
    val local = TaskLocal(0)
    val test =
      for {
        _ <- local.write(100)
        _ <- Task.shift
        v1 <- local.bind(200)(local.read.map(_ * 2))
        _ <- Task.now(assertEquals(v1, 400))
        v2 <- local.read
        _ <- Task.now(assertEquals(v2, 100))
      } yield ()

    test.runAsyncOpt
  }

  testAsync("TaskLocal!.bindL") {
    val local = TaskLocal(0)
    val test =
      for {
        _ <- local.write(100)
        _ <- Task.shift
        v1 <- local.bindL(Task.eval(200))(local.read.map(_ * 2))
        _ <- Task.now(assertEquals(v1, 400))
        v2 <- local.read
        _ <- Task.now(assertEquals(v2, 100))
      } yield ()

    test.runAsyncOpt
  }

  testAsync("TaskLocal!.bindClear") {
    val local = TaskLocal(200)
    val test =
      for {
        _ <- local.write(100)
        _ <- Task.shift
        v1 <- local.bindClear(local.read.map(_ * 2))
        _ <- Task.now(assertEquals(v1, 400))
        v2 <- local.read
        _ <- Task.now(assertEquals(v2, 100))
      } yield ()

    test.runAsyncOpt
  }

  testAsync("TaskLocal.bind clears local with no async boundary") {
    val local = TaskLocal(0)

    def add(i: Int) = {
      local.read.map(_ + i)
    }

    val res = for {
      _ <- local.read.map(assertEquals(_, 0))
      _ <- local.bind(1)(add(1)).map(assertEquals(_, 3))
      _ <- local.read.map(assertEquals(_, 0))
    } yield ()

    // Works without the runAsyncOpt because it is not
    // jumping to an async boundary
    // When taking into consideration local vars, always
    // use runAsyncOpt. This is just for illustration purposes
    res.runAsync

  }

  testAsync("TaskLocal.bind clears local in forked thread") {
    val local = TaskLocal(0)

    def add(i: Int) = {
      local.read.map(_ + i)
    }

    val res = for {
      _ <- local.read.map(assertEquals(_, 0))
      _ <- local.bind(1)(add(1).executeWithFork).map(assertEquals(_, 2))
      _ <- local.read.map(assertEquals(_, 0))
    } yield ()

    // Must use runAsyncOpt because of async boundary
    // Introduced by forked task.
    res.runAsyncOpt

  }

  test("TaskLocal.bind clears current local in current thread") {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val local = TaskLocal(0)

    def add(i: Int) = {
      local.read.map(_ + i)
    }

    //Basically we do the same checks as before but blocking
    //to explicitly check the status of the local in the
    //current execution thread

    assertEquals(local.read.coeval.value, Right(0))

    val res = Await.result(local.bind(1)(add(1)).runAsyncOpt, 5.seconds)

    assertEquals(res, 2)

    //clears the local as expected
    assertEquals(local.read.coeval.value, Right(0))

  }

  test("TaskLocal.bind does not clear current local in forked thread") {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val local = TaskLocal(0)

    def add(i: Int) = {
      local.read.map(_ + i)
    }

    //We do the current local var
    assertEquals(local.read.coeval.value, Right(0))

    //Bind clears the local in the forked thread
    //Which means it doesn't do the proper cleanup
    val res = Await.result(local.bind(1)(add(1).executeWithFork).runAsyncOpt, 5.seconds)

    assertEquals(res, 2)

    // And leaves the current local var with 1
    assertEquals(local.read.coeval.value, Right(1))


  }
}
