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

package monix.eval.internal

import monix.eval.Task.{Context, Error, Now}
import monix.eval.internal.TaskRunLoop.startFull
import monix.eval.{Callback, Coeval, Task}
import monix.execution.Scheduler
import monix.execution.atomic.Atomic
import monix.execution.cancelables.StackedCancelable
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure, Success, Try}


private[eval] object TaskMemoize {
  /**
    * Implementation for `.memoize` and `.memoizeOnSuccess`.
    */
  def apply[A](source: Task[A], cacheErrors: Boolean): Task[A] =
    source match {
      case Now(_) | Error(_) =>
        source
      case Task.Eval(Coeval.Suspend(f: LazyVal[A @unchecked]))
        if !cacheErrors || f.cacheErrors =>
        source
      case Task.Async(r: Register[A] @unchecked)
        if !cacheErrors || r.cacheErrors =>
        source
      case _ =>
        Task.Async(new Register(source, cacheErrors))
    }

  /** Registration function, used in `Task.Async`. */
  private final class Register[A](source: Task[A], val cacheErrors: Boolean)
    extends ((Task.Context, Callback[A]) => Unit) { self =>

    // N.B. keeps state!
    private[this] var thunk = source
    private[this] val state = Atomic(null : AnyRef)

    def apply(ctx: Context, cb: Callback[A]): Unit = {
      implicit val sc = ctx.scheduler
      state.get match {
        case result: Try[A] @unchecked =>
          cb.asyncApply(result)
        case _ =>
          start(ctx, cb)
      }
    }

    /** Saves the final result on completion and triggers the registered
      * listeners.
      */
    @tailrec def cacheValue(value: Try[A])(implicit sc: Scheduler): Unit = {
      // Should we cache everything, error results as well,
      // or only successful results?
      if (self.cacheErrors || value.isSuccess) {
        state.getAndSet(value) match {
          case (p: Promise[A] @unchecked, _) =>
            if (!p.tryComplete(value)) {
              // $COVERAGE-OFF$
              if (value.isFailure)
                sc.reportFailure(value.failed.get)
              // $COVERAGE-ON$
            }
          case _ =>
            () // do nothing
        }
        // GC purposes
        self.thunk = null
      } else {
        // Error happened and we are not caching errors!
        state.get match {
          case current @ (p: Promise[A] @unchecked, _) =>
            // Resetting the state to `null` will trigger the
            // execution again on next `runAsync`
            if (state.compareAndSet(current, null)) {
              p.tryComplete(value)
            } else {
              // Race condition, retry
              // $COVERAGE-OFF$
              cacheValue(value)
              // $COVERAGE-ON$
            }
          case _ =>
            // $COVERAGE-OFF$
            () // Do nothing, as value is probably null already
            // $COVERAGE-ON$
        }
      }
    }

    /** Builds a callback that gets used to cache the result. */
    private def complete(implicit sc: Scheduler): Callback[A] =
      new Callback[A] {
        def onSuccess(value: A): Unit =
          self.cacheValue(Success(value))
        def onError(ex: Throwable): Unit =
          self.cacheValue(Failure(ex))
      }

    /** While the task is pending completion, registers a new listener
      * that will receive the result once the task is complete.
      */
    private def registerListener(
      ref: (Promise[A], StackedCancelable),
      context: Context,
      cb: Callback[A])
      (implicit ec: ExecutionContext): Unit = {

      val (p, c) = ref
      context.connection.push(c)
      p.future.onComplete { r =>
        context.connection.pop()
        context.frameRef.reset()
        startFull(Task.fromTry(r), context, cb, null, null, null, 1)
      }
    }

    /**
      * Starts execution, eventually caching the value on completion.
      */
    @tailrec private def start(context: Context, cb: Callback[A]): Unit = {
      implicit val sc: Scheduler = context.scheduler
      self.state.get match {
        case null =>
          val update = (Promise[A](), context.connection)

          if (!self.state.compareAndSet(null, update)) {
            // $COVERAGE-OFF$
            start(context, cb) // retry
            // $COVERAGE-ON$
          } else {
            self.registerListener(update, context, cb)
            // With light async boundary to prevent stack-overflows!
            Task.unsafeStartTrampolined(self.thunk, context, self.complete)
          }

        case ref: (Promise[A], StackedCancelable) @unchecked =>
          self.registerListener(ref, context, cb)

        case ref: Try[A] @unchecked =>
          // Race condition happened
          // $COVERAGE-OFF$
          cb.asyncApply(ref)
          // $COVERAGE-ON$
      }
    }
  }
}
