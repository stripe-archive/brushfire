package com.stripe.brushfire
package scalding
package features

import com.stripe.brushfire.features._

import com.twitter.algebird._
import com.twitter.scalding._

object ScaldingTrainingPlatform extends ContextlessTrainingPlatform {
  case class TypedPipeExecutor[A](pipe: TypedPipe[A]) extends Executor[Execution, A] {
    def aggregate[B: AggregateContext, C](agg: Aggregator[A, B, C]): Execution[C] =
      pipe.aggregate(agg).getExecution
  }
}
