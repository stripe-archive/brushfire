package com.stripe.brushfire.scalding

import com.stripe.brushfire._
import com.twitter.scalding._

class IrisJob(args: Args) extends TrainerJob(args) {
  import KryoInjections._

  val cols = List("petal-width", "petal-length", "sepal-width", "sepal-length")

  val trainingData =
    TypedPipe
      .from(TextLine(args("input")))
      .map { line =>
        val parts = line.split(",").reverse.toList
        val label = parts.head
        val values = parts.tail.map { s => s.toDouble }
        Instance(line, 0L, Map(cols.zip(values): _*), Map(label -> 1L))
      }

  implicit val stopper = FrequencyStopper[String](10, 3)
  val trainer =
    Trainer(trainingData, KFoldSampler(4))
      .expandTimes(args("output"), 3)
      .expandSmallNodes(args("output") + "/mem", 10)
      .featureImportance(BrierScoreError[String]) { results =>
        results.map { case (k, v) => (k, v.value) }.writeExecution(TypedTsv(args("output") + "/fi"))
      }
}
