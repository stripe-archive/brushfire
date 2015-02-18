package com.stripe.brushfire.scalding

import com.stripe.brushfire._
import com.twitter.scalding._
import com.twitter.algebird.AveragedValue

abstract class csvJob(args: Args) extends TrainerJob(args) {
  import JsonInjections._
  val cols: List[String]
  implicit val stopper = FrequencyStopper[String](10, 3)
  val error = BrierScoreError[String, Long]
  implicit val errorOrdering = Ordering.by[AveragedValue, Double] { av => av.value } // Allows us to compare BrierScoreErrors.

  def load_data(): TypedPipe[Instance[String, Double, Map[String, Long]]] = {
    TypedPipe
      .from(TextLine(args("input")))
      .map { line =>
        val parts = line.split(",").reverse.toList
        val label = parts.head
        val values = parts.tail.map { s => s.toDouble }
        Instance(line, 0L, Map(cols.zip(values): _*), Map(label -> 1L))
      }
  }

  def train(trainingData: TypedPipe[Instance[String, Double, Map[String, Long]]]): Trainer[String, Double, Map[String, Long]] = {
    val trainer = Trainer(trainingData, KFoldSampler(4))
      .expandTimes(args("output"), 3)
      .expandInMemory(args("output") + "/mem", 10)

    trainer
      .validate(error) { results =>
        results.map {
          _.toString
        }.writeExecution(TypedTsv(args("output") + "/bs"))
      }

    trainer
      .featureImportance(error) { results =>
        results.map {
          _.toString
        }.writeExecution(TypedTsv(args("output") + "/fi"))
      }
    trainer
  }

  val trainingData = load_data()

  def trainer() = {
    args.optional("load") match {
      case Some(path) => Trainer(trainingData, KFoldSampler(4)).load(path).prune(args("output") + "/pruned", error = error)
      case None => train(trainingData)
    }
  }
}

class IrisJob(args: Args) extends csvJob(args) {
  val cols = List("petal-width", "petal-length", "sepal-width", "sepal-length")
}

class DigitsJob(args: Args) extends csvJob(args) {
  override implicit val stopper = FrequencyStopper[String](200, 100)
  val cols = 0.to(9).toList.map { "Feature" + _.toString }
}

