package com.stripe.brushfire.scalding

import com.stripe.brushfire._
import com.stripe.brushfire.training._
import com.twitter.scalding._
import com.twitter.algebird.AveragedValue

abstract class CSVJob(args: Args) extends TrainerJob(args) {
  import JsonInjections._
  val cols: List[String]
  implicit val stopper = FrequencyStopper[String](10, 3)
  val error = BrierScoreError[String, Long]
  implicit val errorOrdering = Ordering[Double].on[AveragedValue] { av: AveragedValue => av.value } // Allows us to compare BrierScoreErrors.

  def trainingData: TypedPipe[Instance[String, Double, Map[String, Long]]] = {
    TypedPipe
      .from(TextLine(args("input")))
      .map { line =>
        val parts = line.split(",").reverse.toList
        val label = parts.head
        val values = parts.tail.map { s => s.toDouble }
        Instance(line, 0L, Map(cols.zip(values): _*), Map(label -> 1L))
      }
  }

  def writeTsvExecution[V](path: String): TypedPipe[V] => Execution[Unit] = {
    def fn(results: TypedPipe[V]) = { results.map { _.toString }.writeExecution(TypedTsv(path)) }
    fn
  }

  def writeValueTsvExecution[V](path: String): ValuePipe[V] => Execution[Unit] = {
    def fn(results: ValuePipe[V]) = { results.map { _.toString }.writeExecution(TypedTsv(path)) }
    fn
  }

  def train(trainingData: TypedPipe[Instance[String, Double, Map[String, Long]]]): Trainer[String, Double, Map[String, Long]] = {
    Trainer(trainingData, KFoldSampler(4))
      .expandTimes(args("output"), 3)
    //  .expandInMemory(args("output") + "/mem", 10)
      .validate(error) { writeValueTsvExecution(args("output") + "/bs") }
      .featureImportance(error) { writeTsvExecution(args("output") + "/fi") }
    //  .prune(args("output") + "/pruned", error = error)
      .validate(error) { writeValueTsvExecution(args("output") + "/pruned-bs") }
      .featureImportance(error) { writeTsvExecution(args("output") + "/pruned-fi") }
  }

  def trainer() = {
    args.optional("load") match {
      case Some(path) => Trainer(trainingData, KFoldSampler(4)).load(path)
      case None => train(trainingData)
    }
  }
}

class IrisJob(args: Args) extends CSVJob(args) {
  val cols = List("petal-width", "petal-length", "sepal-width", "sepal-length")
}

class DigitsJob(args: Args) extends CSVJob(args) {
  override implicit val stopper = FrequencyStopper[String](200, 50)
  val cols = 0.to(9).toList.map { "Feature" + _.toString }
}

