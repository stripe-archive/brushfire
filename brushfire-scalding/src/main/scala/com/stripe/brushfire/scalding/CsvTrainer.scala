package com.stripe.brushfire
package scalding

import com.stripe.brushfire.features._

import com.twitter.scalding._
import com.twitter.algebird.AveragedValue

class CsvTrainerJob(args: Args) extends TrainerJob(args) {
  import JsonInjections._

  implicit val stopper = FrequencyStopper[String](10, 3)
  implicit val errorOrdering = Ordering[Double].on[AveragedValue] { av: AveragedValue => av.value } // Allows us to compare BrierScoreErrors.

  val error = BrierScoreError[String, Long]

  def splitRow(row: String): CsvRow = row.split(",")

  val header: CsvRow = splitRow(args("features"))
  val labelFeature: String = args("label")
  val labelIndex: Int = header.indexOf(labelFeature)
  val features: CsvRow = header.filter(_ != labelFeature)

  val rows: TypedPipe[CsvRow] =
    TypedPipe
      .from(TextLine(args("input")))
      .map(splitRow)

  def guessFeatureMapping(header: CsvRow, rows: TypedPipe[CsvRow]): Execution[FeatureMapping[CsvRow]] =
    rows
      .aggregate(CsvRowFeatureMapping.aggregator(header))
      .toIterableExecution
      .map(_.head)

  val trainingData: Execution[TypedPipe[Instance[String, FeatureValue, Map[String, Long]]]] =
    guessFeatureMapping(header, rows).map { mapping =>
      rows.map { row =>
        val label: String = row(labelIndex)
        val fv: Map[String, FeatureValue] = features.map { key =>
          key -> mapping.extract(key, row)
        } (collection.breakOut)

        Instance(row.mkString(","), 0L, fv, Map(label -> 1L))
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

  def train(trainingData: Execution[TypedPipe[Instance[String, FeatureValue, Map[String, Long]]]]): Trainer[String, FeatureValue, Map[String, Long]] = {
    Trainer.fromExecution(trainingData, KFoldSampler(4))
      .expandTimes(args("output"), 3)
      .expandInMemory(args("output") + "/mem", 10)
      .validate(error) { writeValueTsvExecution(args("output") + "/bs") }
      .featureImportance(error) { writeTsvExecution(args("output") + "/fi") }
      .prune(args("output") + "/pruned", error = error)
      .validate(error) { writeValueTsvExecution(args("output") + "/pruned-bs") }
      .featureImportance(error) { writeTsvExecution(args("output") + "/pruned-fi") }
  }

  def trainer() = {
    args.optional("load") match {
      case Some(path) => Trainer.fromExecution(trainingData, KFoldSampler(4)).load(path).prune(args("output") + "/pruned", error = error)
      case None => train(trainingData)
    }
  }
}
