package com.stripe.brushfire
package scalding

import com.stripe.brushfire.features._
import com.stripe.brushfire.scalding.features._

import com.twitter.scalding._
import com.twitter.algebird.AveragedValue

class CsvTrainerJob(args: Args) extends ExecutionJob[Unit](args) with Defaults {
  import JsonInjections._

  implicit val stopper = FrequencyStopper[String](10, 3)
  implicit val errorOrdering = Ordering[Double].on[AveragedValue] { av: AveragedValue => av.value } // Allows us to compare BrierScoreErrors.

  val error = BrierScoreError[String, Long]

  def splitRow(row: String): CsvRow = row.split(",").map(_.trim)

  val columns: CsvRow = splitRow(args("columns"))
  val label: String = args("label")
  val timestamp: Option[String] = args.optional("timestamp")
  val id: Seq[String] = args.optional("id")
    .map(splitRow)
    .getOrElse(Nil)
  val features: Set[String] = args.optional("features")
    .map(splitRow)
    .map(_.toSet)
    .getOrElse(columns.toSet - label)

  val trainingDataParser: TrainingDataParser[CsvRow, Map[String, Long]] =
    CsvRowTrainingDataParser(columns, timestamp, id, label)

  val rows: TypedPipe[CsvRow] =
    TypedPipe
      .from(TextLine(args("input")))
      .map(splitRow)

  val featureEncoding: Execution[DispatchedFeatureEncoding[String]] =
    DispatchedFeatureEncoding
      .trainer[String](ScaldingTrainingPlatform)
      .contramap[CsvRow](trainingDataParser.featureParser.parse(_))
      .run(ScaldingTrainingPlatform.TypedPipeExecutor(rows))

  val trainingData: Execution[TypedPipe[Instance[String, FeatureValue, Map[String, Long]]]] =
    featureEncoding.map { encoding =>
      rows.flatMap { row =>
        val rawFeatures = trainingDataParser.featureParser.parse(row)
        for {
          features <- encoding.encoder.encode(rawFeatures.filterKeys(features)).toOption
        } yield {
          val target    = trainingDataParser.parseTarget(row)
          val id        = trainingDataParser.parseId(row)
          val timestamp = trainingDataParser.parseTimestamp(row)
          Instance(id, timestamp, features, target)
        }
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

  def train(
    trainingData: Execution[TypedPipe[Instance[String, FeatureValue, Map[String, Long]]]]
  )(implicit
    splitter: Splitter[FeatureValue, Map[String, Long]]
  ): Trainer[String, FeatureValue, Map[String, Long]] = {
    Trainer.fromExecution(trainingData, KFoldSampler(4))
      .expandTimes(args("output"), 3)
      .expandInMemory(args("output") + "/mem", 10)
      .validate(error) { writeValueTsvExecution(args("output") + "/bs") }
      .featureImportance(error) { writeTsvExecution(args("output") + "/fi") }
      .prune(args("output") + "/pruned", error = error)
      .validate(error) { writeValueTsvExecution(args("output") + "/pruned-bs") }
      .featureImportance(error) { writeTsvExecution(args("output") + "/pruned-fi") }
  }

  def execution: Execution[Unit] = for {
    encoding <- featureEncoding
    _ <- train(trainingData)(encoding.splitter).execution
  } yield ()
}
