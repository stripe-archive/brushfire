package com.stripe.brushfire
package features

trait FeatureParser[-A] {
  def parse(value: A): Map[String, FValue]
}

trait TrainingDataParser[-A, T] {
  def featureParser: FeatureParser[A]
  def parseTimestamp(a: A): Long
  def parseId(a: A): String
  def parseTarget(a: A): T
}

case class CsvRowFeatureParser(header: CsvRow) extends FeatureParser[CsvRow] {
  private val headerWithIndex: Vector[(String, Int)] = header.zipWithIndex.toVector

  def parse(row: CsvRow): Map[String, FValue] =
    headerWithIndex.map { case (feature, i) =>
      feature -> FValue.dynamic(row(i))
    } (collection.breakOut)
}

object CsvRowFeatureParser {
  def trainer(platform: TrainingPlatform)(header: CsvRow): platform.Trainer[CsvRow, FeatureParser[CsvRow]] =
    platform.Trainer.pure(CsvRowFeatureParser(header))
}

case class CsvRowTrainingDataParser(
  header: CsvRow,
  timestamp: Option[String],
  id: Seq[String],
  target: String
) extends TrainingDataParser[CsvRow, Map[String, Long]] {
  private def indexOf(tpe: String)(col: String): Int = {
    val i = header.indexOf(col)
    if (i < 0) throw new IllegalArgumentException("header missing $tpe column: $col")
    i
  }

  private val timestampIndex: Option[Int] = timestamp.map(indexOf("timestamp"))
  private val idIndices: Seq[Int] = id.map(indexOf("ID"))
  private val targetIndex: Int = indexOf("target")(target)

  val featureParser = CsvRowFeatureParser(header)

  def parseTimestamp(row: CsvRow): Long =
    timestampIndex.map(row(_)).map(_.toDouble.toLong).getOrElse(0L)

  def parseId(row: CsvRow): String =
    if (idIndices.isEmpty) {
      row.mkString(",")
    } else {
      idIndices.map(row(_)).mkString(",")
    }

  def parseTarget(row: CsvRow): Map[String, Long] =
    Map(row(targetIndex) -> 1L)
}
