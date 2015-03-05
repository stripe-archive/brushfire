package com.stripe.brushfire
package spark

import com.twitter.algebird.{ AveragedValue, MapMonoid, Monoid }

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, SparkConf }

class TrainCSV(
    val argv: List[String])(
        val cols: String*)(implicit val stopper: Stopper[Map[String, Long]] = FrequencyStopper[String](10, 3)) extends Defaults with Serializable {
  import JsonInjections._

  implicit val targetMonoid: Monoid[Map[String, Long]] = new MapMonoid[String, Long] {
    override def sumOption(items: TraversableOnce[Map[String, Long]]): Option[Map[String, Long]] =
      super.sumOption(items).map(m => Map.empty[String, Long] ++ m)
  }

  private val error = BrierScoreError[String, Long]
  private implicit val errorOrdering: Ordering[AveragedValue] =
    Ordering[Double].on[AveragedValue](_.value)

  private def args(key: String): String = {
    val switch = s"--$key"

    def loop(as: List[String]): String = as match {
      case `switch` :: value :: _ => value
      case _ :: rest => loop(rest)
      case Nil => ???
    }

    loop(argv)
  }

  private val mkInstance: List[String] => String => Instance[String, Double, Map[String, Long]] = { cols =>
    { line =>
      val parts = line.split(",").reverse.toList
      val label = parts.head
      val values = parts.tail.map { s => s.toDouble }
      Instance(line, 0L, Map(cols.zip(values): _*), Map(label -> 1L))
    }
  }

  private def trainingData(context: SparkContext): RDD[Instance[String, Double, Map[String, Long]]] =
    context
      .textFile(args("input"))
      .map(mkInstance(cols.toList))

  def train(trainingData: RDD[Instance[String, Double, Map[String, Long]]]): Trainer[String, Double, Map[String, Long]] = {
    Trainer(trainingData, KFoldSampler(4))
      .expandTimes(3)
      .expandInMemory(10)
      .saveAsTextFile(args("output"))
  }

  def run(context: SparkContext): Unit =
    println(train(trainingData(context)).trees.count())
}

object Iris {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Iris")
    val context = new SparkContext(conf)
    new TrainCSV(args.toList)(
      "petal-width",
      "petal-length",
      "sepal-width",
      "sepal-length")
      .run(context)
  }
}

object Digits {
  implicit val stopper: Stopper[Map[String, Long]] =
    FrequencyStopper[String](200, 50)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Digits")
    val context = new SparkContext(conf)
    val cols = 0.to(9).toList.map { "Feature" + _.toString }
    new TrainCSV(args.toList)(cols: _*).run(context)
  }
}
