package com.stripe.brushfire.training

import com.stripe.brushfire._
import com.twitter.algebird._

/**
 * FrequencyError sets up the most common case when dealing
 * with discrete distributions:
 * - compute and sum errors separately for each component of the actual distribution
 * - provide a zero for when predictions or actuals are missing
 */
trait FrequencyError[L, M, E] extends Error[Map[L, M], Map[L, Double], E] {

  val semigroup = monoid

  def monoid: Monoid[E]

  def create(actual: Map[L, M], predicted: Map[L, Double]) = {
    if (predicted.isEmpty)
      monoid.zero
    else {
      monoid.sum(actual.map { case (label, count) => error(label, count, predicted) })
    }
  }

  def error(label: L, count: M, probabilities: Map[L, Double]): E
}

case class BrierScoreError[L, M](implicit num: Numeric[M])
    extends FrequencyError[L, M, AveragedValue] {
  lazy val monoid = AveragedValue.group

  def error(label: L, count: M, probabilities: Map[L, Double]): AveragedValue = {
    val differences = Group.minus(Map(label -> 1.0), probabilities)
    val sumSquareDifferences = differences.values.map { math.pow(_, 2) }.sum
    AveragedValue(num.toLong(count), sumSquareDifferences / math.max(differences.size, 1L))
  }
}

case class BinnedBinaryError[M: Monoid]()
    extends FrequencyError[Boolean, M, Map[Int, (M, M)]] {
  lazy val monoid = implicitly[Monoid[Map[Int, (M, M)]]]

  private def percentage(p: Double) = (p * 100).toInt

  def error(label: Boolean, count: M, probabilities: Map[Boolean, Double]) = {
    val tuple = if (label) (count, Monoid.zero[M]) else (Monoid.zero[M], count)
    Map(percentage(probabilities.getOrElse(true, 0.0)) -> tuple)
  }

  def thresholds(err: Map[Int, (M,M)])(implicit num: Numeric[M]): List[(Int, ConfusionMatrix)] =
    err.keys.toList.sorted.map{threshold =>
      threshold -> ConfusionMatrix(
        err.filter{_._1 >= threshold}.map{x => num.toDouble(x._2._1)}.sum,
        err.filter{_._1 < threshold}.map{x => num.toDouble(x._2._2)}.sum,
        err.filter{_._1 >= threshold}.map{x => num.toDouble(x._2._2)}.sum,
        err.filter{_._1 < threshold}.map{x => num.toDouble(x._2._1)}.sum)
    }

  def auc(err: Map[Int, (M, M)])(implicit num: Numeric[M]) =
    thresholds(err).map{_._2}.reverse.sliding(2,1).map{cms =>
      val cm1 = cms(0)
      val cm2 = cms(1)
      (cm2.falsePositiveRate - cm1.falsePositiveRate) *
      (cm1.truePositiveRate + cm2.truePositiveRate)
    }.sum / 2.0
}

case class AccuracyError[L, M](implicit m: Monoid[M])
    extends FrequencyError[L, M, (M, M)] {

  lazy val monoid = implicitly[Monoid[(M, M)]]

  def error(label: L, count: M, probabilities: Map[L, Double]) = {
    val mode = probabilities.maxBy { _._2 }._1
    if (mode == label)
      (count, m.zero)
    else
      (m.zero, count)
  }
}
