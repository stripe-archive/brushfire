package com.stripe.brushfire

import com.twitter.algebird._

case class BinnedError[B, T: Monoid](binner: Iterable[T] => B) extends Error[T, Map[B, T]] {
  val semigroup = implicitly[Semigroup[Map[B, T]]]

  def create(actual: T, predicted: Iterable[T]) = Map(binner(predicted) -> actual)
}

object Errors {
  def averageProbability(predicted: Iterable[Map[Boolean, Long]]): Double = {
    if (predicted.size == 0)
      0.0
    else {
      val scores = predicted.map { m =>
        val trues = math.max(m.getOrElse(true, 0L).toDouble, 0.0)
        val falses = math.max(m.getOrElse(false, 0L).toDouble, 0.0)
        if (trues == 0.0)
          0.0
        else
          trues / (falses + trues)
      }
      scores.sum / scores.size
    }
  }

  def averagePercentage(predicted: Iterable[Map[Boolean, Long]]): Double =
    Math.floor(averageProbability(predicted) * 100) / 100.0
}

/**
 * BrierScore is a container for tracking the Brier score decomposition of a set of errors.
 *
 * See http://www.eumetcal.org/intralibrary/open_virtual_file_path/i2055n15861t/english/msg/ver_prob_forec/uos2/uos2_ko2.htm for more information.
 *
 * @tparam A the classes to be predicted into.
 * @tparam B the probability distribution bins.
 * @param predicted every time an example is falls into a (predicted class, predicted probability bin), that probability is added here.
 * @param actual counts the number of examples per (predicted class, predicted probability bin) that actually fall into the predicted class.
 * @param counts the total number of examples in each (predicted class, predicted probability bin).
 * @param totalCount the total number of examples.
 */
case class BrierScore[A, B](predicted: Map[(A, B), Double], actual: Map[(A, B), Long], counts: Map[(A, B), Long], totalCount: Long) {

  /**
   * classRates contains the percentage of examples that fall into each class.
   */
  def actualClassRates: Map[A, Double] = {
    val actualClassCounts = actual.groupBy { case (ab, ct) => ab._1 }.mapValues { _.map(_._2).sum }

    actualClassCounts.mapValues { _ / math.max(totalCount.toDouble, 1.0) }
  }

  /**
   * Reliability measures how close forecast probabilities are to the true probabilities, and is defined as
   *
   *   reliability = 1/(total # of examples) * sum_{class a} sum_{bin b} [
   *     (# of examples that are predicted to fall into class a with probability in bin b) *
   *       (predicted probability - % of these examples that actually fall into bin b)^2 ]
   */
  def reliability: Double = {
    if (totalCount == 0) {
      0.0
    } else {
      val sqDiffs = Group.minus(predicted, actual.mapValues { _.toDouble }).mapValues { math.pow(_, 2) }
      val inverseCounts = counts.filter { _._2 > 0.0 }.mapValues { 1.0 / _ }
      val weightedSum = Ring.times(sqDiffs, inverseCounts).values.sum

      weightedSum / totalCount
    }
  }

  /**
   * Resolution measures how much the conditional probabilities for each <class, bin> differ from the overall class probabilities,
   * and is defined as
   *
   *   resolution = 1 / (total # of examples) * sum_{class a} sum_{bin b} [
   *      (# of examples that are predicted to fall into class a with probability in bin b) *
   *        (% of these examples that actually fall into class a - % of all examples that fall into class a)^2 ]
   */
  def resolution: Double = {
    if (totalCount == 0) {
      0.0
    } else {
      val inverseCounts = counts.filter { _._2 > 0.0 }.mapValues { 1.0 / _ }
      val actualRates = Ring.times(actual.mapValues { _.toDouble }, inverseCounts)
      val sqDiffs = actualRates.map { case (ab, rate) => (ab, math.pow(rate - actualClassRates.getOrElse(ab._1, 0.0), 2).toDouble) }
      val weightedSum = Ring.times(sqDiffs, counts.mapValues { _.toDouble }).values.sum

      weightedSum / totalCount
    }
  }

  /**
   * Uncertainty measures the inherent uncertainty of the event, and is defined as
   *
   *   uncertainty = sum_{class a} (% of examples that fall into class a) * (1 - % of examples that fall into class a)
   */
  def uncertainty: Double = actualClassRates.mapValues { r => r * (1 - r) }.values.sum

  /**
   * Score is the total Brier score.
   */
  def score: Double = reliability - resolution + uncertainty
}

/**
 * BrierScoreSemigroup is a semigroup for adding BrierScores.
 */
case class BrierScoreSemigroup[A, B] extends Semigroup[BrierScore[A, B]] {

  def plus(l: BrierScore[A, B], r: BrierScore[A, B]): BrierScore[A, B] = {
    BrierScore[A, B](Monoid.plus(l.predicted, r.predicted),
      Monoid.plus(l.actual, r.actual),
      Monoid.plus(l.counts, r.counts),
      l.totalCount + r.totalCount)
  }

}

/**
 * BrierDecompositionError computes Brier scores and their decompositions.
 *
 * @tparam A the classes to be predicted into.
 * @tparam B the probability distribution bins.
 */
case class BrierDecompositionError[A, B](binner: Double => B) extends Error[Map[A, Long], BrierScore[A, B]] {
  val semigroup = BrierScoreSemigroup[A, B]()

  def normalizedFrequencies(m: Map[A, Long]): Map[A, Double] = {
    val nonNeg = m.mapValues { n => math.max(n, 0L) }
    val total = math.max(nonNeg.values.sum, 1L)
    nonNeg.mapValues { _.toDouble / total }
  }

  def create(actual: Map[A, Long], predicted: Iterable[Map[A, Long]]): BrierScore[A, B] = {
    predicted match {
      case Nil => BrierScore[A, B](Map[(A, B), Double](), Map[(A, B), Long](), Map[(A, B), Long](), 0L)
      case _ =>
        val probs = predicted.map(normalizedFrequencies)
        val averagedScores = Monoid.sum(probs).mapValues { _ / predicted.size }
        val binnedPredictions = averagedScores.map { case (a, score) => ((a, binner(score)), score) }

        val binnedActuals = binnedPredictions.map { case (ab, score) => (ab, actual.getOrElse(ab._1, 0L)) }
        val binCounts = binnedPredictions.map { case (ab, score) => (ab, 1L) }

        BrierScore[A, B](binnedPredictions, binnedActuals, binCounts, 1L)
    }
  }
}
