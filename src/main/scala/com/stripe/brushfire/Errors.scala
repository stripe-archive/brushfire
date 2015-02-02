package com.stripe.brushfire

import com.twitter.algebird._

/**
 * FrequencyError sets up the most common case when dealing
 * with discrete distributions:
 * - normalize each prediction before combining them
 * - normalize to probablities after combining
 * - compute and sum errors separately for each component of the actual distribution
 * - provide a zero for when predictions or actuals are missing
 */
trait FrequencyError[L, E] extends Error[Map[L, Long], E] {

  val semigroup = monoid

  def monoid: Monoid[E]

  def normalizedFrequencies(m: Map[L, Long]): Map[L, Double] = {
    val nonNeg = m.mapValues { n => math.max(n, 0L) }
    val total = math.max(nonNeg.values.sum, 1L)
    nonNeg.mapValues { _.toDouble / total }
  }

  def create(actual: Map[L, Long], predicted: Iterable[Map[L, Long]]) = {
    if (predicted.isEmpty)
      monoid.zero
    else {
      val normalized = predicted.map(normalizedFrequencies)
      val probabilities = Monoid.sum(normalized).mapValues { _ / predicted.size }
      monoid.sum(actual.map { case (label, count) => error(label, count, probabilities) })
    }
  }

  def error(label: L, count: Long, probabilities: Map[L, Double]): E
}

/**
 * BrierScore is a container for tracking the Brier score decomposition of a set of errors.
 *
 * See http://www.eumetcal.org/intralibrary/open_virtual_file_path/i2055n15861t/english/msg/ver_prob_forec/uos2/uos2_ko2.htm for more information.
 *
 * @tparam A the classes to be predicted into.
 * @tparam B the probability distribution bins.
 * @param predicted every time an example falls into a (predicted class, predicted probability bin), that probability is added here.
 * @param actual counts the number of examples per (predicted class, predicted probability bin) that actually fall into the predicted class.
 * @param counts the total number of examples in each (predicted class, predicted probability bin).
 * @param totalCount the total number of examples.
 */
case class BrierScore[A, B](predicted: Map[(A, B), Double], actual: Map[(A, B), Long], counts: Map[(A, B), Long], totalCount: Long) {

  /**
   * actualClassRates contains the percentage of observations that fall into each class.
   */
  def actualClassRates: Map[A, Double] = {
    val actualClassCounts = actual.groupBy { case (ab, ct) => ab._1 }.mapValues { _.map(_._2).sum }

    actualClassCounts.mapValues { _ / math.max(totalCount.toDouble, 1.0) }
  }

  /**
   * Reliability measures how close forecast probabilities are to the true probabilities, and is defined as
   *
   *   reliability = 1 / (total # of examples) * sum_{class a} sum_{bin b} [
   *                   (# of examples that are predicted to fall into class a with probability in bin b) *
   *                   (predicted probability - % of these examples that actually fall into class a)^2 ]
   *               = 1 / (total # of examples) * sum_{class a} sum_{bin b} [
   *                   (weighted count of examples that fall into class a with probability in bin b, where weights are the probability -
   *                   # of these examples that actually fall into bin b)^2 /
   *                   (# of examples that are predicted to fall into class a with probability in bin b)]
   */
  def reliability: Double = {
    if (totalCount == 0) {
      0.0
    } else {
      val inverseCounts = counts.filter { _._2 > 0.0 }.mapValues { 1.0 / _ }
      val predictedProbs = Ring.times(predicted, inverseCounts)
      val actualRates = Ring.times(actual.mapValues { _.toDouble }, inverseCounts)
      val sqDiffs = Group.minus(predictedProbs, actualRates).mapValues { math.pow(_, 2) }
      val weightedSum = Ring.times(sqDiffs, counts.mapValues { _.toDouble }).values.sum

      weightedSum / totalCount
    }
  }

  /**
   * Resolution measures how much the conditional probabilities for each <class, bin> differ from the overall class probabilities,
   * and is defined as
   *
   *   resolution = 1 / (total # of examples) * sum_{class a} sum_{bin b} [
   *                  (# of examples that are predicted to fall into class a with probability in bin b) *
   *                  (% of these examples that actually fall into class a - % of all examples that fall into class a)^2 ]
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

object BrierScore {
  implicit def monoid[A, B]: Monoid[BrierScore[A, B]] = new Monoid[BrierScore[A, B]] {
    val zero = BrierScore[A, B](Map[(A, B), Double](), Map[(A, B), Long](), Map[(A, B), Long](), 0L)

    def plus(l: BrierScore[A, B], r: BrierScore[A, B]): BrierScore[A, B] = {
      BrierScore[A, B](Monoid.plus(l.predicted, r.predicted),
        Monoid.plus(l.actual, r.actual),
        Monoid.plus(l.counts, r.counts),
        l.totalCount + r.totalCount)
    }
  }
}

/**
 * BrierDecompositionError computes Brier scores and their decompositions.
 *
 * The bins used in the Brier score calculation are quantiles (represented by a Double of the lower endpoint).
 * @param bins the number of bins to use (defaults to 10, ie deciles)
 * @tparam A the classes to be predicted into.
 */

case class BrierDecompositionError[A](bins: Int = 10) extends FrequencyError[A, BrierScore[A, Double]] {
  val monoid = BrierScore.monoid[A, Double]

  def bin(x: Double): Double = (x * bins).toInt / bins.toDouble

  def error(label: A, count: Long, probabilities: Map[A, Double]) = {
    val binnedPredictions = probabilities.map { case (a, score) => ((a, bin(score)), score * count) }
    val binnedActuals = binnedPredictions.map { case (ab, _) => (ab, if (label == ab._1) count else 0L) }
    val binCounts = binnedPredictions.map { case (ab, _) => (ab, count) }

    BrierScore(binnedPredictions, binnedActuals, binCounts, count)
  }
}
