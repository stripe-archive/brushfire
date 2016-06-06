package com.stripe.brushfire.mondrian

import scala.util.Random.nextDouble

object Util {

  /**
   * Select a sample value uniformly-distributed between `from`
   * (inclusive) and `until` (exclusive).
   */
  def sample(from: Double, until: Double): Double =
    (until - from) * nextDouble + from

  /**
   * Select a weighted sample distributed from [0, weights.size].
   *
   * Each weight determines the likelihood of its index being
   * chosen. The probabability that i will be chosen is equal to:
   *
   *   weights(i) / weights.sum
   */
  def weightedSample(weights: Vector[Double]): Int = {
    def choose(i: Int, limit: Double): Int =
      if (i <= 0) 0 else {
        val lim = limit - weights(i)
        if (lim <= 0.0) i else choose(i - 1, lim)
      }
    choose(weights.size - 1, nextDouble * weights.sum)
  }

  /**
   * Compute the dot product of two vectors.
   *
   * The vectors are given in a sparse representation using maps: a
   * missing key is assumed to have a zero value.
   */
  def dot[K](m0: Map[K, Int], m1: Map[K, Int]): Int =
    m0.iterator.map { case (k, v0) => v0 * m1.getOrElse(k, 0) }.sum
}
