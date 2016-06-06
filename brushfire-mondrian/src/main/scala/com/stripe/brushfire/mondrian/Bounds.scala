package com.stripe.brushfire.mondrian

import scala.collection.mutable.Builder
import scala.math.{ max, min }

import Util._

object Bounds {

  /**
   * Compute the pairwise min value from two vectors.
   *
   * The given vectors should both have the same size.
   */
  def min(xs: Vector[Double], ys: Vector[Double]): Vector[Double] = {
    def loop(i: Int, b: Builder[Double, Vector[Double]]): Vector[Double] =
      if (i >= xs.size) b.result else {
        loop(i + 1, b += scala.math.min(xs(i), ys(i)))
      }
    loop(0, Vector.newBuilder[Double])
  }

  /**
   * Compute the pairwise max value from two vectors.
   *
   * The given vectors should both have the same size.
   */
  def max(xs: Vector[Double], ys: Vector[Double]): Vector[Double] = {
    def loop(i: Int, b: Builder[Double, Vector[Double]]): Vector[Double] =
      if (i >= xs.size) b.result else {
        loop(i + 1, b += scala.math.max(xs(i), ys(i)))
      }
    loop(0, Vector.newBuilder[Double])
  }

  /**
   * Compute the placewise deltas between x and the given bounds.
   *
   * If lower(i) <= xs(i) <= upper(i), then the term for i will be
   * zero. Otherwise, it will be a positive number corresponding to
   * the extent it is outside the given bounds.
   *
   * The given vectors should all have the same size.
   */
  def deltas(upper: Vector[Double], xs: Vector[Double], lower: Vector[Double]): Vector[Double] = {
    val bldr = Vector.newBuilder[Double]
    def loop(i: Int): Vector[Double] =
      if (i >= xs.size) bldr.result else {
        val a = upper(i)
        val x = xs(i)
        val b = lower(i)
        bldr += (if (x >= a) x - a else if (b >= x) b - x else 0.0)
        loop(i + 1)
      }
    loop(0)
  }

  /**
   * Compute the overall rate at which the vector `xs` lies outside of
   * the given bounds.
   *
   * This method is equivalent to deltas(upper, xs, lower).sum, but
   * may be more efficient in many cases.
   *
   * The given vectors should all have the same size.
   */
  def rate(upper: Vector[Double], xs: Vector[Double], lower: Vector[Double]): Double = {
    def loop(i: Int, sum: Double): Double =
      if (i >= xs.size) sum else {
        val a = upper(i)
        val x = xs(i)
        val b = lower(i)
        val add = if (x >= a) x - a else if (b >= x) b - x else 0.0
        loop(i + 1, sum + add)
      }
    loop(0, 0.0)
  }
}
