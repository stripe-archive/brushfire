package com.stripe.brushfire

import scala.util.Random
import scala.util.hashing.MurmurHash3
import spire.algebra.Order
import spire.syntax.all._

/**
 * Simple data type that provides rules to order nodes during
 * traversal.
 *
 * In some cases subtypes of Reorder will also wraps RNG state, for
 * instances that need to randomly select instances. Thus, Reorder is
 * not guaranteed to be referentially-transparent. Fresh instances
 * should be used with each traversal.
 *
 * Reorder will also continue to recurse into a given structure using
 * a provided callback method.
 *
 * The reason that the node type is provided to `apply` (instead of
 * `Reorder`) has to do with how generic trees are specified. Since
 * TreeOps uses path-dependent types to specify node types, the
 * current design is a bit of a kludge that makes it easy to get the
 * types right when handling a Reorder instance to a TreeTraversal
 * instance (which is parameterized on a generic tree type).
 */
trait Reorder[A] {

  /**
   * Seed a reorder instance with a stable identifier.
   *
   * This ensures that reorders which are non-deterministic in general
   * (e.g. shuffled) will produce the same reordering for the same
   * seed across many traversals.
   *
   * On deterministic reorders this method is a noop.
   */
  def setSeed(seed: Option[String]): Reorder[A]

  /**
   * Perform a reordering.
   *
   * This method takes two nodes (`n1` and `n2`), as well as two
   * functions:
   *
   *  - `f`: function from node to identifying annotation
   *  - `g`: function from two nodes to a combined result
   *
   * The `f` function is used in cases where sorting or weighting is
   * necessary (in those cases A will be a weight or similar). The `g`
   * function is used to recurse on the result -- i.e. the possibly
   * reordered nodes `n1` and `n2` will be passed to `g` after the
   * reordering occurs.
   */
  def apply[N, S](n1: N, n2: N, f: N => A, g: (N, N) => S): S
}

object Reorder {

  /**
   * Reorder instance that traverses into the left node first.
   */
  def unchanged[A]: Reorder[A] =
    new UnchangedReorder()

  /**
   * Reorder instance that traverses into the node with the higher
   * weight first.
   */
  def weightedDepthFirst[A: Order]: Reorder[A] =
    new WeightedReorder()

  /**
   * Reorder instance that traverses into a random node first. Each
   * node has equal probability of being selected.
   */
  def shuffled[A](seed: Int): Reorder[A] =
    new ShuffledReorder(new Random(seed))

  /**
   * Reorder instance that traverses into a random node, but choose
   * the random node based on the ratio between its weight and the
   * total weight of both nodes.
   *
   * If the left node's weight was 10, and the right node's weight was
   * 5, the left node would be picked 2/3 of the time.
   */
  def probabilisticWeightedDepthFirst[A](seed: Int, conversion: A => Double): Reorder[A] =
    new ProbabilisticWeighted(new Random(seed), conversion)

  class UnchangedReorder[A] extends Reorder[A] {
    def setSeed(seed: Option[String]): Reorder[A] =
      this
    def apply[N, S](n1: N, n2: N, f: N => A, g: (N, N) => S): S =
      g(n1, n2)
  }

  class WeightedReorder[A: Order] extends Reorder[A] {
    def setSeed(seed: Option[String]): Reorder[A] =
      this
    def apply[N, S](n1: N, n2: N, f: N => A, g: (N, N) => S): S =
      if (f(n1) >= f(n2)) g(n1, n2) else g(n2, n1)
  }

  class ShuffledReorder[A](r: Random) extends Reorder[A] {
    def setSeed(seed: Option[String]): Reorder[A] =
      seed match {
        case Some(s) =>
          new ShuffledReorder(new Random(MurmurHash3.stringHash(s)))
        case None =>
          this
      }
    def apply[N, S](n1: N, n2: N, f: N => A, g: (N, N) => S): S =
      if (r.nextBoolean) g(n1, n2) else g(n2, n1)
  }

  class ProbabilisticWeighted[A](r: Random, conversion: A => Double) extends Reorder[A] {
    def setSeed(seed: Option[String]): Reorder[A] =
      seed match {
        case Some(s) =>
          new ProbabilisticWeighted(new Random(MurmurHash3.stringHash(s)), conversion)
        case None =>
          this
      }
    def apply[N, S](n1: N, n2: N, f: N => A, g: (N, N) => S): S = {
      val w1 = conversion(f(n1))
      val w2 = conversion(f(n2))
      val sum = w1 + w2
      if (r.nextDouble * sum < w1) g(n1, n2) else g(n2, n1)
    }
  }
}
