package com.stripe.brushfire.mondrian

import scala.util.Random.nextDouble
import scala.math.{ log, max, min }

object Util {

  type Positive = Double

  def zipWith[A](xs: Vector[A], ys: Vector[A])(f: (A, A) => A): Vector[A] =
    xs.zip(ys).map { case (a, b) => f(a, b) }

  def weightedSample(weights: Vector[Double]): Int = {
    val total = weights.sum
    val x = nextDouble * total
    weights.iterator.scanLeft(x) { (acc, w) => acc - w }.indexWhere(_ <= 0.0) - 1
  }

  def sample(from: Double, until: Double): Double =
    (until - from) * nextDouble + from

  def dot[K](m0: Map[K, Int], m1: Map[K, Int]): Int =
    m0.iterator.map { case (k, v0) => v0 * m1.getOrElse(k, 0) }.sum
}

import Util._

sealed abstract class Node {
  def timestamp: Double
  def lowerBounds: Vector[Double]
  def upperBounds: Vector[Double]

  def fold[A](isLeaf: A)(isBranch: (A, A) => A): A =
    this match {
      case Leaf(_, _, _) =>
        isLeaf
      case Branch(_, _, _, _, _, left, right) =>
        isBranch(left.fold(isLeaf)(isBranch), right.fold(isLeaf)(isBranch))
    }
}

object Node {
  def leaf(t: Double, x: Vector[Double]): Node =
    Leaf(t, x, x)
}

case class Branch(
  splitDim: Int,
  splitPoint: Double,
  timestamp: Double,
  lowerBounds: Vector[Double],
  upperBounds: Vector[Double],
  leftChild: Node,
  rightChild: Node) extends Node

case class Leaf(
  timestamp: Double,
  lowerBounds: Vector[Double],
  upperBounds: Vector[Double]) extends Node

case class MondrianTree(root: Option[Node], λ: Double) {

  def histogram(xss: TraversableOnce[Vector[Double]]): Map[String, Int] =
    xss.foldLeft(Map.empty[String, Int]) { (m, xs) =>
      val p = path(xs)
      m.updated(p, m.getOrElse(p, 0) + 1)
    }

  def depth: Int =
    root match {
      case None => 0
      case Some(n) => n.fold(1)((x, y) => max(x, y) + 1)
    }

  def size: Int =
    root match {
      case None => 0
      case Some(n) => n.fold(1)(_ + _)
    }

  def path(x: Vector[Double]): String = {
    def loop(node: Node): String =
      node match {
        case leaf @ Leaf(_, _, _) =>
          ":" + leaf.toString
        case Branch(i, pt, _, _, _, left, right) =>
          if (x(i) <= pt) "L" + loop(left)
          else "R" + loop(right)
      }
    root.map(loop).getOrElse("")
  }

  def absorb(xs: Vector[Double]): MondrianTree =
    MondrianTree(Some(root.fold(Node.leaf(λ, xs))(n => extendNode(n, xs))), λ)

  def extendNode(t: Node, x: Vector[Positive]): Node = {

    def introduceNewParent(deltas: Vector[Double], e: Double, timestamp: Double, j: Node): Node = {
      val i = weightedSample(deltas)
      val xi = x(i)
      val jub = j.upperBounds(i)
      val lb2 = zipWith(j.lowerBounds, x)(_ min _)
      val ub2 = zipWith(j.upperBounds, x)(_ max _)
      val leaf = Node.leaf(λ, x)
      if (xi > jub) { // cut <= xi
        Branch(i, sample(jub, xi), timestamp, lb2, ub2, j, leaf)
      } else { // cut > xi
        Branch(i, sample(xi, j.lowerBounds(i)), timestamp, lb2, ub2, leaf, j)
      }
    }

    def extendBlock(parentTimestamp: Double, j: Node): Node = {
      val lowerDeltas = zipWith(j.lowerBounds, x)((a, b) => max(a - b, 0))
      val upperDeltas = zipWith(x, j.upperBounds)((a, b) => max(a - b, 0))
      val deltas = zipWith(lowerDeltas, upperDeltas)(_ + _)
      val rate = lowerDeltas.sum + upperDeltas.sum
      // probability that e > 1/rate is:
      //   exp(-e*rate)
      val e = log(1.0 / nextDouble) / rate
      if (rate > 0.0 && parentTimestamp + e < j.timestamp) {
        introduceNewParent(deltas, e, parentTimestamp + e, j)
      } else {
        val lb2 = j.lowerBounds.zip(x).map { case (a, b) => a min b }
        val ub2 = j.upperBounds.zip(x).map { case (a, b) => a max b }
        j match {
          case b @ Branch(i, pt, t, _, _, left, right) =>
            if (x(i) <= pt) {
              b.copy(leftChild = extendBlock(t, left), lowerBounds = lb2, upperBounds = ub2)
            } else {
              b.copy(rightChild = extendBlock(t, right), lowerBounds = lb2, upperBounds = ub2)
            }
          case leaf @ Leaf(_, _, _) =>
            leaf.copy(lowerBounds = lb2, upperBounds = ub2)
        }
      }
    }

    extendBlock(0.0, t)
  }
}

object MondrianTree {

  def empty(λ: Double): MondrianTree =
    MondrianTree(None, λ)

  def apply(xss: TraversableOnce[Vector[Positive]], λ: Double): MondrianTree =
    xss.foldLeft(MondrianTree.empty(λ))((t, xs) => t.absorb(xs))
}
