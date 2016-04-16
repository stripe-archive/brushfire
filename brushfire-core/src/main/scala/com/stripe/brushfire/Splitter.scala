package com.stripe.brushfire

import com.twitter.algebird._

/**
 * Produces candidate splits from the instances at a leaf node.
 * @tparam V feature values
 * @tparam T target distributions
 */
trait Splitter[V, T] {
  /** the type of a representation of a joint distribution of feature values and predictions */
  type S

  /** return a new joint distribution from a value and a target distribution */
  def create(value: V, target: T): S

  /** semigroup to sum up joint distributions */
  def semigroup: Semigroup[S]

  /** return candidate splits given a joint distribution and the parent node's target distrubution */
  def split(parent: T, stats: S): Iterable[Split[V, T]]
}


case class BinarySplitter[V: Ordering, T: Monoid](partition: V => Predicate[V]) extends Splitter[V, T] {

  type S = Map[V, T]
  def create(value: V, target: T) = Map(value -> target)

  val semigroup = implicitly[Semigroup[Map[V, T]]]

  def split(parent: T, stats: Map[V, T]): Iterable[Split[V, T]] = {
    stats.keys.map { v =>
      val predicate = partition(v)
      val (trues, falses) = stats.partition { case (v, d) => predicate(v) }
      Split(predicate, Monoid.sum(trues.values), Monoid.sum(falses.values))
    }
  }
}

case class DispatchedSplitter[A: Ordering, B, C: Ordering, D, T](
  ordinal: Splitter[A, T],
  nominal: Splitter[B, T],
  continuous: Splitter[C, T],
  sparse: Splitter[D, T])
    extends Splitter[Dispatched[A, B, C, D], T] {

  type S = Dispatched[ordinal.S, nominal.S, continuous.S, sparse.S]
  val semigroup =
    Semigroup.from[S] {
      case (Ordinal(l), Ordinal(r)) => Ordinal(ordinal.semigroup.plus(l, r))
      case (Nominal(l), Nominal(r)) => Nominal(nominal.semigroup.plus(l, r))
      case (Continuous(l), Continuous(r)) => Continuous(continuous.semigroup.plus(l, r))
      case (Sparse(l), Sparse(r)) => Sparse(sparse.semigroup.plus(l, r))
      case (a, b) => sys.error("Values do not match: " + (a, b))
    }

  def create(value: Dispatched[A, B, C, D], target: T) = {
    value match {
      case Ordinal(a) => Ordinal(ordinal.create(a, target))
      case Nominal(b) => Nominal(nominal.create(b, target))
      case Continuous(c) => Continuous(continuous.create(c, target))
      case Sparse(d) => Sparse(sparse.create(d, target))
    }
  }

  def split(parent: T, stats: S) = stats match {
    case Ordinal(as) => Dispatched.wrapSplits(ordinal.split(parent, as))(Dispatched.ordinal)
    case Nominal(bs) => Dispatched.wrapSplits(nominal.split(parent, bs))(Dispatched.nominal)
    case Continuous(cs) => Dispatched.wrapSplits(continuous.split(parent, cs))(Dispatched.continuous)
    case Sparse(ds) => Dispatched.wrapSplits(sparse.split(parent, ds))(Dispatched.sparse)
  }
}

case class RandomSplitter[V, T](original: Splitter[V, T])
    extends Splitter[V, T] {
  type S = original.S
  val semigroup = original.semigroup
  def create(value: V, target: T) = original.create(value, target)
  def split(parent: T, stats: S): Iterable[Split[V, T]] =
    scala.util.Random.shuffle(original.split(parent, stats)).headOption
}

case class BinnedSplitter[V, T](original: Splitter[V, T])(fn: V => V)
    extends Splitter[V, T] {
  type S = original.S
  def create(value: V, target: T) = original.create(fn(value), target)
  val semigroup = original.semigroup
  def split(parent: T, stats: S): Iterable[Split[V, T]] =
    original.split(parent, stats)
}

case class QTreeSplitter[T: Monoid](k: Int)
    extends Splitter[Double, T] {

  type S = QTree[T]

  val semigroup = new QTreeSemigroup[T](k)
  def create(value: Double, target: T) = QTree(value -> target)

  def split(parent: T, stats: QTree[T]): Iterable[Split[Double, T]] = {
    findAllThresholds(stats).map { threshold =>
      val predicate = Predicate.Lt(threshold)
      val leftDist = stats.rangeSumBounds(stats.lowerBound, threshold)._1
      val rightDist = stats.rangeSumBounds(threshold, stats.upperBound)._1
      Split(predicate, leftDist, rightDist)
    }
  }

  def findAllThresholds(node: QTree[T]): Iterable[Double] = {
    node.lowerChild.toList.flatMap { l => findAllThresholds(l) } ++
      node.upperChild.toList.flatMap { u => findAllThresholds(u) } ++
      findThreshold(node).toList
  }

  def findThreshold(node: QTree[T]): Option[Double] = {
    for (l <- node.lowerChild; u <- node.upperChild)
      yield l.upperBound
  }
}

case class SpaceSaverSplitter[V, L](capacity: Int = 1000)
    extends Splitter[V, Map[L, Long]] {

  type S = Map[L, SpaceSaver[V]]
  val semigroup = implicitly[Semigroup[S]]

  def create(value: V, target: Map[L, Long]) = target.mapValues { c =>
    Semigroup.intTimes(c, SpaceSaver(capacity, value))
  }

  def split(parent: Map[L, Long], stats: S): Iterable[Split[V, Map[L, Long]]] =
    stats
      .values
      .flatMap(_.counters.keys).toSet
      .map { (v: V) =>
        val mins = stats.mapValues { ss => ss.frequency(v).min }
        Split(Predicate.isEq(v), mins, Group.minus(parent, mins))
      }
}
