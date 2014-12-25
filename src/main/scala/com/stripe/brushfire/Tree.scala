package com.stripe.brushfire

import com.twitter.algebird._

sealed trait Node[K, V, T]
case class SplitNode[K, V, T](val children: Seq[(K, Predicate[V], Node[K, V, T])]) extends Node[K, V, T]
case class LeafNode[K, V, T](
  val index: Int,
  target: T) extends Node[K, V, T]

case class Tree[K, V, T](root: Node[K, V, T]) {
  private def findLeaf(row: Map[K, V], start: Node[K, V, T]): Option[LeafNode[K, V, T]] = {
    start match {
      case leaf: LeafNode[K, V, T] => Some(leaf)
      case SplitNode(children) =>
        children
          .find { case (feature, predicate, _) => predicate(row.get(feature)) }
          .flatMap { case (_, _, child) => findLeaf(row, child) }
    }
  }

  def leafAt(leafIndex: Int): Option[LeafNode[K, V, T]] = leafAt(leafIndex, root)

  def leafAt(leafIndex: Int, start: Node[K, V, T]): Option[LeafNode[K, V, T]] = {
    start match {
      case leaf: LeafNode[K, V, T] => if (leaf.index == leafIndex) Some(leaf) else None
      case SplitNode(children) =>
        children
          .flatMap { case (_, _, child) => leafAt(leafIndex, child) }
          .headOption
    }
  }

  def leafFor(row: Map[K, V]) = findLeaf(row, root)

  def leafIndexFor(row: Map[K, V]) = findLeaf(row, root).map { _.index }

  def targetFor(row: Map[K, V]) = findLeaf(row, root).map { _.target }

  def growByLeafIndex(fn: Int => Seq[(K, Predicate[V], T)]) = {
    var newIndex = -1
    def incrIndex = {
      newIndex += 1
      newIndex
    }

    def growFrom(start: Node[K, V, T]): Node[K, V, T] = {
      start match {
        case LeafNode(index, target) => {
          val newChildren = fn(index)
          if (newChildren.isEmpty)
            LeafNode[K, V, T](incrIndex, target)
          else
            SplitNode[K, V, T](newChildren.map {
              case (feature, predicate, target) =>
                (feature, predicate, LeafNode[K, V, T](incrIndex, target))
            })
        }
        case SplitNode(children) => SplitNode[K, V, T](children.map {
          case (feature, predicate, child) =>
            (feature, predicate, growFrom(child))
        })
      }
    }

    Tree(growFrom(root))
  }

  def updateByLeafIndex(fn: Int => Option[Node[K, V, T]]) = {
    def updateFrom(start: Node[K, V, T]): Node[K, V, T] = {
      start match {
        case LeafNode(index, target) =>
          fn(index).getOrElse(start)
        case SplitNode(children) => SplitNode[K, V, T](children.map {
          case (feature, predicate, child) =>
            (feature, predicate, updateFrom(child))
        })
      }
    }

    Tree(updateFrom(root))
      .growByLeafIndex { i => Nil } //this will renumber all the leaves
  }
}

object Tree {
  def empty[K, V, T](t: T): Tree[K, V, T] = Tree(LeafNode[K, V, T](0, t))
  def expand[K, V, T: Monoid, E](times: Int, leaf: LeafNode[K, V, T], splitter: Splitter[V, T], error: Error[T, E], stopper: Stopper[T], instances: Iterable[Instance[K, V, T]]): Node[K, V, T] = {
    if (times > 0 && stopper.shouldSplit(leaf.target)) {
      implicit val jdSemigroup = splitter.semigroup

      Semigroup.sumOption(instances.flatMap { instance =>
        instance.features.map { case (f, v) => Map(f -> splitter.create(v, instance.target)) }
      }).flatMap { featureMap =>
        val splits = featureMap.toList.flatMap {
          case (feature, stats) =>
            splitter.split(leaf.target, stats).flatMap { split =>
              split.trainingError(error).map { err => (feature, split, err) }
            }
        }

        if (splits.isEmpty)
          None
        else {
          val (splitFeature, split, _) = splits.minBy { _._3 }(error.ordering)
          val edges = split.predicates.toList.map {
            case (pred, _) =>
              val newInstances = instances.filter { inst => pred(inst.features.get(splitFeature)) }
              val target = Monoid.sum(newInstances.map { _.target })
              (pred, target, newInstances)
          }

          if (edges.count { case (pred, target, newInstances) => newInstances.size > 0 } > 1) {
            Some(SplitNode(edges.map {
              case (pred, target, newInstances) =>
                (splitFeature, pred, expand(times - 1, LeafNode[K, V, T](0, target), splitter, error, stopper, newInstances))
            }))
          } else {
            None
          }
        }
      }.getOrElse(leaf)
    } else {
      leaf
    }
  }
}

