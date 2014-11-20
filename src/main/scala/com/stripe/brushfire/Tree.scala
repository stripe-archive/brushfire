package com.stripe.brushfire

import com.twitter.algebird._

sealed trait Node[V, T]
case class SplitNode[V, T](val children: Seq[(String, Predicate[V], Node[V, T])]) extends Node[V, T]
case class LeafNode[V, T](
  val index: Int,
  target: T) extends Node[V, T]

case class Tree[V, T](root: Node[V, T]) {
  private def findLeaf(row: Map[String, V], start: Node[V, T]): Option[LeafNode[V, T]] = {
    start match {
      case leaf: LeafNode[V, T] => Some(leaf)
      case SplitNode(children) =>
        children
          .find { case (feature, predicate, _) => predicate(row.get(feature)) }
          .flatMap { case (_, _, child) => findLeaf(row, child) }
    }
  }

  def leafAt(leafIndex: Int): Option[LeafNode[V, T]] = leafAt(leafIndex, root)

  def leafAt(leafIndex: Int, start: Node[V, T]): Option[LeafNode[V, T]] = {
    start match {
      case leaf: LeafNode[V, T] => if (leaf.index == leafIndex) Some(leaf) else None
      case SplitNode(children) =>
        children
          .flatMap { case (_, _, child) => leafAt(leafIndex, child) }
          .headOption
    }
  }

  def leafIndexFor(row: Map[String, V]) = findLeaf(row, root).map { _.index }

  def targetFor(row: Map[String, V]) = findLeaf(row, root).map { _.target }

  def growByLeafIndex(fn: Int => Seq[(String, Predicate[V], T)]) = {
    var newIndex = -1
    def incrIndex = {
      newIndex += 1
      newIndex
    }

    def growFrom(start: Node[V, T]): Node[V, T] = {
      start match {
        case LeafNode(index, target) => {
          val newChildren = fn(index)
          if (newChildren.isEmpty)
            LeafNode[V, T](incrIndex, target)
          else
            SplitNode[V, T](newChildren.map {
              case (feature, predicate, target) =>
                (feature, predicate, LeafNode[V, T](incrIndex, target))
            })
        }
        case SplitNode(children) => SplitNode[V, T](children.map {
          case (feature, predicate, child) =>
            (feature, predicate, growFrom(child))
        })
      }
    }

    Tree(growFrom(root))
  }

  def updateByLeafIndex(fn: Int => Option[T]) = {
    def updateFrom(start: Node[V, T]): Node[V, T] = {
      start match {
        case LeafNode(index, target) =>
          LeafNode[V, T](index, fn(index).getOrElse(target))
        case SplitNode(children) => SplitNode[V, T](children.map {
          case (feature, predicate, child) =>
            (feature, predicate, updateFrom(child))
        })
      }
    }

    Tree(updateFrom(root))
  }
}

object Tree {
  def empty[V, T](t: T): Tree[V, T] = Tree(LeafNode[V, T](0, t))
}

