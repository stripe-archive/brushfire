package com.stripe.brushfire

import scala.reflect.ClassTag

import scala.collection.immutable.Queue
import scala.collection.mutable.ArrayBuilder

class HeapTree[K, V, T, A] private (
  // Labels are required to traverse the tree properly. They also denote the type of node.
  //  - Split nodes are denoted with a positive index.
  //  - Leaf nodes are denoted with a negative index.
  //  - External (fake) nodes are denoted with 0.
  labels: Array[Int],

  // There are 2 keys per node (binary splits with potentially different keys).
  keys: Array[K], // 2n

  // There are 2 predicates per node (binary splits).
  predicates: Array[Predicate[V]],

  // There are as many targets as nodes, but some are null.
  targets: Array[T],

  // The node annotations of each node. If None, then it is assumed that
  // `A =:= Unit`.
  annotations: Option[Array[A]]
) extends TreeLike[HeapTree, K, V, T, A] {

  type TreeNode = Int

  def root: TreeNode = 0

  private def labelFor(node: TreeNode): Int = {
    require(node >= 0 && node < labels.length, "invalid node reference")
    val label = labels(node)
    require(label != 0, "invalid node")
    label
  }

  def annotation(node: TreeNode): A = annotations match {
    case None => ().asInstanceOf[A]
    case Some(as) => as(labelFor(node))
  }

  def withNode[B](node: TreeNode)(
    split: Seq[(K, Predicate[V], TreeNode)] => B,
    leaf: (T, A) => B
  ): B = {
    val label = labelFor(node)
    if (label > 0) {
      val l = 2 * (label - 1)
      val r = l + 1
      val lChild = 2 * label
      val rChild = 2 * label + 1
      val children = (keys(l), predicates(l), lChild) :: (keys(r), predicates(r), rChild) :: Nil
      split(children)
    } else if (label < 0) {
      val target = targets(-label)
      leaf(target, annotation(node))
    } else {
      throw new IllegalArgumentException("invalid internal node reference")
    }
  }
}

object HeapTree {
  def fromTree[K: ClassTag, V, T: ClassTag](tree: Tree[K, V, T]): HeapTree[K, V, T, Unit] = {
    val labelsBldr = ArrayBuilder.make[Int]()
    val keysBldr = ArrayBuilder.make[K]()
    val predBldr = ArrayBuilder.make[Predicate[V]]()
    val targetsBldr = ArrayBuilder.make[T]()

    val nullKey = null.asInstanceOf[K]
    val nullPred = null.asInstanceOf[Predicate[V]]

    val maxDepth = tree.fold((_, _) => 1)(_.map(_._3).max + 1)

    def addInternalNode(i: Int, lKey: K, lPred: Predicate[V], rKey: K, rPred: Predicate[V], target: T): Unit = {
      labelsBldr += i
      keysBldr += lKey
      keysBldr += rKey
      predBldr += lPred
      predBldr += rPred
      targetsBldr += target
    }

    def addExternalNode(): Unit = {
      labelsBldr += 0
    }

    // We add nodes to the heap in level-order. We perform a BFS of the tree,
    // using the `nodes` queue to keep order the search. Internal nodes (split
    // + leaf nodes) are wrapped in `Some`, while external nodes are denoted
    // with `None`.
    def flatten(i: Int, nodes: Queue[Option[(Int, Node[K, V, T, Unit])]]): Unit = {
      if (nodes.nonEmpty) {
        nodes.dequeue match {
          case (Some((depth, SplitNode(Seq((lKey, lPred, lChild), (rKey, rPred, rChild))))), rest) =>
            addInternalNode(i, lKey, lPred, rKey, rPred, null.asInstanceOf[T])
            val childDepth = depth + 1
            flatten(i + 1, rest.enqueue(Some(childDepth -> lChild)).enqueue(Some(childDepth -> rChild)))

          case (Some((_, SplitNode(children))), _) =>
            throw new IllegalArgumentException("expected binary splits, but found ${children.size}-ary split")

          case (Some((depth, LeafNode(_, target, _))), rest) =>
            addInternalNode(i, nullKey, nullPred, nullKey, nullPred, target)
            val nodes0 =
              if (depth < maxDepth) rest.enqueue(None).enqueue(None)
              else rest
            flatten(i + 1, rest)

          case (None, rest) =>
            addExternalNode()
            flatten(i, rest)

        }
      }
    }

    flatten(1, Queue(Some(1 -> tree.root)))
    new HeapTree(labelsBldr.result(), keysBldr.result(), predBldr.result(), targetsBldr.result(), None)
  }
}
