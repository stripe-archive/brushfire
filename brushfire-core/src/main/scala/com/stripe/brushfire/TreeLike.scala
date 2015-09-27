package com.stripe.brushfire

import scala.language.higherKinds

/**
 * Extended by tree-like classes, which provide some basic operations on trees.
 */
trait TreeLike[C[_, _, _, _], K, V, T, A] {
  /**
   * The type of the tree's nodes.
   */
  type TreeNode

  /**
   * Returns the root of the tree.
   */
  def root: TreeNode

  /**
   * Returns the anotation for the given node.
   */
  def annotation(node: TreeNode): A

  /**
   * The catamorphism for the nodes.
   */
  def withNode[B](node: TreeNode)(
    split: Seq[(K, Predicate[V], TreeNode)] => B,
    leaf: (T, A) => B
  ): B

  /**
   * Perform a fold on the tree, starting from the leaves and moving up.
   *
   * @param leaf the function to map leaf nodes to B
   * @param split the function to map split nodes to B
   */
  def fold[B](leaf: (T, A) => B)(split: Seq[(K, Predicate[V], B)] => B): B = {
    lazy val foldSplit: Seq[(K, Predicate[V], TreeNode)] => B = { children =>
      split(children.map { case (k, pred, child) =>
        (k, pred, recur(child))
      })
    }

    def recur(node: TreeNode): B =
      withNode(node)(foldSplit, leaf)

    recur(root)
  }
}
