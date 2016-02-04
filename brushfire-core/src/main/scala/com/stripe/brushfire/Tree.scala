package com.stripe.brushfire

import com.stripe.bonsai.FullBinaryTreeOps
import com.twitter.algebird._

object Tree {
  def apply[K, V, T](node: Node[K, V, T, Unit]): Tree[K, V, T] =
    AnnotatedTree(node)

  def singleton[K, V, T](t: T): Tree[K, V, T] = Tree(LeafNode(0, t, ()))

  implicit def fullBinaryTreeOpsForTree[K, V, T]: FullBinaryTreeOps[Tree[K, V, T], (K, Predicate[V], Unit), (Int, T, Unit)] =
    new FullBinaryTreeOpsForAnnotatedTree[K, V, T, Unit]
}
