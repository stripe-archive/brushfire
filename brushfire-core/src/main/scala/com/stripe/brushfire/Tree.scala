package com.stripe.brushfire

import com.twitter.algebird._

// type Tree[K, V, T] = AnnotatedTree[K, V, T, Unit]

object Tree {
  def apply[K, V, T](node: Node[K, V, T, Unit]): Tree[K, V, T] =
    AnnotatedTree(node)

  def singleton[K, V, T](t: T): Tree[K, V, T] = Tree(LeafNode(0, t, ()))
}
