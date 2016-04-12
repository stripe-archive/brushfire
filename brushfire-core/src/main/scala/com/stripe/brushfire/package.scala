package com.stripe

import spire.std.AnyInstances

package object brushfire extends AnyInstances {
  type Tree[K, V, T] = AnnotatedTree[K, V, T, Unit]
  type BranchLabel[K, V, A] = (K, Predicate[V], A)
  type LeafLabel[T, A] = (Int, T, A)
}
