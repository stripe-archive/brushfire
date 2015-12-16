package com.stripe.brushfire

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.math.Ordering
import scala.util.Random
import scala.util.hashing.MurmurHash3

import com.twitter.algebird._
import com.stripe.bonsai.FullBinaryTreeOps

object Types {
  type BranchLabel[K, V, A] = (K, Predicate[V], A)
  type LeafLabel[T, A] = (Int, T, A)

  //type Ops[Tree] = FullBinaryTreeOps[Tree]
  //type Aux[Tree, K, V, T, A] = FullBinaryTreeOps.Aux[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]
  //type AnnotatedTreeTraversal[K, V, T, A] = TreeTraversal[AnnotatedTree[K, V, T, A], K, V, T, A]
}
