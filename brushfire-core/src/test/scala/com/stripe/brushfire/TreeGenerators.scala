package com.stripe.brushfire

import com.twitter.algebird.Semigroup

import org.scalacheck.{ Gen, Arbitrary }
import org.scalacheck.Arbitrary.arbitrary

import Predicate.{ isEq, notEq, lt, ltEq, gt, gtEq }

object PredicateGenerators {
  val MaxPredicateDepth = 2

  implicit def arbPredicate[V: Arbitrary]: Arbitrary[Predicate[V]] =
    Arbitrary(genPredicate(arbitrary[V]))

  def genPredicate[V](g: Gen[V]): Gen[Predicate[V]] =
    Gen.oneOf(g.map(isEq), g.map(notEq), g.map(lt), g.map(ltEq), g.map(gt), g.map(gtEq))
}

object TreeGenerators {
  import PredicateGenerators._

  val DefaultTreeHeight = 6

  implicit def arbitraryTree[K: Arbitrary, V: Arbitrary: Ordering, T: Arbitrary, A: Arbitrary: Semigroup]: Arbitrary[AnnotatedTree[K, V, T, A]] =
    Arbitrary(genBinaryTree(arbitrary[K], arbitrary[V], arbitrary[T], arbitrary[A], DefaultTreeHeight))

  def genBinaryTree[K, V: Ordering, T, A: Semigroup](genK: Gen[K], genV: Gen[V], genT: Gen[T], genA: Gen[A], height: Int): Gen[AnnotatedTree[K, V, T, A]] = {
    def genLeafNode(index: Int): Gen[LeafNode[K, V, T, A]] = for {
      t <- genT
      a <- genA
    } yield LeafNode(index, t, a)

    def genSplit(index: Int, maxDepth: Int) = for {
      key <- genK
      pred <- genPredicate(genV)
      left <- genNode(index, maxDepth - 1)
      right <- genNode(index | (1 << (maxDepth - 1)), maxDepth - 1)
    } yield SplitNode(key, pred, left, right)

    def genNode(index: Int, maxDepth: Int): Gen[Node[K, V, T, A]] =
      if (maxDepth > 1) {
        Gen.frequency(maxDepth -> genSplit(index, maxDepth), 1 -> genLeafNode(index))
      } else {
        genLeafNode(index)
      }

    genNode(0, height).map(AnnotatedTree(_))
  }
}
