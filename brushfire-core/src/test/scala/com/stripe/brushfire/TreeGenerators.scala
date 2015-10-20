package com.stripe.brushfire

import com.twitter.algebird.Semigroup
import org.scalacheck.{ Gen, Arbitrary }
import org.scalacheck.Arbitrary.arbitrary

object PredicateGenerators {
  val MaxPredicateDepth = 2

  implicit def arbPredicate[V: Arbitrary: Ordering]: Arbitrary[Predicate[V]] =
    Arbitrary(genPredicate(arbitrary[V]))

  def genPredicate[V: Ordering](genV: Gen[V], depth: Int = 1): Gen[Predicate[V]] =
    if (depth >= MaxPredicateDepth) {
      Gen.oneOf(genV.map(EqualTo(_)), genV.map(LessThan(_)))
    } else {
      Gen.oneOf[Predicate[V]](
        genV.map(EqualTo(_)),
        genV.map(LessThan(_)),
        genPredicate(genV, depth + 1).map(Not(_)),
        Gen.nonEmptyListOf(genPredicate(genV, depth + 1)).map(AnyOf(_)),
        Gen.option(genPredicate(genV, depth + 1)).map(IsPresent(_)))
    }
}

object TreeGenerators {
  import PredicateGenerators._

  val DefaultTreeHeight = 6

  implicit def arbitraryTree[K: Arbitrary, V: Arbitrary: Ordering, T: Arbitrary, A: Arbitrary: Semigroup]: Arbitrary[Tree[K, V, T, A]] =
    Arbitrary(genBinaryTree(arbitrary[K], arbitrary[V], arbitrary[T], arbitrary[A], DefaultTreeHeight))

  def genBinaryTree[K, V: Ordering, T, A: Semigroup](genK: Gen[K], genV: Gen[V], genT: Gen[T], genA: Gen[A], height: Int): Gen[Tree[K, V, T, A]] = {
    def genLeafNode(index: Int): Gen[LeafNode[K, V, T, A]] = {
      for {
        t <- genT
        a <- genA
      } yield {
        LeafNode(index, t, a)
      }
    }

    def genSplit(index: Int, maxDepth: Int) = for {
      pred <- genPredicate(genV)
      key <- genK
      left <- genNode(index, maxDepth - 1)
      right <- genNode(index | (1 << (maxDepth - 1)), maxDepth - 1)
    } yield SplitNode(List(
      (key, pred, left),
      (key, Not(pred), right)))

    def genNode(index: Int, maxDepth: Int): Gen[Node[K, V, T, A]] =
      if (maxDepth > 1) {
        Gen.frequency(maxDepth -> genSplit(index, maxDepth), 1 -> genLeafNode(index))
      } else {
        genLeafNode(index)
      }

    genNode(0, height).map(Tree(_))
  }
}
