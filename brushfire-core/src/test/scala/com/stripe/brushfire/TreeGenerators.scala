package com.stripe.brushfire

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

  implicit def arbitraryTree[K: Arbitrary, V: Arbitrary: Ordering, T: Arbitrary]: Arbitrary[Tree[K, V, T]] =
    Arbitrary(genBinaryTree(arbitrary[K], arbitrary[V], arbitrary[T], DefaultTreeHeight))

  def genBinaryTree[K, V: Ordering, T](genK: Gen[K], genV: Gen[V], genT: Gen[T], height: Int): Gen[Tree[K, V, T]] = {
    def genLeafNode(index: Int): Gen[LeafNode[K, V, T, Unit]] =
      genT.map(LeafNode(index, _))

    def genSplit(index: Int, maxDepth: Int) = for {
      key <- genK
      pred <- genPredicate(genV)
      left <- genNode(index, maxDepth - 1)
      right <- genNode(index | (1 << (maxDepth - 1)), maxDepth - 1)
    } yield SplitNode(key, pred, left, right)

    def genNode(index: Int, maxDepth: Int): Gen[Node[K, V, T, Unit]] =
      if (maxDepth > 1) {
        Gen.frequency(maxDepth -> genSplit(index, maxDepth), 1 -> genLeafNode(index))
      } else {
        genLeafNode(index)
      }

    genNode(0, height).map(Tree(_))
  }
}
