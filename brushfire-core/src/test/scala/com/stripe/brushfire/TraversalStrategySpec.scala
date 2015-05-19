package com.stripe.brushfire

import com.twitter.algebird._

import org.scalacheck.Prop
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers

class TraversalStrategySpec extends WordSpec with Matchers with Checkers {
  import TreeGenerators._

  "FirstMatch" should {
    "always choose the left side of a split in a binary tree" in {
      val simpleTreeGen = genBinaryTree(arbitrary[String], arbitrary[Double], arbitrary[Map[String, Long]], 2)
        .filter(_.root match {
          case SplitNode(children) =>
            children.collect { case (_, IsPresent(_), _) => true }.isEmpty
          case _ =>
            false
        })
      check(Prop.forAll(simpleTreeGen) { tree =>
        (tree.root: @unchecked) match {
          case SplitNode(children) =>
            tree.leafFor(Map.empty, TraversalStrategy.firstMatch) == Some(children.head._3)
        }
      })
    }
  }

  def split[T, A: Semigroup](key: String, pred: Predicate[Double], left: Node[String, Double, T, A], right: Node[String, Double, T, A]): SplitNode[String, Double, T, A] =
    SplitNode((key, pred, left) :: (key, Not(pred), right) :: Nil)

  "MaxWeightedMatch" should {
    "choose the heaviest node in a split" in {
      val split1 = split("f1", LessThan(0D), LeafNode(0, -1D, 12), LeafNode(0, 1D, 9))
      AnnotatedTree(split1).leafFor(Map.empty, TraversalStrategy.maxWeightedMatch) shouldBe Some(split1.children.head._3)

      val split2 = split("f2", LessThan(0D), LeafNode(0, -1D, -3), LeafNode(0, 1D, 9))
      AnnotatedTree(split2).leafFor(Map.empty, TraversalStrategy.maxWeightedMatch) shouldBe Some(split2.children.last._3)
    }

    "choose heaviest path in a tree" in {
      val tree = AnnotatedTree(
        split("f1", LessThan(0D),
          split("f2", LessThan(0D),
            LeafNode(0, -1D, 33),
            LeafNode(1, 1D, 53)),
          split("f2", LessThan(0D),
            LeafNode(2, -100D, 77),
            LeafNode(3, 333D, 19))))

      tree.leafFor(Map.empty, TraversalStrategy.maxWeightedMatch) shouldBe Some(LeafNode(2, -100D, 77))
      tree.leafFor(Map("f1" -> -1D), TraversalStrategy.maxWeightedMatch) shouldBe Some(LeafNode(1, 1D, 53))
      tree.leafFor(Map("f2" -> 1D), TraversalStrategy.maxWeightedMatch) shouldBe Some(LeafNode(3, 333D, 19))
      tree.leafFor(Map("f1" -> -1D, "f2" -> -1D), TraversalStrategy.maxWeightedMatch) shouldBe Some(LeafNode(0, -1D, 33))
    }
  }

  "ProbabilisticWeightedMatch" should {
    // What we can expect from Java's RNG. We ignore the first, because the code does.
    // seed = 0 : X, 0.24053641567148587, 0.6374174253501083,  0.5504370051176339, 0.5975452777972018
    // seed = 1 : X, 0.41008081149220166, 0.20771484130971707, 0.3327170559595112, 0.9677559094241207

    "choose predictable weighted probabilistic node in a split" in {
      val split1 = split("f1", LessThan(0D), LeafNode(0, -1D, 2405), LeafNode(1, 1D, 10000 - 2405))
      AnnotatedTree(split1).leafFor(Map.empty, TraversalStrategy.probabilisticWeightedMatch(0L)) shouldBe
        Some(LeafNode(1, 1D, 10000 - 2405))

      val split2 = split("f1", LessThan(0D), LeafNode(0, -1D, 2406), LeafNode(1, 1D, 10000 - 2406))
      AnnotatedTree(split2).leafFor(Map.empty, TraversalStrategy.probabilisticWeightedMatch(0L)) shouldBe
        Some(LeafNode(0, -1D, 2406))

      val split3 = split("f1", LessThan(0D), LeafNode(0, -1D, 4100), LeafNode(1, 1D, 10000 - 4100))
      AnnotatedTree(split3).leafFor(Map.empty, TraversalStrategy.probabilisticWeightedMatch(1L)) shouldBe
        Some(LeafNode(1, 1D, 10000 - 4100))

      val split4 = split("f1", LessThan(0D), LeafNode(0, -1D, 4101), LeafNode(1, 1D, 10000 - 4101))
      AnnotatedTree(split4).leafFor(Map.empty, TraversalStrategy.probabilisticWeightedMatch(1L)) shouldBe
        Some(LeafNode(0, -1D, 4101))
    }

    "choose predictable weighted probabilistic path in a tree" in {
      val tree = AnnotatedTree(
        split("f1", LessThan(0D),
          split("f2", LessThan(0D), // 30
            LeafNode(0, 1D, 8),
            LeafNode(1, 2D, 22)),
          split("f2", LessThan(0D), // 70
            LeafNode(2, 3D, 15),
            LeafNode(3, 4D, 55))))

      tree.leafFor(Map.empty, TraversalStrategy.probabilisticWeightedMatch(0L)) shouldBe
        Some(LeafNode(1, 2D, 22))
      tree.leafFor(Map.empty, TraversalStrategy.probabilisticWeightedMatch(1L)) shouldBe
        Some(LeafNode(2, 3D, 15))
      tree.leafFor(Map("f1" -> 1D), TraversalStrategy.probabilisticWeightedMatch(0L)) shouldBe
        Some(LeafNode(3, 4D, 55))
      tree.leafFor(Map("f1" -> -1D), TraversalStrategy.probabilisticWeightedMatch(0L)) shouldBe
        Some(LeafNode(0, 1D, 8))
    }
  }
}
