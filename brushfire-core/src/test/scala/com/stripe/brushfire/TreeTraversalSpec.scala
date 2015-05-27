package com.stripe.brushfire

import scala.util.Random

import com.twitter.algebird._

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers

class TreeTraversalSpec extends WordSpec with Matchers with Checkers {
  import TreeGenerators._

  "depthFirst" should {
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
            TreeTraversal.depthFirst.find(tree, Map.empty[String, Double]).headOption == Some(children.head._3)
        }
      })
    }

    "traverse in order" in {
      check { (tree: Tree[String, Double, Map[String, Long]]) =>
        TreeTraversal.depthFirst
          .find(tree, Map.empty[String, Double])
          .map(_.index)
          .sliding(2)
          .forall {
            case Seq(_) => true
            case Seq(i, j) => i < j
          }
      }
    }
  }

  def split[T, A: Semigroup](key: String, pred: Predicate[Double], left: Node[String, Double, T, A], right: Node[String, Double, T, A]): SplitNode[String, Double, T, A] =
    SplitNode((key, pred, left) :: (key, Not(pred), right) :: Nil)

  "weightedDepthFirst" should {
    implicit val traversal = TreeTraversal.weightedDepthFirst[String, Double, Double, Int]

    "choose the heaviest node in a split" in {
      val split1 = split("f1", LessThan(0D), LeafNode(0, -1D, 12), LeafNode(0, 1D, 9))
      AnnotatedTree(split1).leafFor(Map.empty[String, Double]) shouldBe Some(split1.children.head._3)

      val split2 = split("f2", LessThan(0D), LeafNode(0, -1D, -3), LeafNode(0, 1D, 9))
      AnnotatedTree(split2).leafFor(Map.empty[String, Double]) shouldBe Some(split2.children.last._3)
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

      tree.leafFor(Map.empty) shouldBe Some(LeafNode(2, -100D, 77))
      tree.leafFor(Map("f1" -> -1D)) shouldBe Some(LeafNode(1, 1D, 53))
      tree.leafFor(Map("f2" -> 1D)) shouldBe Some(LeafNode(3, 333D, 19))
      tree.leafFor(Map("f1" -> -1D, "f2" -> -1D)) shouldBe Some(LeafNode(0, -1D, 33))
    }
  }

  def collectLeafs[K, V, T, A](node: Node[K, V, T, A]): Set[LeafNode[K, V, T, A]] =
    node match {
      case SplitNode(children) =>
        children.collect {
          case (_, p, n) if p(None) => collectLeafs(n)
        }.flatten.toSet

      case leaf @ LeafNode(_, _, _) =>
        Set(leaf)
    }

  "probabilisticWeightedMatch" should {
    // What we can expect from Java's RNG. We ignore the first, because the code does.
    // seed = 0 : X, 0.24053641567148587, 0.6374174253501083,  0.5504370051176339, 0.5975452777972018
    // seed = 1 : X, 0.41008081149220166, 0.20771484130971707, 0.3327170559595112, 0.9677559094241207

    def traversal(seed: Long) = TreeTraversal.probabilisticWeightedDepthFirst[String, Double, Double, Int](new Random(seed))

    "choose predictable weighted probabilistic node in a split" in {
      val split1 = split("f1", LessThan(0D), LeafNode(0, -1D, 2405), LeafNode(1, 1D, 10000 - 2405))
      traversal(0).find(AnnotatedTree(split1), Map.empty[String, Double]) shouldBe
        Seq(LeafNode(1, 1D, 10000 - 2405), LeafNode(0, -1D, 2405))

      val split2 = split("f1", LessThan(0D), LeafNode(0, -1D, 2406), LeafNode(1, 1D, 10000 - 2406))
      traversal(0).find(AnnotatedTree(split2), Map.empty[String, Double]) shouldBe
        Seq(LeafNode(0, -1D, 2406), LeafNode(1, 1D, 10000 - 2406))

      val split3 = split("f1", LessThan(0D), LeafNode(0, -1D, 4100), LeafNode(1, 1D, 10000 - 4100))
      traversal(1).find(AnnotatedTree(split3), Map.empty[String, Double]) shouldBe
        Seq(LeafNode(1, 1D, 10000 - 4100), LeafNode(0, -1D, 4100))

      val split4 = split("f1", LessThan(0D), LeafNode(0, -1D, 4101), LeafNode(1, 1D, 10000 - 4101))
      traversal(1).find(AnnotatedTree(split4), Map.empty[String, Double]) shouldBe
        Seq(LeafNode(0, -1D, 4101), LeafNode(1, 1D, 10000 - 4101))
    }

    "choose predictable weighted probabilistic path in a tree" in {
      val tree = AnnotatedTree(
        split("f1", LessThan(0D),
          split("f2", LessThan(0D), // 30
            LeafNode(0, 1D, 8),
            LeafNode(1, 2D, 22)),
          split("f2", LessThan(0D), // 70
            LeafNode(2, 3D, 24),
            LeafNode(3, 4D, 46))))

      traversal(0).find(tree, Map.empty[String, Double]).headOption shouldBe
        Some(LeafNode(1, 2D, 22))
      traversal(1).find(tree, Map.empty[String, Double]).headOption shouldBe
        Some(LeafNode(2, 3D, 24))
      traversal(1).find(tree, Map("f1" -> 1D)).headOption shouldBe
        Some(LeafNode(3, 4D, 46))
      traversal(0).find(tree, Map("f1" -> -1D)).headOption shouldBe
        Some(LeafNode(0, 1D, 8))
    }
  }
}
