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
            TreeTraversal.depthFirst.find(tree, Map.empty[String, Double], None).headOption == Some(children.head._3)
        }
      })
    }

    "traverse in order" in {
      check { (tree: Tree[String, Double, Map[String, Long]]) =>
        TreeTraversal.depthFirst
          .find(tree, Map.empty[String, Double], None)
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

  "probabilisticWeightedDepthFirst" should {
    // What we can expect from Java's RNG, as used in DepthFirstTreeTraversal.
    //
    // seed = "b", 0.2703319497247767, 0.0932475649560437, 0.7544424971549143, 0.40708156683248087, 0.5937069662406929
    // seed = "c", 0.28329487121994257, 0.6209168529111834, 0.896329641267321, 0.4295933416724764, 0.28764827423012496
    // seed = "z", 0.3982796920464978, 0.09452393725015651, 0.2645674766831084, 0.45670292528849277, 0.4919659310853626

    val traversal = TreeTraversal.probabilisticWeightedDepthFirst[String, Double, Double, Int]

    "choose predictable weighted probabilistic node in a split" in {
      val split1 = split("f1", LessThan(0D), LeafNode(0, -1D, 2703), LeafNode(1, 1D, 10000 - 2703))
      traversal.find(AnnotatedTree(split1), Map.empty[String, Double], Some("b")) shouldBe
        Seq(LeafNode(1, 1D, 10000 - 2703), LeafNode(0, -1D, 2703))

      val split2 = split("f1", LessThan(0D), LeafNode(0, -1D, 2704), LeafNode(1, 1D, 10000 - 2704))
      traversal.find(AnnotatedTree(split2), Map.empty[String, Double], Some("b")) shouldBe
        Seq(LeafNode(0, -1D, 2704), LeafNode(1, 1D, 10000 - 2704))

      val split3 = split("f1", LessThan(0D), LeafNode(0, -1D, 3982), LeafNode(1, 1D, 10000 - 3982))
      traversal.find(AnnotatedTree(split3), Map.empty[String, Double], Some("z")) shouldBe
        Seq(LeafNode(1, 1D, 10000 - 3982), LeafNode(0, -1D, 3982))

      val split4 = split("f1", LessThan(0D), LeafNode(0, -1D, 3983), LeafNode(1, 1D, 10000 - 3983))
      traversal.find(AnnotatedTree(split4), Map.empty[String, Double], Some("z")) shouldBe
        Seq(LeafNode(0, -1D, 3983), LeafNode(1, 1D, 10000 - 3983))
    }

    "choose predictable weighted probabilistic path in a tree" in {
      val tree = AnnotatedTree(
        split("f1", LessThan(0D),
          split("f2", LessThan(0D), // 30
            LeafNode(0, 1D, 9),
            LeafNode(1, 2D, 21)),
          split("f2", LessThan(0D), // 70
            LeafNode(2, 3D, 24),
            LeafNode(3, 4D, 46))))

      traversal.find(tree, Map.empty[String, Double], Some("c")).headOption shouldBe
        Some(LeafNode(1, 2D, 21))
      traversal.find(tree, Map.empty[String, Double], Some("z")).headOption shouldBe
        Some(LeafNode(2, 3D, 24))
      traversal.find(tree, Map("f1" -> 1D), Some("z")).headOption shouldBe
        Some(LeafNode(3, 4D, 46))
      traversal.find(tree, Map("f1" -> -1D), Some("b")).headOption shouldBe
        Some(LeafNode(0, 1D, 9))
    }
  }
}
