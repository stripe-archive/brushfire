package com.stripe.brushfire

import scala.util.Random

import com.twitter.algebird._

import org.scalacheck.{ Gen, Prop }
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers

class TreeTraversalSpec extends WordSpec with Matchers with Checkers {
  import TreeGenerators._

  type TreeSDM[A] = AnnotatedTree[String, Double, Map[String, Long], A]
  type TreeSDD[A] = AnnotatedTree[String, Double, Double, A]

  def split[T, A: Semigroup](key: String, pred: Predicate[Double], left: Node[String, Double, T, A], right: Node[String, Double, T, A]): SplitNode[String, Double, T, A] =
    SplitNode(key, pred, left, right)

  "depthFirst" should {
    "always choose the left side of a split in a binary tree" in {
      val simpleTreeGen: Gen[TreeSDM[Unit]] =
        genBinaryTree(arbitrary[String], arbitrary[Double], arbitrary[Map[String, Long]], arbitrary[Unit], 2)
          .filter(_.root match {
            case SplitNode(_, p, _, _, _) =>
              p match {
                case IsPresent(_) => false
                case _ => true
              }
            case _ =>
              false
          })
      check(Prop.forAll(simpleTreeGen) { (tree: TreeSDM[Unit]) =>
        (tree.root: @unchecked) match {
          case SplitNode(_, _, LeafNode(li, lt, la), LeafNode(ri, rt, ra), _) =>
            TreeTraversal.depthFirst[TreeSDM[Unit], String, Double, Map[String, Long], Unit]
              .search(tree, Map.empty[String, Double], None).headOption == Some((li, lt, la))
        }
      })
    }

    "traverse in order" in {
      check { (tree: TreeSDM[Unit]) =>
        val traversal =
          TreeTraversal.depthFirst[TreeSDM[Unit], String, Double, Map[String, Long], Unit]
        val leaves: Stream[(Int, Map[String, Long], Unit)] =
          traversal.search(tree, Map.empty[String, Double], None)

        leaves
          .map { case (index, _, _) => index }
          .sliding(2)
          .forall {
            case Seq(_) => true
            case Seq(i, j) => i < j
          }
      }
    }
  }

  "weightedDepthFirst" should {
    implicit val traversal = TreeTraversal.weightedDepthFirst[TreeSDD[Int], String, Double, Double, Int]

    "choose the heaviest node in a split" in {
      val split1 = split("f1", LessThan(0D), LeafNode(0, -1D, 12), LeafNode(0, 1D, 9))
      AnnotatedTree(split1).leafFor(Map.empty[String, Double]) shouldBe Some((0, -1D, 12))

      val split2 = split("f2", LessThan(0D), LeafNode(0, -1D, -3), LeafNode(0, 1D, 9))
      AnnotatedTree(split2).leafFor(Map.empty[String, Double]) shouldBe Some((0, 1D, 9))
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

      tree.leafFor(Map.empty) shouldBe Some((2, -100D, 77))
      tree.leafFor(Map("f1" -> -1D)) shouldBe Some((1, 1D, 53))
      tree.leafFor(Map("f2" -> 1D)) shouldBe Some((3, 333D, 19))
      tree.leafFor(Map("f1" -> -1D, "f2" -> -1D)) shouldBe Some((0, -1D, 33))
    }
  }

  def collectLeafs[K, V, T, A](node: Node[K, V, T, A]): Set[LeafNode[K, V, T, A]] =
    node match {
      case SplitNode(_, p, lc, rc, _) =>
        (if (p.run(None)) List(lc, rc) else Nil).flatMap(collectLeafs).toSet
      case leaf @ LeafNode(_, _, _) =>
        Set(leaf)
    }

  "probabilisticWeightedDepthFirst" should {
    // What we can expect from Java's RNG, as used in DepthFirstTreeTraversal.
    //
    // seed = "b", 0.2703319497247767, 0.0932475649560437, 0.7544424971549143, 0.40708156683248087, 0.5937069662406929
    // seed = "c", 0.28329487121994257, 0.6209168529111834, 0.896329641267321, 0.4295933416724764, 0.28764827423012496
    // seed = "z", 0.3982796920464978, 0.09452393725015651, 0.2645674766831084, 0.45670292528849277, 0.4919659310853626

    val traversal = TreeTraversal.probabilisticWeightedDepthFirst[TreeSDD[Int], String, Double, Double, Int]()

    "choose a predictable node from a split" in {
      val split1 = split("f1", LessThan(0D), LeafNode(0, -1D, 2703), LeafNode(1, 1D, 10000 - 2703))
      traversal.search(AnnotatedTree(split1), Map.empty[String, Double], Some("b")) shouldBe
        Seq((1, 1D, 10000 - 2703), (0, -1D, 2703))

      val split2 = split("f1", LessThan(0D), LeafNode(0, -1D, 2704), LeafNode(1, 1D, 10000 - 2704))
      traversal.search(AnnotatedTree(split2), Map.empty[String, Double], Some("b")) shouldBe
        Seq((0, -1D, 2704), (1, 1D, 10000 - 2704))

      val split3 = split("f1", LessThan(0D), LeafNode(0, -1D, 3982), LeafNode(1, 1D, 10000 - 3982))
      traversal.search(AnnotatedTree(split3), Map.empty[String, Double], Some("z")) shouldBe
        Seq((1, 1D, 10000 - 3982), (0, -1D, 3982))

      val split4 = split("f1", LessThan(0D), LeafNode(0, -1D, 3983), LeafNode(1, 1D, 10000 - 3983))
      traversal.search(AnnotatedTree(split4), Map.empty[String, Double], Some("z")) shouldBe
        Seq((0, -1D, 3983), (1, 1D, 10000 - 3983))
    }

    "choose a predictable path through a tree" in {
      val tree = AnnotatedTree(
        split("f1", LessThan(0D),
          split("f2", LessThan(0D), // 30
            LeafNode(0, 1D, 9),
            LeafNode(1, 2D, 21)),
          split("f2", LessThan(0D), // 70
            LeafNode(2, 3D, 24),
            LeafNode(3, 4D, 46))))

      traversal.search(tree, Map.empty, Some("c")).headOption shouldBe Some((1, 2D, 21))
      traversal.search(tree, Map.empty, Some("z")).headOption shouldBe Some((2, 3D, 24))
      traversal.search(tree, Map("f1" -> 1D), Some("z")).headOption shouldBe Some((3, 4D, 46))
      traversal.search(tree, Map("f1" -> -1D), Some("b")).headOption shouldBe Some((0, 1D, 9))

      def stableSearch(t: TreeSDD[Int]): (Int, Double, Int) =
        traversal.search(tree, Map.empty, Some("x")).head

      def unstableSearch(t: TreeSDD[Int]): (Int, Double, Int) =
        traversal.search(tree, Map.empty, None).head

      // ensure that parallel traversal have the same results as
      // sequential traversals.
      val trees = (1 to 16).map(_ => tree)

      // should never fail
      trees.par.map(stableSearch) shouldBe trees.map(stableSearch)

      // may fail very occasionally -- but this verifies that without
      // using a stable seed, we get faster non-stable behavior.
      trees.par.map(unstableSearch) should not be trees.map(unstableSearch)
    }
  }
}
