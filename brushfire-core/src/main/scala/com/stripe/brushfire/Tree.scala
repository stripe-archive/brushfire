package com.stripe.brushfire

import com.stripe.bonsai.FullBinaryTreeOps
import com.twitter.algebird._
import spire.algebra.PartialOrder

object Tree {
  def apply[K, V, T](node: Node[K, V, T, Unit]): Tree[K, V, T] =
    AnnotatedTree(node)

  def singleton[K, V, T](t: T): Tree[K, V, T] =
    AnnotatedTree(LeafNode(0, t, ()))

  def expand[K, V: PartialOrder, T: Monoid](times: Int, treeIndex: Int, leaf: LeafNode[K, V, T, Unit], splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T], sampler: Sampler[K], instances: Iterable[Instance[K, V, T]]): Node[K, V, T, Unit] = {
    if (times > 0 && stopper.shouldSplit(leaf.target)) {
      implicit val jdSemigroup = splitter.semigroup

      Semigroup.sumOption(instances.flatMap { instance =>
        instance.features.map { case (f, v) =>
          if(sampler.includeFeature(f, treeIndex, leaf.index))
            Map(f -> splitter.create(v, instance.target))
          else
            Map.empty[K, splitter.S]
        }
      }).flatMap { featureMap =>

        val splits = for {
          (f, s) <- featureMap.toList
          split <- splitter.split(leaf.target, s)
          tpl <- evaluator.evaluate(split)
        } yield (f, tpl)

        if (splits.isEmpty) None else {
          val (splitFeature, (Split(pred, left, right), _)) = splits.maxBy { case (f, (x, s)) => s }
          def expandChild(dist: T): Node[K, V, T, Unit] = {
            val newInstances = instances.filter { inst =>
              inst.features.get(splitFeature) match {
                case Some(v) => pred(v)
                case None => true
              }
            }
            val target = Monoid.sum(newInstances.map(_.target))
            expand(times - 1, treeIndex, LeafNode(0, target), splitter, evaluator, stopper, sampler, newInstances)
          }
          Some(SplitNode(splitFeature, pred, expandChild(left), expandChild(right)))
        }
      }.getOrElse(leaf)
    } else {
      leaf
    }
  }

  implicit def fullBinaryTreeOpsForTree[K, V, T]: FullBinaryTreeOps[Tree[K, V, T], (K, Predicate[V], Unit), (Int, T, Unit)] =
    new FullBinaryTreeOpsForAnnotatedTree[K, V, T, Unit]
}
