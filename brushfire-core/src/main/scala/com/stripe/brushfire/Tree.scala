package com.stripe.brushfire

import com.twitter.algebird._

// type Tree[K, V, T] = AnnotatedTree[K, V, T, Unit]

object Tree {
  def apply[K, V, T](node: Node[K, V, T, Unit]): Tree[K, V, T] =
    AnnotatedTree(node)

  def singleton[K, V, T](t: T): Tree[K, V, T] = Tree(LeafNode(0, t, ()))

  def expand[K, V, T: Monoid](times: Int, treeIndex: Int, leaf: LeafNode[K, V, T, Unit], splitter: Splitter[V, T], evaluator: Evaluator[T], stopper: Stopper[T], sampler: Sampler[K], instances: Iterable[Instance[K, V, T]]): Node[K, V, T, Unit] = {
    if (times > 0 && stopper.shouldSplit(leaf.target)) {
      implicit val jdSemigroup = splitter.semigroup

      Semigroup.sumOption(instances.flatMap { instance =>
        instance.features.map { case (f, v) =>
          if(sampler.includeFeature(f, treeIndex, leaf.index))
            Map(f -> splitter.create(v, instance.target))
          else
            Map.empty[K,splitter.S]
        }
      }).flatMap { featureMap =>
        val splits = featureMap.toList.flatMap {
          case (f, s) =>
            splitter.split(leaf.target, s).flatMap { x =>
              val targets = x.predicates.map{_._2}
              evaluator
                .trainingError(rootTarget, targets)
                .map{s => f -> (x, s) }
            }
        }

        if(splits.isEmpty)
          None
        else {
          val (splitFeature, (split, _)) = splits.minBy { case (f, (x, s)) => s }
          val edges = split.predicates.toList.map {
            case (pred, _) =>
              val newInstances = instances.filter { inst => pred(inst.features.get(splitFeature)) }
              val target = Monoid.sum(newInstances.map { _.target })
              (pred, target, newInstances)
          }

          if (edges.count { case (_, _, newInstances) => newInstances.nonEmpty } > 1) {
            Some(SplitNode(edges.map {
              case (pred, target, newInstances) =>
                (splitFeature, pred, expand[K, V, T](times - 1, treeIndex, LeafNode(0, target), splitter, evaluator, stopper, sampler, newInstances))
            }))
          } else {
            None
          }
        }
      }.getOrElse(leaf)
    } else {
      leaf
    }
  }
}
