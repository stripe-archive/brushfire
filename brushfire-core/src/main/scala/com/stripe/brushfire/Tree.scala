package com.stripe.brushfire

import com.twitter.algebird._

// type Tree[K, V, T] = AnnotatedTree[K, V, T, Unit]

object Tree {
  def apply[K, V, T](node: Node[K, V, T, Unit]): Tree[K, V, T] =
    AnnotatedTree(node)

  def singleton[K, V, T](t: T): Tree[K, V, T] = Tree(LeafNode(0, t, ()))

  def expand[K, V, T: Monoid](times: Int, leaf: LeafNode[K, V, T, Unit], splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T], instances: Iterable[Instance[K, V, T]]): Node[K, V, T, Unit] = {
    if (times > 0 && stopper.shouldSplit(leaf.target)) {
      implicit val jdSemigroup = splitter.semigroup

      Semigroup.sumOption(instances.flatMap { instance =>
        instance.features.map { case (f, v) => Map(f -> splitter.create(v, instance.target)) }
      }).flatMap { featureMap =>
        val splits = featureMap.toList.flatMap {
          case (f, s) =>
            splitter.split(leaf.target, s).map { x => f -> evaluator.evaluate(x) }
        }

        val (splitFeature, (split, score)) = splits.maxBy { case (f, (x, s)) => s }
        val edges = split.predicates.toList.map {
          case (pred, _) =>
            val newInstances = instances.filter { inst => pred(inst.features.get(splitFeature)) }
            val target = Monoid.sum(newInstances.map { _.target })
            (pred, target, newInstances)
        }

        if (edges.count { case (pred, target, newInstances) => newInstances.size > 0 } > 1) {
          Some(SplitNode(edges.map {
            case (pred, target, newInstances) =>
              (splitFeature, pred, expand[K, V, T](times - 1, LeafNode(0, target), splitter, evaluator, stopper, newInstances))
          }))
        } else {
          None
        }
      }.getOrElse(leaf)
    } else {
      leaf
    }
  }
}
