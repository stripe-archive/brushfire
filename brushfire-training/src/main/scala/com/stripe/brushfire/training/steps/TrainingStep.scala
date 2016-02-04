package com.stripe.brushfire.training.steps

import com.stripe.brushfire._
import com.stripe.brushfire.training._
import com.twitter.algebird._

trait TrainingStep[K,V,T] {

  type K1
  type V1
  type V2

  def prepare(trees: Map[Int, Tree[K,V,T]], instance: Instance[K,V,T]): Seq[((Int,Int,K1), V1)]
  def lift(tree: Tree[K,V,T], leafIndex: Int, key: K1, v1: V1): Traversable[V2]
  def update(tree: Tree[K,V,T], map: Map[Int,V2]): Tree[K,V,T]

  def semigroup1: Semigroup[V1]
  def semigroup2: Semigroup[V2]
  def ordering: Ordering[K1]
}

case class UpdateTargets[K,V,T](sampler: Sampler[K])(implicit val semigroup1: Semigroup[T])
  extends TrainingStep[K,V,T] {

  type K1 = Unit
  type V1 = T
  type V2 = T

  val semigroup2 = semigroup1
  val ordering = implicitly[Ordering[Unit]]

  def prepare(trees: Map[Int, Tree[K,V,T]], instance: Instance[K,V,T]) = {
     for (
        (treeIndex, tree) <- trees.toList;
        i <- 1.to(sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)).toList;
        leafIndex <- tree.leafIndexFor(instance.features).toList
      ) yield (treeIndex, leafIndex, ()) -> instance.target
  }

  def lift(tree: Tree[K,V,T], leafIndex: Int, key: K1, v1: T) = List(v1)

  def update(tree: Tree[K,V,T], map: Map[Int,T]) = {
    tree.updateByLeafIndex { index => map.get(index).map { t => LeafNode(index, t) } }
  }
}

case class Expand[K,V,T](sampler: Sampler[K], stopper: Stopper[T], splitter: Splitter[V, T], evaluator: Evaluator[T])(implicit val ordering: Ordering[K])
  extends TrainingStep[K,V,T] {

  type K1 = K
  type V1 = splitter.S
  type V2 = (K, Split[V, T], Double)

  def prepare(trees: Map[Int, Tree[K,V,T]], instance: Instance[K,V,T]) = {
    lazy val features = instance.features.mapValues { value => splitter.create(value, instance.target) }

    for
      ((treeIndex, tree) <- trees.toList;
      i <- 1.to(sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)).toList;
      (index, target, annotation) <- tree.leafFor(instance.features).toList if stopper.shouldSplit(target);
      (feature, stats) <- features if (sampler.includeFeature(feature, treeIndex, index)))
        yield (treeIndex, index, feature) -> stats
  }

  def lift(tree: Tree[K,V,T], leafIndex: Int, key: K, v1: V1) = {
    tree.leafAt(leafIndex).toList.flatMap { leaf =>
      splitter
        .split(leaf.target, v1)
        .flatMap { split =>
          val leaves = List(split.leftDistribution, split.rightDistribution)
          evaluator.trainingError(leaves).map { goodness =>
            (key, split, goodness)
          }
        }
    }
  }

  def update(tree: Tree[K,V,T], map: Map[Int,V2]) = {
    tree.growByLeafIndex { index =>
      map.get(index).map { case (feature, split, _) =>
          split.createSplitNode(feature)
      }
    }
  }

  val semigroup1 = splitter.semigroup
  val semigroup2 = new Semigroup[V2] {
    def plus(a: V2, b: V2) = {
      if (a._3 > b._3) a else b
    }
  }
}

 /**
   * Prune a tree to minimize validation error.
   *
   * Recursively replaces each split with a leaf when it would have a lower error than the sum of the child leaves
   * errors.
   *
   * @param validationData A Map from leaf index to validation data.
   * @param voter to create predictions from target distributions.
   * @param error to calculate an error statistic given observations (validation) and predictions (training).
   * @return The new, pruned tree.
   */
  /*def prune[P, E](validationData: Map[Int, T], voter: Voter[T, P], error: Error[T, P, E])(implicit targetMonoid: Monoid[T], errorOrdering: Ordering[E]): AnnotatedTree[K, V, T, A] = {
    AnnotatedTree(pruneNode(validationData, this.root, voter, error)._2).renumberLeaves
  }
*/
  /**
   * Prune a tree to minimize validation error, starting from given root node.
   *
   * This method recursively traverses the tree from the root, branching on splits, until it finds leaves, then goes back
   * down the tree combining leaves when such a combination would reduce validation error.
   *
   * @param validationData Map from leaf index to validation data.
   * @param start The root node of the tree.
   * @return A node at the root of the new, pruned tree.
   */
/*  def pruneNode[P, E](validationData: Map[Int, T], start: Node[K, V, T, A], voter: Voter[T, P], error: Error[T, P, E])(implicit targetMonoid: Monoid[T], errorOrdering: Ordering[E]): (Map[Int, T], Node[K, V, T, A]) = {
    type ChildSeqType = (K, Predicate[V], Node[K, V, T, A])
    start match {
      case leaf @ LeafNode(_, _, _) =>
        // Bounce at the bottom and start back up the tree.
        (validationData, leaf)

      case SplitNode(children) =>
        // Call pruneNode on each child, accumulating modified children and
        // additions to the validation data along the way.
        val (newData, newChildren) =
          children.foldLeft((validationData, Seq[ChildSeqType]())) {
            case ((vData, childSeq), (k, p, child)) =>
              pruneNode(vData, child, voter, error) match {
                case (v, c) => (v, childSeq :+ (k, p, c))
              }
          }
        // Now that we've taken care of the children, prune the current level.
        val childLeaves = newChildren.collect { case (k, v, s @ LeafNode(_, _, _)) => (k, v, s) }

        if (childLeaves.size == newChildren.size) {
          // If all children are leaves, we can potentially prune.
          val parent = SplitNode(newChildren)
          pruneLevel(parent, childLeaves, newData, voter, error)
        } else {
          // If any children are SplitNodes, we can't prune.
          (newData, SplitNode(newChildren))
        }
    }
  }
*/
  /**
   * Test conditions and optionally replace parent with a new leaf that combines children.
   *
   * Also merges validation data for any combined leaves. This relies on a hack that assumes no leaves have negative
   * indices to start out.
   *
   * @param parent
   * @param children
   * @return
   */
/*  def pruneLevel[P, E](
    parent: SplitNode[K, V, T, A],
    children: Seq[(K, Predicate[V], LeafNode[K, V, T, A])],
    validationData: Map[Int, T],
    voter: Voter[T, P],
    error: Error[T, P, E])(implicit targetMonoid: Monoid[T],
      errorOrdering: Ordering[E]): (Map[Int, T], Node[K, V, T, A]) = {

    // Get training and validation data and validation error for each leaf.
    val (targets, validations, errors) = children.unzip3 {
      case (k, p, leaf) =>
        val trainingTarget = leaf.target
        val validationTarget = validationData.getOrElse(leaf.index, targetMonoid.zero)
        val leafError = error.create(validationTarget, voter.combine(Some(trainingTarget)))
        (trainingTarget, validationTarget, leafError)
    }

    val targetSum = targetMonoid.sum(targets) // Combined training targets to create the training data of the potential combined node.
    val targetPrediction = voter.combine(Some(targetSum)) // Generate prediction from combined target.
    val validationSum = targetMonoid.sum(validations) // Combined validation target for combined node.
    val errorOfSums = error.create(validationSum, targetPrediction) // Error of potential combined node.
    val sumOfErrors = error.semigroup.sumOption(errors) // Sum of errors of leaves.
    // Compare sum of errors and error of sums (and lower us out of the sum of errors Option).
    val doCombine = sumOfErrors.exists { sumOE => errorOrdering.gteq(sumOE, errorOfSums) }
    if (doCombine) { // Create a new leaf from the combination of the children.
      // Find a unique (negative) index for the new leaf:
      val newIndex = -1 * children.map { case (k, p, leaf) => Math.abs(leaf.index) }.max
      val node = LeafNode[K, V, T, A](newIndex, targetSum, parent.annotation)
      (validationData + (newIndex -> validationSum), node)
    } else {
      (validationData, parent)
    }
  }*/

  /*

  def expand[K, V, T: Monoid](times: Int, treeIndex: Int, leaf: LeafNode[K, V, T, Unit], splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T], sampler: Sampler[K], instances: Iterable[Instance[K, V, T]]): Node[K, V, T, Unit] = {
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
            splitter.split(leaf.target, s).map { x => f -> evaluator.evaluate(x) }
        }

        if(splits.isEmpty)
          None
        else {
          val (splitFeature, (split, _)) = splits.maxBy { case (f, (x, s)) => s }
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
  */

