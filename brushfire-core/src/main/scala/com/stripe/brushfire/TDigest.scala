package com.stripe.brushfire

import com.tdunning.math.stats.TDigest
import com.twitter.algebird.{Monoid, Semigroup}

private [this] case object TDigestSemigroup extends Semigroup[TDigest] {
  override def plus(l: TDigest, r: TDigest): TDigest = {
    val td = TDigest.createDigest(math.max(l.compression(), r.compression()))
    td.add(l)
    td.add(r)
    td
  }

  override def sumOption(iter: TraversableOnce[TDigest]): Option[TDigest] = {
    iter.foldLeft(None: Option[TDigest]) {
      case (None, el) =>
        val td = TDigest.createDigest(el.compression())
        td.add(el)
        Some(td)

      case (f@Some(acc), el) =>
        acc.add(el)
        f
    }
  }
}

object TDigestSplitter {
  /**
   * Estimate the number of items on either side of a quantile split
   */
  private def splitCounts(q: Double, digest: TDigest): (Long, Long) = {
    // the cumulative density reported is sometimes outside of [0,1] so we need to bound it. otherwise
    // the estimated target distribution will be far from realistic... leading the Evaluator to
    // frequently pick a suboptimal split point
    val left = (digest.cdf(q).max(0.0).min(1.0) * digest.size().toDouble).toLong
    val right = (digest.size - left).max(0L)
    (left, right)
  }

  /**
   * Create a singleton [[scala.collection.Map]] if the value is positive, else return [[scala.collection.Map.empty]]
   */
  private def positiveOrEmpty[L]: (L, Long) => Map[L, Long] = {
    case (key, value) if value > 0L => Map(key -> value)
    case _ => Map.empty[L, Long]
  }

  private def targetDistribution[L](q: Double)(target: L, digest: TDigest): (Map[L, Long], Map[L, Long]) = {
    val (left, right) = splitCounts(q, digest)
    (positiveOrEmpty(target, left), positiveOrEmpty(target, right))
  }
}

case class TDigestSplitter[L](k: Int = 25, compression: Double = 100.0) extends Splitter[Double, Map[L, Long]] {
  override type S = Map[L, TDigest]

  override def split[A](parent: Map[L, Long], stats: S, annotation: A): Iterable[Split[Double, Map[L, Long], A]] = {
    implicit val tds = TDigestSemigroup
    import TDigestSplitter.targetDistribution

    val splits = for {
      // merge the statistics from all targets
      merged <- Semigroup.sumOption(stats.valuesIterator).toSeq

      // generate the requested number of splits evenly between [1/k, 1]
      // we can skip 0 because the predicate is LessThan, and no targets should
      // exist below the 0th quantile
      i <- 1 to k

      // first estimate the nth quantile from the merged statistics
      // this will become a potential split point in the resulting tree
      q = merged.quantile(i.toDouble / k.toDouble).max(0.0).min(merged.size().toDouble)

      // then estimate the target distribution using the target's statistics
      (left, right) = Monoid.sum(stats.map(Function.tupled(targetDistribution(q))))

      // the goodness score of an entirely empty split should not be the best
      // and so they can be discarded immediately
      if left.nonEmpty || right.nonEmpty
    } yield {
      BinarySplit(LessThan(q), left, right, annotation)
    }

    // if the input is not continuous or has too few examples we will end up
    // with a smaller number of actual splits than requested, and the rest will be
    // duplicates... remove the dupes
    splits.distinct
  }

  override def semigroup: Semigroup[S] = {
    implicit val tds = TDigestSemigroup
    implicitly[Semigroup[S]]
  }

  override def create(value: Double, target: Map[L, Long]): S = {
    target.mapValues {
      case count if count <= Int.MaxValue.toLong =>
        val td = TDigest.createDigest(compression)
        td.add(value, count.toInt)
        td
    }
  }
}