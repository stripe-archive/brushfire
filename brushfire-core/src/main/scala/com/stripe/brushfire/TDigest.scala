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
  private def splitDists[L](q: Double)(target: L, digest: TDigest): (Map[L, Long], Map[L, Long]) = {
    // the cumulative density reported is sometimes outside of [0,1] so we need to bound it. otherwise
    // the estimated target distribution will be far from realistic... leading the Evaluator to
    // frequently pick a suboptimal split point
    val leftDist = (digest.cdf(q).max(0.0).min(1.0) * digest.size().toDouble).toLong
    val rightDist = (digest.size - leftDist).max(0L)
    val left = if (leftDist > 0L) Map(target -> leftDist) else Map.empty[L, Long]
    val right = if (rightDist > 0L) Map(target -> rightDist) else Map.empty[L, Long]
    (left, right)
  }
}

case class TDigestSplitter[L](k: Int = 25, compression: Double = 100.0) extends Splitter[Double, Map[L, Long]] {
  override type S = Map[L, TDigest]

  override def split(parent: Map[L, Long], stats: S): Iterable[Split[Double, Map[L, Long]]] = {
    implicit val tds = TDigestSemigroup
    import TDigestSplitter.splitDists

    val splits = for {
      // merge the statistics from all targets
      merged <- Semigroup.sumOption(stats.valuesIterator).toSeq
      i <- 1 to k
      q = merged.quantile(i.toDouble / k.toDouble).max(0.0).min(merged.size().toDouble)
      (leftDist, rightDist) = Monoid.sum(stats.map(Function.tupled(splitDists(q))))
      if leftDist.nonEmpty || rightDist.nonEmpty
    } yield {
      BinarySplit(LessThan(q), leftDist, rightDist)
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