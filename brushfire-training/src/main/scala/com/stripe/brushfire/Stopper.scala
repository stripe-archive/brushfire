package com.stripe.brushfire

/** Provides stopping conditions which guide when splits will be attempted */
trait Stopper[T] {
  def shouldSplit(target: T): Boolean
  def shouldSplitDistributed(target: T): Boolean
  def samplingRateToSplitLocally(target: T): Double
}

case class FrequencyStopper[L](maxInMemorySize: Long, minSize: Long) extends Stopper[Map[L, Long]] {
  def shouldSplit(target: Map[L, Long]) = target.size > 1 && target.values.sum > minSize
  def shouldSplitDistributed(target: Map[L, Long]) = target.values.sum > maxInMemorySize
  def samplingRateToSplitLocally(target: Map[L, Long]) = math.min(1.0, maxInMemorySize.toDouble / target.values.sum)
}
