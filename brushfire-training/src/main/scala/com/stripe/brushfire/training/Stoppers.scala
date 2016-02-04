package com.stripe.brushfire.training

import com.stripe.brushfire._

case class FrequencyStopper[L](maxInMemorySize: Long, minSize: Long) extends Stopper[Map[L, Long]] {
  def shouldSplit(target: Map[L, Long]) = target.size > 1 && target.values.sum > minSize
  def shouldSplitDistributed(target: Map[L, Long]) = target.values.sum > maxInMemorySize
  def samplingRateToSplitLocally(target: Map[L, Long]) = math.min(1.0, maxInMemorySize.toDouble / target.values.sum)
}
