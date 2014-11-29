package com.stripe.brushfire

case class FrequencyStopper[L](threshold: Long, localThreshold: Long) extends Stopper[Map[L, Long]] {
  def canSplit(target: Map[L, Long]) = target.size > 1
  def shouldSplitLocally(target: Map[L, Long]) = target.values.sum > localThreshold
  def shouldSplitDistributed(target: Map[L, Long]) = target.values.sum > threshold
}
