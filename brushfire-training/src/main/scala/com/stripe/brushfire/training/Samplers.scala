package com.stripe.brushfire.training

import com.stripe.brushfire._
import com.twitter.algebird._

object SingleTreeSampler extends Sampler[Any] {
  val numTrees = 1
  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = 1
  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int) = true
  def includeFeature(name: Any, treeIndex: Int, leafIndex: Int) = true
}

case class KFoldSampler(numTrees: Int) extends Sampler[Any] {
  val murmur = MurmurHash128(12345)

  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = {
    val (hash1, _) = murmur(id)
    val h = math.abs(hash1)
    if (h % numTrees == treeIndex)
      0
    else
      1
  }

  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int) = {
    timesInTrainingSet(id, timestamp, treeIndex) == 0
  }

  def includeFeature(name: Any, treeIndex: Int, leafIndex: Int) = true
}

case class RFSampler(numTrees: Int, featureRate: Double, samplingRate: Double = 1.0)
    extends Sampler[String] {

  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = {
    val rand = random(id, treeIndex)
    //poisson generator, from knuth
    val l = math.exp(-samplingRate)
    var k = 0
    var p = 1.0
    while (p > l) {
      k += 1
      p *= rand.nextDouble
    }
    k - 1
  }

  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int) = {
    timesInTrainingSet(id, timestamp, treeIndex) == 0
  }

  def includeFeature(name: String, treeIndex: Int, leafIndex: Int) = {
    val seed = (treeIndex << 16) + leafIndex
    random(name, seed).nextDouble < featureRate
  }

  def random(key: String, seed: Int) = {
    val murmur = MurmurHash128(seed)
    val (hash1, _) = murmur(key)
    new scala.util.Random(hash1)
  }
}

case class TimeGroupedSampler[K](base: Sampler[K], period: Long, groups: Int) extends Sampler[K] {
  def numTrees = base.numTrees * groups

  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = {
    val timeGroup = (timestamp / period) % groups
    val treeGroup = treeIndex % groups
    val indexInGroup = treeIndex / groups

    if (timeGroup == treeGroup)
      base.timesInTrainingSet(id, timestamp, indexInGroup)
    else
      0
  }

  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int) = {
    timesInTrainingSet(id, timestamp, treeIndex) == 0
  }

  def includeFeature(name: K, treeIndex: Int, leafIndex: Int) = base.includeFeature(name, treeIndex, leafIndex)
}

case class OutOfTimeSampler[K](base: Sampler[K], threshold: Long) extends Sampler[K] {
  def numTrees = base.numTrees

  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = {
    if (timestamp < threshold)
      base.timesInTrainingSet(id, timestamp, treeIndex)
    else
      0
  }

  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int) = {
    timestamp >= threshold
  }

  def includeFeature(name: K, treeIndex: Int, leafIndex: Int) = base.includeFeature(name, treeIndex, leafIndex)
}
