package com.stripe.brushfire

import com.twitter.algebird._

object SingleTreeSampler extends Sampler {
  val numTrees = 1
  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = 1
  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int) = true
  def includeFeature(name: String, treeIndex: Int, leafIndex: Int) = true
}

case class KFoldSampler(numTrees: Int) extends Sampler {
  val murmur = MurmurHash128(12345)

  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = {
    val (hash1, hash2) = murmur(id)
    val h = math.abs(hash1).toLong
    if (h % numTrees == treeIndex)
      0
    else
      1
  }

  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int) = {
    timesInTrainingSet(id, timestamp, treeIndex) == 0
  }

  def includeFeature(name: String, treeIndex: Int, leafIndex: Int) = true
}

case class RFSampler(numTrees: Int, featureRate: Double, samplingRate: Double = 1.0)
    extends Sampler {

  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = {
    val rand = random(id, treeIndex)
    //poisson generator, from knuth
    var l = math.exp(-samplingRate)
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
    val (hash1, hash2) = murmur(key)
    new scala.util.Random(hash1)
  }
}

case class TimeGroupedSampler(base: Sampler, period: Long, groups: Int) extends Sampler {
  def numTrees = base.numTrees * groups

  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int) = {
    val timeGroup = (timestamp / period) % groups
    val treeGroup = treeIndex % groups
    val indexInGroup = treeIndex / groups

    if (timeGroup == treeGroup)
      base.timesInTrainingSet(id, timestamp, treeIndex / groups)
    else
      0
  }

  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int) = {
    timesInTrainingSet(id, timestamp, treeIndex) == 0
  }

  def includeFeature(name: String, treeIndex: Int, leafIndex: Int) = base.includeFeature(name, treeIndex, leafIndex)
}

case class OutOfTimeSampler(base: Sampler, threshold: Long) extends Sampler {
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

  def includeFeature(name: String, treeIndex: Int, leafIndex: Int) = base.includeFeature(name, treeIndex, leafIndex)
}
