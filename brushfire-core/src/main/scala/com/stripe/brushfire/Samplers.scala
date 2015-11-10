package com.stripe.brushfire

import com.twitter.algebird._

object SingleTreeSampler extends Sampler[Any, Any] {
  val numTrees = 1
  def timesInTrainingSet(metadata: Any, treeIndex: Int) = 1
  def includeInValidationSet(metadata: Any, treeIndex: Int) = true
  def includeFeature(metadata: Any, name: Any, treeIndex: Int, leafIndex: Int) = true
}

case class KFoldSampler[-M](id: M => String, numTrees: Int) extends Sampler[M, Any] {
  val murmur = MurmurHash128(12345)

  def timesInTrainingSet(metadata: M, treeIndex: Int) = {
    val (hash1, _) = murmur(id(metadata))
    val h = math.abs(hash1)
    if (h % numTrees == treeIndex)
      0
    else
      1
  }

  def includeInValidationSet(metadata: M, treeIndex: Int) = {
    timesInTrainingSet(metadata, treeIndex) == 0
  }

  def includeFeature(metadata: M, name: Any, treeIndex: Int, leafIndex: Int) = true
}

case class RFSampler[-M](id: M => String, numTrees: Int, featureRate: Double, samplingRate: Double = 1.0)
    extends Sampler[M, String] {

  def timesInTrainingSet(metadata: M, treeIndex: Int) = {
    val rand = random(id(metadata), treeIndex)
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

  def includeInValidationSet(metadata: M, treeIndex: Int) = {
    timesInTrainingSet(metadata, treeIndex) == 0
  }

  def includeFeature(metadata: M, name: String, treeIndex: Int, leafIndex: Int) = {
    val seed = (treeIndex << 16) + leafIndex
    random(name, seed).nextDouble < featureRate
  }

  def random(key: String, seed: Int) = {
    val murmur = MurmurHash128(seed)
    val (hash1, _) = murmur(key)
    new scala.util.Random(hash1)
  }
}

case class TimeGroupedSampler[-M, -K](timestamp: M => Long, base: Sampler[M, K], period: Long, groups: Int) extends Sampler[M, K] {
  def numTrees = base.numTrees * groups

  def timesInTrainingSet(metadata: M, treeIndex: Int) = {
    val timeGroup = (timestamp(metadata) / period) % groups
    val treeGroup = treeIndex % groups
    val indexInGroup = treeIndex / groups

    if (timeGroup == treeGroup)
      base.timesInTrainingSet(metadata, indexInGroup)
    else
      0
  }

  def includeInValidationSet(metadata: M, treeIndex: Int) = {
    timesInTrainingSet(metadata, treeIndex) == 0
  }

  def includeFeature(metadata: M, name: K, treeIndex: Int, leafIndex: Int) = {
    base.includeFeature(metadata, name, treeIndex, leafIndex)
  }
}

case class MetadataFilterSampler[-M, -K](base: Sampler[M, K], includeInTraining: M => Boolean) extends Sampler[M, K] {
  def numTrees = base.numTrees

  def timesInTrainingSet(metadata: M, treeIndex: Int) = {
    if (includeInTraining(metadata))
      base.timesInTrainingSet(metadata, treeIndex)
    else
      0
  }

  def includeInValidationSet(metadata: M, treeIndex: Int) = {
    !includeInTraining(metadata)
  }

  def includeFeature(metadata: M, name: K, treeIndex: Int, leafIndex: Int) = {
    base.includeFeature(metadata, name, treeIndex, leafIndex)
  }
}
