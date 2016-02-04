package com.stripe.brushfire.training

import com.stripe.brushfire._

case class ConfusionMatrix(
  truePositives: Double,
  trueNegatives: Double,
  falsePositives: Double,
  falseNegatives: Double) {

  def positives = truePositives + falseNegatives
  def negatives = trueNegatives + falsePositives

  def sensitivity = truePositives / positives
  def recall = sensitivity
  def truePositiveRate = sensitivity

  def specificity = trueNegatives / negatives
  def trueNegativeRate = specificity

  def precision = truePositives / (truePositives + falsePositives)
  def positivePredictiveValue = precision

  def negativePredictiveValue = trueNegatives / (trueNegatives + falseNegatives)

  def falsePositiveRate = falsePositives / negatives

  def falseDiscoveryRate = 1.0 - precision

  def falseNegativeRate = falseNegatives / positives

  def accuracy = (truePositives + trueNegatives) / (positives + negatives)

  def f1 = (2 * truePositives) / (2 * truePositives + falsePositives + falseNegatives)
}
