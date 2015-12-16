package com.stripe

package object brushfire {
  type Tree[K, V, T] = AnnotatedTree[K, V, T, Unit]
}
