package com.google.cloud.dataflow.sdk.transforms

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.values.PCollection

import scala.reflect.ClassTag
import com.twitter.chill.ClosureCleaner


class DList[T: ClassTag](val native: PCollection[T]) {
  def apply[U: ClassTag](trans: PTransform[PCollection[T], PCollection[U]])
  : DList[U] = {
    new DList[U](native.apply(trans))
  }

  def map[U: ClassTag](f: T => U): DList[U] = {
    val func = DList.clean(f)
    val trans = ParDo.of(new SDoFn[T, U]() {
      override def processElement(c: ProcessContext): Unit = {
        c.output(func(c.element()))
      }
    }).named("ScalaMapTransformed")

    new DList[U](native.apply(trans))
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): DList[U] = {
    val func = DList.clean(f)
    val trans = ParDo.of(new SDoFn[T, U]() {
      override def processElement(c: ProcessContext): Unit = {
        val outputs = func(c.element())
        for (o <- outputs) {
          c.output(o)
        }
      }
    }).named("ScalaFlatMapTransformed")
    new DList[U](native.apply(trans))
  }

  def filter(f: T => Boolean): DList[T] = {
    val func = DList.clean(f)
    val trans = ParDo.of(new SDoFn[T, T]() {
      override def processElement(c: ProcessContext): Unit = {
        if (func(c.element())) {
          c.output(c.element())
        }
      }
    }).named("ScalaFilterTransformed")
    new DList[T](native.apply(trans))
  }

  def persist(path: String, name: Option[String] = None): Unit = {
    val trans = TextIO.Write.named(name.getOrElse("Persist")).to(path)
    // TODO how to remove this cast ???
    native.asInstanceOf[PCollection[String]].apply(trans)
  }
}

object DList {
  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner(f)
    f
  }
}