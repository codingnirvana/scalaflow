package me.juhanlol.dataflow

import java.lang.reflect.{Modifier, Method}
import java.net.URI

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.dataflow.sdk.coders._
import com.google.cloud.dataflow.sdk.values.{TimestampedValue, KV}
import org.joda.time.Instant
import scala.collection.mutable
import scala.reflect.runtime.universe._

trait CoderFactory {
  def create[_](typeArgumentCoders: List[Coder[_]]): Coder[_]
}

object CoderFactory {
  def of(clazz: Class[_], method: Method): CoderFactory = {
    new CoderFactory {
      override def create[_](coders: List[Coder[_]]): Coder[_] = {
        // TODO catch errors
        method.invoke(null, coders.toArray:_*).asInstanceOf[Coder[_]]
      }
    }
  }
}

class CoderRegistry {
  val coderMap = mutable.Map[Type, CoderFactory]()

  // register common types
  // TODO byte[] coder ???
  registerCoder[Int](classOf[VarIntCoder])
  registerCoder[java.lang.Integer](classOf[VarIntCoder])
  registerCoder[Long](classOf[VarLongCoder])
  registerCoder[java.lang.Long](classOf[VarLongCoder])
  registerCoder[Double](classOf[DoubleCoder])
  registerCoder[java.lang.Double](classOf[DoubleCoder])
  registerCoder[String](classOf[StringUtf8Coder])
  registerCoder[java.lang.String](classOf[StringUtf8Coder])
  registerCoder[Instant](classOf[InstantCoder])
  registerCoder[java.lang.Void](classOf[VoidCoder])
  registerCoder[TimestampedValue[_]](classOf[TimestampedValue.TimestampedValueCoder[_]])

  registerCoder[TableRow](classOf[TableRowJsonCoder])

  registerCoder[KV[_, _]](classOf[KvCoder[_, _]])
  registerCoder[java.lang.Iterable[_]](classOf[IterableCoder[_]])
  registerCoder[java.util.List[_]](classOf[ListCoder[_]])
 // registerCoder[java.net.URI](classOf[StringDelegateCoder[URI]])

  def registerCoder[T: TypeTag](coderClazz: Class[_]): Unit = {
    val resolvedInfo = TypeResolver.resolve[T]()
    val numTypeArgs = resolvedInfo.numArgs

    val factoryMethodArgTypes = Array.fill(numTypeArgs)(classOf[Coder[_]])

    var factoryMethod: Method = null
    try {
      factoryMethod = coderClazz.getDeclaredMethod(
        "of", factoryMethodArgTypes: _*)
    } catch {
      case exn @ (_: NoSuchMethodException | _: SecurityException) =>
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "does not have an accessible method named 'of' with "
            + numTypeArgs + " arguments of Coder type", exn)
    }

    if (!Modifier.isStatic(factoryMethod.getModifiers)) {
      throw new IllegalArgumentException(
          "cannot register Coder " + coderClazz + ": "
          + "method named 'of' with " + numTypeArgs
          + " arguments of Coder type is not static")
    }
    if (!coderClazz.isAssignableFrom(factoryMethod.getReturnType)) {
      throw new IllegalArgumentException(
          "cannot register Coder " + coderClazz + ": "
          + "method named 'of' with " + numTypeArgs
          + " arguments of Coder type does not return a " + coderClazz)
    }
    try {
      if (!factoryMethod.isAccessible()) {
        factoryMethod.setAccessible(true)
      }
    } catch {
      case exn: SecurityException =>
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "method named 'of' with " + numTypeArgs
            + " arguments of Coder type is not accessible", exn)
    }

    coderMap.put(resolvedInfo.raw, CoderFactory.of(coderClazz, factoryMethod))
  }

  def getDefaultCoder[T: TypeTag]: Coder[T] = {
    val tpe = typeOf[T]
    getDefaultCoder(tpe).asInstanceOf[Coder[T]]
  }

  def getDefaultCoder(tpe: Type): Coder[_] = {
    tpe match {
      case TypeRef(_, _, args) =>
        val typeArgCoders = args.map(getDefaultCoder(_))
        coderMap(tpe.erasure).create(typeArgCoders)
      case _ => coderMap(tpe.erasure).create(Nil)
    }
  }
}