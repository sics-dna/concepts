package se.sics.concepts.core

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/** Enable Kryo serialization of classes here */
class CKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[CGraph])
    kryo.register(classOf[GraphBuilder])
  }
}
