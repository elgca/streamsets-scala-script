package kw.streamsets.processor.implicits

import java.math.{BigInteger, BigDecimal => JBigDecimal}
import java.sql.Timestamp
import java.time.ZonedDateTime
import java.util.Date

import com.streamsets.pipeline.api.Field

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}

trait Decorators {

  implicit class AsField(any: Any) {
    def asField: Field = any match {
      case s: Field => s
      case s: Int => Field.create(s)
      case s: Long => Field.create(s)
      case s: Boolean => Field.create(s)
      case s: Short => Field.create(s)
      case s: Byte => Field.create(s)
      case s: Float => Field.create(s)
      case s: Double => Field.create(s)
      case s: JBigDecimal => Field.create(s)
      case s: BigDecimal => Field.create(s.bigDecimal)
      case s: BigInteger => Field.create(new JBigDecimal(s.toString))
      case s: BigInt => Field.create(new JBigDecimal(s.toString))
      case s: String => Field.create(s)
      case Symbol(s) => Field.create(s)
      case s: Array[Byte] => Field.create(s)
      case s: Timestamp => Field.createTime(s)
      case s: java.sql.Date => Field.createDate(s)
      case s: ZonedDateTime => Field.createZonedDateTime(s)
      case s: Date => Field.createDate(s)
      case map: Map[_, _] =>
        Field.create {
          map.map {
            case (Symbol(k), v) => k -> v.asField
            case (k: String, v) => k -> v.asField
            case (k, v) => k.toString -> v.asField
          }.asJava
        }
      case s: TraversableOnce[Any] => Field.create(s.map(_.asField).toSeq.asJava)
      case ccs: Product =>
        val names = ccs.getClass.getDeclaredFields.map(x => x.getName)
        if (names.length >= ccs.productArity) {
          names.toIterator.zip(ccs.productIterator).toMap.asField
        } else {
          ccs.productIterator.asField
        }
    }
  }

}
