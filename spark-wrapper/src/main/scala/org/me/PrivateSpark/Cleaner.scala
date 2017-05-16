package org.me.PrivateSpark

import java.lang.reflect.{Modifier}

object Cleaner {

  def enforcePurity[T, U](f : T => U): ((T) => U) = {
    var foo : Class[_] = f.getClass
    // Purge things accessible by enclosure
    while (foo != null) {
      foo.getDeclaredFields.foreach(field => {
        println(field)
        field.setAccessible(true)
        if (!Modifier.isFinal(field.getModifiers)) {
          def fieldName = field.getName
          throw new IllegalArgumentException("Field references non-final parameter " + fieldName)
        }
      })
      foo = foo.getEnclosingClass()
    }
    // Purge things accessible by inheritance
    foo = f.getClass
    while (foo != null) {
      foo.getDeclaredFields.foreach(field => {
        println(field)
        field.setAccessible(true)
        if (!Modifier.isFinal(field.getModifiers)) {
          def fieldName = field.getName
          throw new IllegalArgumentException("Field references non-final parameter " + fieldName)
        }
      })
      foo = foo.getSuperclass()
    }
    f
  }

}
