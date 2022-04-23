package org.astraea.workloads.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Parameter;

/**
 * This {@link NamedArg} is used to annotate the argument of Workload function. The workload
 * simulation framework access the argument information(argument name & type) by Java Reflection.
 * But the name of each argument was stripped away during the compilation process. If someone
 * accesses the name of any argument by {@link Parameter#getName()}, this will return something like
 * "arg0", which is totally meaningless. To overcome this limitation, one can add the `-parameters`
 * flag during the class compilation to keep the name of argument in classfile. But this will
 * increase the complexity of build tool. Instead, we use this annotation to mark the name of each
 * argument. Also provides some human-friendly description if the author willing to.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface NamedArg {
  String name();

  String description() default "";
}
