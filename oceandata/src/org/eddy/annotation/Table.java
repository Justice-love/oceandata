/**
 * 
 * @creatTime 下午2:03:35
 * @author XuYi
 */
package org.eddy.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author XuYi
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface Table {

	String name();
	
//	String[] rowKeyColum();
	
	String mix() default "";
	
	String keyPattern() default "";
}
