/**
 * 
 * @creatTime 下午5:19:59
 * @author XuYi
 */
package org.eddy.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.eddy.annotation.HbaseCommond;
import org.eddy.annotation.Table;
import org.eddy.exception.HbaseException;
import org.eddy.register.ProxyMethod;

/**
 * @author XuYi
 *
 */
public class ProxyUtil {

	/**
	 * 
	 * @param glass
	 * @return
	 * @creatTime 下午5:20:51
	 * @author XuYi
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, ProxyMethod> parseMethod(Class<?> glass) {
		if (null == glass) {
			throw new HbaseException("注册class为空");
		}
		if (!glass.isAnnotationPresent(Table.class)) {
			throw new HbaseException("未定义Table注解");
		}
		if (!glass.isInterface()) {
			throw new HbaseException("请注册接口, class: " + glass.getName());
		}
		Map<String, ProxyMethod> result = new LinkedHashMap<String, ProxyMethod>();
		Method[] methods = glass.getDeclaredMethods();
		for (Method method : methods) {
			Annotation[] annotations = method.getAnnotations();
			if (!checkAnnotation(annotations)) {
				continue;
			}
			Map<Class<? extends Annotation>, Annotation> anns = new HashMap<Class<? extends Annotation>, Annotation>();
			for (Annotation a : annotations) {
				if (a.getClass().getInterfaces() != null && a.getClass().getInterfaces().length != 1) {
					throw new HbaseException("不正确的注解");
				}
				anns.put((Class<? extends Annotation>) a.getClass().getInterfaces()[0], a);
			}
			Table table = glass.getAnnotation(Table.class);
			Class<?> re = method.getReturnType();
			HbaseCommond commond;
			if (void.class.equals(re)) {
				commond = HbaseCommond.INSERT;
			} else if (Collection.class.isAssignableFrom(re)) {
				commond = HbaseCommond.SELECT_MANY;
			} else {
				commond = HbaseCommond.SELECT_ONE;
			}
			
			result.put(method.getName(), new ProxyMethod(commond, method, anns, table));
		}
		return result;
	}
	
	/**
	 * 验证method上是否添加了指定注解(待实现)
	 * @param annotations
	 * @return true:验证通过;false:验证未通过
	 * @creatTime 上午9:44:51
	 * @author XuYi
	 */
	static boolean checkAnnotation(Annotation[] annotations) {
		return true;
	}
	
}
