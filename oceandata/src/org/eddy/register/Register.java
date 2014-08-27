/**
 * 
 * @creatTime 下午4:44:30
 * @author XuYi
 */
package org.eddy.register;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eddy.exception.HbaseException;
import org.eddy.proxy.MapperProxy;
import org.eddy.util.ProxyUtil;

/**
 * @author XuYi
 *
 */
public class Register {

	private static Register instance = new Register();
	private ConcurrentHashMap<Class<?>, Object> proxyClass = new ConcurrentHashMap<Class<?>, Object>();
	
	private Register() {}
	
	public static Register getInstance() {
		return instance;
	}
	
	public void register(Class<?>[] glasses) {
		for (Class<?> glass : glasses) {
			Map<String, ProxyMethod> methods = ProxyUtil.parseMethod(glass);
			MapperProxy<?> mapProxy = new MapperProxy<>(methods);
			Object obj = Proxy.newProxyInstance(glass.getClassLoader(), new Class[]{glass}, mapProxy);
			proxyClass.put(glass, obj);
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getRegister(Class<T> glass) {
		if (null == glass) {
			throw new HbaseException("参数不能为空");
		}
		T t = (T) proxyClass.get(glass);
		if (t == null) {
			throw new HbaseException("未找到对应的代理类, 请确认是否注册, class:" + glass.getName());
		}
		return t;
	}
}
