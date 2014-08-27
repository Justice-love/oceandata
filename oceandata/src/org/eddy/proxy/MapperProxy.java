/**
 * 
 * @creatTime 上午10:03:18
 * @author XuYi
 */
package org.eddy.proxy;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;

import org.eddy.datasource.ConnectionTemplate;
import org.eddy.exception.HbaseException;
import org.eddy.register.ProxyMethod;

/**
 * @author XuYi
 *
 */
public class MapperProxy<T> implements InvocationHandler, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5237565435682212561L;

	Map<String, ProxyMethod> proxyMethods;
	ConnectionTemplate connection;
	
	public MapperProxy(Map<String, ProxyMethod> proxyMethods) {
		super();
		this.proxyMethods = proxyMethods;
		this.connection = new ConnectionTemplate();
	}



	/* (non-Javadoc)
	 * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		ProxyMethod proxyMethod = proxyMethods.get(method.getName());
		if (Object.class.equals(method.getDeclaringClass())) {
			return method.invoke(this, args);
		}
		if (null == proxyMethod) {
			throw new HbaseException("未正确定义的方法, method: " + method.getName());
		}
		return proxyMethod.execute(connection, args);
	}
	
}
