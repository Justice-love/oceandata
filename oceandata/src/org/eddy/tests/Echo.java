/**
 * 
 * @creatTime 下午3:38:57
 * @author XuYi
 */
package org.eddy.tests;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;

/**
 * @author XuYi
 * 
 */
public class Echo implements EchoMBean{

	private String str = "789";

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eddy.tests.EchoMBean#print(java.lang.String)
	 */
	@Override
	public String print(String str) {
		return str;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eddy.tests.EchoMBean#getStr()
	 */
	@Override
	public String getStr() {
		// TODO Auto-generated method stub
		return this.str;
	}

	/* (non-Javadoc)
	 * @see org.eddy.tests.EchoMBean#getOA()
	 */
	@Override
	public String getOA() {
		// TODO Auto-generated method stub
		return "123";
	}

//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see javax.management.DynamicMBean#getAttribute(java.lang.String)
//	 */
//	@Override
//	public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
//		return this.str;
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see
//	 * javax.management.DynamicMBean#setAttribute(javax.management.Attribute)
//	 */
//	@Override
//	public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
//		// TODO Auto-generated method stub
//
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see javax.management.DynamicMBean#getAttributes(java.lang.String[])
//	 */
//	@Override
//	public AttributeList getAttributes(String[] attributes) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see
//	 * javax.management.DynamicMBean#setAttributes(javax.management.AttributeList
//	 * )
//	 */
//	@Override
//	public AttributeList setAttributes(AttributeList attributes) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see javax.management.DynamicMBean#invoke(java.lang.String,
//	 * java.lang.Object[], java.lang.String[])
//	 */
//	@Override
//	public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see javax.management.DynamicMBean#getMBeanInfo()
//	 */
//	@Override
//	public MBeanInfo getMBeanInfo() {
//		try {
//			Class cls = this.getClass();
//			// 用反射获得 "upTime" 属性的读方法
//			Method readMethod = cls.getMethod("getStr", new Class[]{});
//			// 用反射获得构造方法
//			Constructor constructor = cls.getConstructor(new Class[] {});
//			// 关于 "upTime" 属性的元信息 : 名称为 UpTime，只读属性 ( 没有写方法 )。
//			MBeanAttributeInfo upTimeMBeanAttributeInfo = new MBeanAttributeInfo("str", "The time span since server start", readMethod, null);
//			// 关于构造函数的元信息
//			MBeanConstructorInfo mBeanConstructorInfo = new MBeanConstructorInfo("Constructor for ServerMonitor", constructor);
//			// ServerMonitor 的元信息，为了简单起见，在这个例子里，
//			// 没有提供 invocation 以及 listener 方面的元信息
//			MBeanInfo mBeanInfo = new MBeanInfo(cls.getName(), "Monitor that controls the server", new MBeanAttributeInfo[] { upTimeMBeanAttributeInfo }, new MBeanConstructorInfo[] { mBeanConstructorInfo }, null, null);
//			return mBeanInfo;
//		} catch (Exception e) {
//			throw new Error(e);
//		}
//
//		
//	}

}
