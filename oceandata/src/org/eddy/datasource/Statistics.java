/**
 * 
 * @creatTime 下午4:33:14
 * @author XuYi
 */
package org.eddy.datasource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

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
public class Statistics implements StatisticsMBean, DynamicMBean{

	public final AtomicLong connectionsRequested = new AtomicLong(0);
	
	public final AtomicLong connectionsRelease = new AtomicLong(0);
	
	public final AtomicLong cacheInts = new AtomicLong(0);
	
	public final AtomicLong cacheMiss = new AtomicLong(0);
	
	public void incrementConnectionsRequested() {
		connectionsRequested.incrementAndGet();
	}
	
	public void incrementConnectionsRelease() {
		connectionsRelease.incrementAndGet();
	}
	
	public void incrementCacheInts() {
		cacheInts.incrementAndGet();
	}
	
	public void incrementCacheMiss() {
		cacheMiss.incrementAndGet();
	}

	/* (non-Javadoc)
	 * @see javax.management.DynamicMBean#getAttribute(java.lang.String)
	 */
	@Override
	public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
		if ("connections requested".equals(attribute)) {
			return this.connectionsRequested.get();
		} else if ("Connections Release".equals(attribute)){
			return this.connectionsRelease.get();
		} else if ("cache ints".equals(attribute)) {
			return this.cacheInts.get();
		} else if ("Cache Miss".equals(attribute)) {
			return this.cacheMiss.get();
		} else {
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see javax.management.DynamicMBean#setAttribute(javax.management.Attribute)
	 */
	@Override
	public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see javax.management.DynamicMBean#getAttributes(java.lang.String[])
	 */
	@Override
	public AttributeList getAttributes(String[] attributes) {
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.management.DynamicMBean#setAttributes(javax.management.AttributeList)
	 */
	@Override
	public AttributeList setAttributes(AttributeList attributes) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.management.DynamicMBean#invoke(java.lang.String, java.lang.Object[], java.lang.String[])
	 */
	@Override
	public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.management.DynamicMBean#getMBeanInfo()
	 */
	@Override
	public MBeanInfo getMBeanInfo() {
		try {
			Class<?> cls = this.getClass();
			Constructor<?> constructor = cls.getConstructor(new Class[] {});
			Method readConnectionsRequested = cls.getMethod("readConnectionsRequested", new Class[]{});
			Method readConnectionsRelease = cls.getMethod("readConnectionsRelease", new Class[]{});
			Method readCacheInts = cls.getMethod("readCacheInts", new Class[]{});
			Method readCacheMiss = cls.getMethod("readCacheMiss", new Class[]{});
			MBeanAttributeInfo connectionsRequested = new MBeanAttributeInfo("connections requested", "connections requested", readConnectionsRequested, null);
			MBeanAttributeInfo connectionsRelease = new MBeanAttributeInfo("Connections Release", "Connections Release", readConnectionsRelease, null);
			MBeanAttributeInfo cacheInts = new MBeanAttributeInfo("cache ints", "cache ints", readCacheInts, null);
			MBeanAttributeInfo cacheMiss = new MBeanAttributeInfo("Cache Miss", "Cache Miss", readCacheMiss, null);
			MBeanConstructorInfo mBeanConstructorInfo = new MBeanConstructorInfo("Constructor for Statistics", constructor);
			MBeanInfo mBeanInfo = new MBeanInfo(cls.getName(), "Monitor that controls the server", new MBeanAttributeInfo[] { connectionsRequested, connectionsRelease, cacheInts, cacheMiss }, new MBeanConstructorInfo[] {mBeanConstructorInfo}, null, null);
			return mBeanInfo;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see org.eddy.datasource.StatisticsMBean#readConnectionsRequested()
	 */
	@Override
	public String readConnectionsRequested() {
		return this.connectionsRequested.get() + "";
	}

	/* (non-Javadoc)
	 * @see org.eddy.datasource.StatisticsMBean#readConnectionsRelease()
	 */
	@Override
	public String readConnectionsRelease() {
		return this.connectionsRelease.get() + "";
	}

	/* (non-Javadoc)
	 * @see org.eddy.datasource.StatisticsMBean#readCacheInts()
	 */
	@Override
	public String readCacheInts() {
		return this.cacheInts.get() + "";
	}

	/* (non-Javadoc)
	 * @see org.eddy.datasource.StatisticsMBean#readCacheMiss()
	 */
	@Override
	public String readCacheMiss() {
		return this.cacheMiss.get() + "";
	}
}
