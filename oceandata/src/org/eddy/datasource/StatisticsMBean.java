/**
 * 
 * @creatTime 下午3:44:26
 * @author XuYi
 */
package org.eddy.datasource;

/**
 * @author XuYi
 *
 */
public interface StatisticsMBean {
	void incrementConnectionsRequested();
	
	void incrementConnectionsRelease();
	
	void incrementCacheInts();
	
	void incrementCacheMiss();
	
	String readConnectionsRequested();
	
	String readConnectionsRelease();
	
	String readCacheInts();
	
	String readCacheMiss();
	
}
