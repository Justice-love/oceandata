/**
 * 
 * @creatTime 下午3:49:41
 * @author XuYi
 */
package org.eddy.datasource;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.cache.LRUCache;
import org.eddy.exception.HbaseException;
import org.eddy.pool.TableFactory;

/**
 * @author XuYi
 * 
 */
public class HbaseDataSource {

	private static Configuration config;
	private HTablePool pool;
	private Statistics statistics = new Statistics();
	private static final HbaseDataSource instance = new HbaseDataSource();
	private long handlTime = 5 * 60 * 1000;
	private int maxSize = 100;
	private LRUCache cache = new LRUCache(maxSize);
	private MBeanServer mbs;
	private static final String MBEAN_HBASE = "org.eddy.datasource:type=Statistics";

	/**
	 * 构造函数
	 * 
	 * @creatTime 下午3:57:06
	 * @author XuYi
	 */
	private HbaseDataSource() {
		// 初始化 Configuration
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master.hadoop,slave1.hadoop,slave2.hadoop");

		// 初始化HTablePool
		pool = new HTablePool(config, maxSize, new TableFactory());
		initJMX();
	}

	/**
	 * 获取datasource实例
	 * 
	 * @return
	 * @creatTime 下午3:57:13
	 * @author XuYi
	 */
	public static HbaseDataSource getInstance() {
		return instance;
	}

	/**
	 * 获取hbase连接
	 * 
	 * @param tableName
	 *            对应的表明
	 * @return
	 * @creatTime 下午4:03:09
	 * @author XuYi
	 */
	public HbaseConnection getConnection(String tableName) {
		if (StringUtils.isEmpty(tableName)) {
			throw new HbaseException("表名不能为空");
		}
		HbaseConnection connection = new HbaseConnection(this, tableName);
		return connection;
	}

	/**
	 * 获取物理连接
	 * 
	 * @param tableName
	 * @return
	 * @creatTime 下午5:30:39
	 * @author XuYi
	 */
	HTableInterface getRealCollection(String tableName) {
		statistics.incrementConnectionsRequested();
		return pool.getTable(Bytes.toBytes(tableName));
	}

	/**
	 * 释放资源
	 * 
	 * @param table
	 * @creatTime 下午5:30:48
	 * @author XuYi
	 * @throws IOException
	 */
	void releaseCollection(HTableInterface table) throws IOException {
		if (null == table) {
			throw new HbaseException("HTable对象为空,无法释放资源");
		}
		statistics.incrementConnectionsRelease();
		table.flushCommits();
		pool.putTable(table);
	}

	/**
	 * 初始化JMX
	 * 
	 * @creatTime 下午3:12:24
	 * @author XuYi
	 */
	protected void initJMX() {
		this.mbs = ManagementFactory.getPlatformMBeanServer();
		try {

			ObjectName name = new ObjectName(MBEAN_HBASE);

			this.mbs.registerMBean(this.statistics, name);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public long getHandlTime() {
		return handlTime;
	}

	public void setHandlTime(long handlTime) {
		this.handlTime = handlTime;
	}

	public int getMaxSize() {
		return this.maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public HTablePool getPool() {
		return pool;
	}

	public void setPool(HTablePool pool) {
		this.pool = pool;
	}

	public Statistics getStatistics() {
		return statistics;
	}

	public void setStatistics(Statistics statistics) {
		this.statistics = statistics;
	}

	public LRUCache getCache() {
		return cache;
	}

	public void setCache(LRUCache cache) {
		this.cache = cache;
	}

}
