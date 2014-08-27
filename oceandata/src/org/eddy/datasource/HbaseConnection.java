/**
 * 
 * @creatTime 下午3:53:00
 * @author XuYi
 */
package org.eddy.datasource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.eddy.cache.CacheKey;
import org.eddy.execute.Executable;
import org.eddy.execute.GetExecute;
import org.eddy.execute.PutExecute;
import org.eddy.execute.ScanExecute;
import org.eddy.filter.MineFilter;

/**
 * @author XuYi
 * 
 */
public class HbaseConnection extends HandleExceute{

	private HbaseDataSource dataSource;
	private String tableName;
	private long createdTime;
	private HTableInterface table;
	
	
	public HbaseConnection(HbaseDataSource dataSource, String tableName) {
		super();
		this.dataSource = dataSource;
		this.tableName = tableName;
		table = this.dataSource.getRealCollection(tableName);
		this.createdTime = System.currentTimeMillis();
	}
	
	/**
	 * 获取多条记录
	 * @param startRow
	 * @param endRow
	 * @param familay
	 * @param qualifier
	 * @param pattern
	 * @return
	 * @creatTime 上午10:41:00
	 * @author XuYi
	 */
	@SuppressWarnings("unchecked")
	public List<LinkedHashMap<String, byte[]>> scan(String startRow, String endRow, String[] familay, String[] qualifier, String pattern, List<MineFilter> filters) {
		CacheKey key = HbaseConnection.createCacheKey(startRow + "|" + endRow, familay, qualifier, HbaseMethod.Scan);
		Executable executable = dataSource.getCache().get(key.toString());
		if (null == executable) {
			dataSource.getStatistics().incrementCacheMiss();
			executable = new ScanExecute(this.getDataSource(), startRow, endRow, familay, qualifier, pattern, filters, dataSource.getCache());
			return (List<LinkedHashMap<String, byte[]>>) excute(executable, this);
		} else {
			dataSource.getStatistics().incrementCacheInts();
			return (List<LinkedHashMap<String, byte[]>>) excute(executable, this);
		}
	}
	
	/**
	 * 以字符串形式返回多条数据
	 * @param startRow
	 * @param endRow
	 * @param familay
	 * @param qualifier
	 * @param pattern
	 * @return
	 * @creatTime 下午12:06:56
	 * @author XuYi
	 */
	public List<LinkedHashMap<String, String>> scanString(String startRow, String endRow, String[] familay, String[] qualifier, String pattern, List<MineFilter> filters) {
		List<LinkedHashMap<String, byte[]>> list = scan(startRow, endRow, familay, qualifier, pattern, filters);
		List<LinkedHashMap<String, String>> result = new ArrayList<LinkedHashMap<String, String>>();
		for (LinkedHashMap<String, byte[]> map : list) {
			result.add(byteMap2StringMap(map));
		}
		return result;
	}
	
	/**
	 * 获取单条记录
	 * @param row
	 * @param familay
	 * @param qualifier
	 * @param pattern
	 * @return
	 * @creatTime 上午11:21:05
	 * @author XuYi
	 */
	@SuppressWarnings("unchecked")
	public LinkedHashMap<String, byte[]> get(String row, String familay[], String[] qualifier, String pattern, List<MineFilter> filters) {
		CacheKey key = HbaseConnection.createCacheKey(row, familay, qualifier, HbaseMethod.Get);
		Executable executable = dataSource.getCache().get(key.toString());
		if (null == executable) {
			dataSource.getStatistics().incrementCacheMiss();
			executable = new GetExecute(this.getDataSource(), row, familay, qualifier, pattern, filters, dataSource.getCache());
			List<LinkedHashMap<String, byte[]>> list = (List<LinkedHashMap<String, byte[]>>) excute(executable, this);
			return returnOneElementFromCollection(list);
		} else {
			dataSource.getStatistics().incrementCacheInts();
			List<LinkedHashMap<String, byte[]>> list = (List<LinkedHashMap<String, byte[]>>) excute(executable, this);
			return returnOneElementFromCollection(list);
		}
	}
	
	/**
	 * 以字符串形式返回一条记录
	 * @param row
	 * @param familay
	 * @param qualifier
	 * @param pattern
	 * @return
	 * @creatTime 下午12:06:05
	 * @author XuYi
	 */
	public LinkedHashMap<String, String> getString(String row, String familay[], String[] qualifier, String pattern, List<MineFilter> filters) {
		LinkedHashMap<String, byte[]> map = get(row, familay, qualifier, pattern, filters);
		return byteMap2StringMap(map);
	}
	
	/**
	 * 向hbase中插入一行记录
	 * @param row
	 * @param familay
	 * @param qualifier
	 * @param value
	 * @param pattern
	 * @creatTime 上午11:52:18
	 * @author XuYi
	 */
	public void put(String row, String[] familay, String[] qualifier, String[] value, String pattern) {
		CacheKey key = HbaseConnection.createCacheKey(row, familay, qualifier, HbaseMethod.Put);
		Executable executable = dataSource.getCache().get(key.toString());
		if (null == executable) {
			dataSource.getStatistics().incrementCacheMiss();
			executable = new PutExecute(dataSource, row, familay, qualifier, value, pattern, dataSource.getCache());
			excute(executable, this);
		} else {
			dataSource.getStatistics().incrementCacheInts();
			excute(executable, this);
		}
	}
	

	/**
	 * 生成缓存键
	 * @param startRow
	 * @param endRow
	 * @param familay
	 * @param qualifier
	 * @param scan
	 * @return
	 * @creatTime 下午4:42:07
	 * @author XuYi
	 */
	public static CacheKey createCacheKey(String row, String[] familay, String[] qualifier, HbaseMethod scan) {
		return new CacheKey(row, familay, qualifier, scan);
	}

	public void close() throws IOException {
		this.dataSource.releaseCollection(this.table);
	}
	
	/*get set 方法 */
	public HbaseDataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(HbaseDataSource dataSource) {
		this.dataSource = dataSource;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}
	
	
	public HTableInterface getTable() {
		return table;
	}

	public void setTable(HTableInterface table) {
		this.table = table;
	}


	public static enum HbaseMethod{
		Scan, Get, Put
	}
}


