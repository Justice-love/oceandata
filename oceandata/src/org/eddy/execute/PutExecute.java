/**
 * 
 * @creatTime 上午11:28:02
 * @author XuYi
 */
package org.eddy.execute;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.cache.CacheKey;
import org.eddy.cache.LRUCache;
import org.eddy.datasource.HbaseConnection;
import org.eddy.datasource.HbaseDataSource;
import org.eddy.datasource.HbaseConnection.HbaseMethod;
import org.eddy.exception.HbaseException;
import org.eddy.util.RowKeyUtil;

/**
 * @author XuYi
 *
 */
public class PutExecute implements Executable {

	private HbaseDataSource dataSource;
	private String row;
	private String[] familay;
	private String[] qualifier;
	private String[] value;
	private String pattern;
	
	
	
	public PutExecute(HbaseDataSource dataSource, String row, String[] familay, String[] qualifier, String[] value, String pattern, LRUCache cache) {
		super();
		this.dataSource = dataSource;
		this.row = row;
		this.familay = familay;
		this.qualifier = qualifier;
		this.value = value;
		this.pattern = pattern;
		CacheKey key = HbaseConnection.createCacheKey(row, familay, qualifier, HbaseMethod.Put);
		cache.put(key.toString(), this);
	}

	/* (non-Javadoc)
	 * @see org.eddy.execute.Executable#execute(org.eddy.datasource.HbaseConnection)
	 */
	@Override
	public Object execute(HbaseConnection connection) throws IOException {
		HTableInterface table = connection.getTable();
		if (StringUtils.isEmpty(row) || familay == null || familay.length < 1 || qualifier == null || qualifier.length < 1 || value == null || value.length < 1 || !RowKeyUtil.rowKeyMatch(row, pattern) || familay.length != qualifier.length || familay.length != value.length) {
			throw new HbaseException("插入Hbase失败  row:" + row + " familay: "+ familay +" qualifier: "+ qualifier +" value: "+ value +" pattern: " + pattern);
		}
		try {
			Put put = new Put(Bytes.toBytes(row));
			for (int i = 0; i < familay.length; i++) {
				put.add(Bytes.toBytes(familay[i]), Bytes.toBytes(qualifier[i]), Bytes.toBytes(value[i]));
			}
			table.put(put);
			return null;
		} finally {
			connection.close();
		}
	}

	/* (non-Javadoc)
	 * @see org.eddy.execute.Executable#getDataSource()
	 */
	@Override
	public HbaseDataSource getDataSource() {
		return this.dataSource;
	}

	/* (non-Javadoc)
	 * @see org.eddy.execute.Executable#manageResult(java.lang.Object)
	 */
	@Override
	public List<LinkedHashMap<String, byte[]>> manageResult(Object obj) throws IOException {
		return null;
	}

}
