/**
 * 
 * @creatTime 下午5:25:52
 * @author XuYi
 */
package org.eddy.execute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.cache.CacheKey;
import org.eddy.cache.LRUCache;
import org.eddy.datasource.HbaseConnection;
import org.eddy.datasource.HbaseDataSource;
import org.eddy.datasource.HbaseConnection.HbaseMethod;
import org.eddy.exception.HbaseException;
import org.eddy.filter.MineFilter;
import org.eddy.util.RowKeyUtil;

/**
 * @author XuYi
 * 
 */
public class ScanExecute implements Executable {

	private HbaseDataSource dataSource;
	private String startRow;
	private String endRow;
	private String[] familay;
	private String[] qualifier;
	private String pattern;
	private List<MineFilter> filters;

	public ScanExecute(HbaseDataSource dataSource, String startRow, String endRow, String[] familay, String[] qualifier, String pattern, List<MineFilter> filters, LRUCache cache) {
		super();
		this.dataSource = dataSource;
		this.startRow = startRow;
		this.endRow = endRow;
		this.familay = familay;
		this.qualifier = qualifier;
		this.pattern = pattern;
		this.filters = filters;
		CacheKey key = HbaseConnection.createCacheKey(startRow + "|" + endRow, familay, qualifier, HbaseMethod.Scan);
		cache.put(key.toString(), this);
		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eddy.execute.Executable#execute()
	 */
	@Override
	public Object execute(HbaseConnection connection) throws IOException {
		HTableInterface table = connection.getTable();
		if (!RowKeyUtil.rowKeyMatch(startRow, pattern) || !RowKeyUtil.rowKeyMatch(endRow, pattern)) {
			throw new HbaseException("行键格式不符合规范:" + pattern);
		}
		ResultScanner resultScanner = null;
		try {
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			if (!StringUtils.isEmpty(startRow)) {
				scan.setStartRow(Bytes.toBytes(startRow));
			}
			if (!StringUtils.isEmpty(endRow)) {
				scan.setStopRow(Bytes.toBytes(endRow));
			}
			if (null != familay && familay.length > 0) {
				for (int i = 0; i < familay.length; i++) {
					if (null != qualifier && qualifier.length == familay.length) {
						scan.addColumn(Bytes.toBytes(familay[i]), Bytes.toBytes(qualifier[i]));
					} else {
						scan.addFamily(Bytes.toBytes(familay[i]));
					}
				}
			}
			List<Filter> list = new ArrayList<Filter>();
			if (null != filters && filters.size() != 0) {
				for (MineFilter f : filters) {
					list.add(f.getFilter());
				}
			}
			if (!list.isEmpty()) {
				FilterList fl = new FilterList(Operator.MUST_PASS_ALL, list);
				scan.setFilter(fl);
			}
			resultScanner = table.getScanner(scan);
			List<LinkedHashMap<String, byte[]>> result = manageResult(resultScanner);
			return result;
		} finally {
			if (null != resultScanner) {
				resultScanner.close();
			}
			connection.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eddy.execute.Executable#getConnection()
	 */
	@Override
	public HbaseDataSource getDataSource() {
		// TODO Auto-generated method stub
		return this.dataSource;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eddy.execute.Executable#manageResult(java.lang.Object)
	 */
	@Override
	public List<LinkedHashMap<String, byte[]>> manageResult(Object obj) throws IOException {
		ResultScanner rs = (ResultScanner) obj;
		if (null == rs) {
			return null;
		}
		List<LinkedHashMap<String, byte[]>> result = new ArrayList<LinkedHashMap<String, byte[]>>();
		Result res = null;
		while ((res = rs.next()) != null) {
			LinkedHashMap<String, byte[]> lm = new LinkedHashMap<String, byte[]>();
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = res.getNoVersionMap();
			for (Entry<byte[], NavigableMap<byte[], byte[]>> entry : map.entrySet()) {
				for (Entry<byte[], byte[]> value : entry.getValue().entrySet()) {
					lm.put(Bytes.toString(entry.getKey()) + ":" + Bytes.toString(value.getKey()), value.getValue());
				}
			}
			result.add(lm);
		}
		return result;
	}

}
