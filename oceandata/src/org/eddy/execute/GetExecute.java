/**
 * 
 * @creatTime 上午10:49:32
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.cache.CacheKey;
import org.eddy.cache.LRUCache;
import org.eddy.datasource.HbaseConnection;
import org.eddy.datasource.HbaseConnection.HbaseMethod;
import org.eddy.datasource.HbaseDataSource;
import org.eddy.exception.HbaseException;
import org.eddy.filter.MineFilter;
import org.eddy.util.RowKeyUtil;

/**
 * @author XuYi
 *
 */
public class GetExecute implements Executable {

	private HbaseDataSource dataSource;
	private String row;
	private String[] familay;
	private String[] qualifier;
	private String pattern;
	private List<MineFilter> filters;
	
	public GetExecute(HbaseDataSource dataSource, String row, String[] familay, String[] qualifier, String pattern, List<MineFilter> filters, LRUCache cache) {
		super();
		this.dataSource = dataSource;
		this.row = row;
		this.familay = familay;
		this.qualifier = qualifier;
		this.pattern = pattern;
		this.filters = filters;
		CacheKey key = HbaseConnection.createCacheKey(row, familay, qualifier, HbaseMethod.Get);
		cache.put(key.toString(), this);
	}

	/* (non-Javadoc)
	 * @see org.eddy.execute.Executable#execute()
	 */
	@Override
	public Object execute(HbaseConnection connection) throws IOException {
		HTableInterface table = connection.getTable();
		if (!RowKeyUtil.rowKeyMatch(row, pattern)) {
			throw new HbaseException("行键不存在或者格式不符合规范  row:" + row + " pattern: " + pattern);
		}
		try {
			Get get = new Get(Bytes.toBytes(row));
			get.setCacheBlocks(true);
			if (null != familay && familay.length > 0) {
				for (int i = 0; i < familay.length; i++) {
					if (null != qualifier && qualifier.length == familay.length && !StringUtils.isEmpty(familay[i]) && !StringUtils.isEmpty(qualifier[i])) {
						get.addColumn(Bytes.toBytes(familay[i]), Bytes.toBytes(qualifier[i]));
					} else if (!StringUtils.isEmpty(familay[i])){
						get.addFamily(Bytes.toBytes(familay[i]));
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
				get.setFilter(fl);
			}
			Result re = table.get(get);
			List<LinkedHashMap<String, byte[]>> result = manageResult(re);
			return result;
		} finally {
			connection.close();
		}
	}

	/* (non-Javadoc)
	 * @see org.eddy.execute.Executable#getConnection()
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
		Result rs = (Result) obj;
		if (null == rs) {
			return null;
		}
		List<LinkedHashMap<String, byte[]>> result = new ArrayList<LinkedHashMap<String, byte[]>>();
		LinkedHashMap<String, byte[]> lm = new LinkedHashMap<String, byte[]>();
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = rs.getNoVersionMap();
		for (Entry<byte[], NavigableMap<byte[], byte[]>> entry : map.entrySet()) {
			for (Entry<byte[], byte[]> value : entry.getValue().entrySet()) {
				lm.put(Bytes.toString(entry.getKey()) + ":" + Bytes.toString(value.getKey()), value.getValue());
			}
		}
		result.add(lm);
		return result;
	}

}
