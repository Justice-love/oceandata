/**
 * 
 * @creatTime 下午3:31:14
 * @author XuYi
 */
package org.eddy.filter;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.exception.HbaseException;

/**
 * @author XuYi
 * 
 */
public class NullFilter implements MineFilter {

	private byte[] familay;
	private byte[] qualifier;

	public NullFilter(String colum) {
		super();
		if (colum == null || colum.indexOf(":") < 0) {
			throw new HbaseException("不正确的列定义, colum:" + colum);
		}
		String[] arr = colum.split(":");
		this.familay = Bytes.toBytes(arr[0]);
		this.qualifier = Bytes.toBytes(arr[1]);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eddy.filter.MineFilter#getFilter()
	 */
	@Override
	public Filter getFilter() {
		SingleColumnValueFilter filter = new SingleColumnValueFilter(familay, qualifier, CompareOp.EQUAL, Bytes.toBytes(""));
		filter.setFilterIfMissing(true);
		return filter;
	}
}
