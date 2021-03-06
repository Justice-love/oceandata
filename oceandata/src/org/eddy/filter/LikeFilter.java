/**
 * 
 * @creatTime 下午4:38:16
 * @author XuYi
 */
package org.eddy.filter;

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.annotation.Like;
import org.eddy.exception.HbaseException;

/**
 * @author XuYi
 * 
 */
public class LikeFilter implements MineFilter {
	private Map<String, Object> param;
	private byte[] familay;
	private byte[] qualifier;
	private String paramKey;

	public LikeFilter(Map<String, Object> param, Like like) {
		super();
		this.param = param;
		String colum = like.colum();
		if (colum == null || colum.indexOf(":") < 0) {
			throw new HbaseException("不正确的列定义, colum:" + colum);
		}
		String[] arr = colum.split(":");
		this.familay = Bytes.toBytes(arr[0]);
		this.qualifier = Bytes.toBytes(arr[1]);
		this.paramKey = like.paramKey();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.eddy.filter.MineFilter#getFilter()
	 */
	@Override
	public Filter getFilter() {
		Object obj = param.get(paramKey);
		if (null == obj) {
			throw new HbaseException("为正确定义匹配的参数 paramKey:" + paramKey);
		}
		String result;
		if(obj instanceof Date) result = "" + ((Date)obj).getTime();
		else  result = obj.toString();
		SingleColumnValueFilter filter = new SingleColumnValueFilter(familay, qualifier, CompareOp.EQUAL, new RegexStringComparator(result));
		filter.setFilterIfMissing(true);
		return filter;
	}

}
