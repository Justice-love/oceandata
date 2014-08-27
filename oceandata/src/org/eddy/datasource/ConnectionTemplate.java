/**
 * 
 * @creatTime 上午10:26:04
 * @author XuYi
 */
package org.eddy.datasource;

import java.lang.annotation.Annotation;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.eddy.annotation.SelectColum;
import org.eddy.annotation.Table;
import org.eddy.exception.HbaseException;
import org.eddy.filter.MineFilter;

/**
 * @author XuYi
 *
 */
public class ConnectionTemplate {
	
	private static HbaseDataSource dataSource = HbaseDataSource.getInstance();
	

	/**
	 * 
	 * @param annotations
	 * @param param
	 * @param table
	 * @return
	 * @creatTime 下午4:19:53
	 * @author XuYi
	 */
	public Object selectOne(Map<Class<? extends Annotation>, Annotation> annotations, Map<String, Object> param, List<String> keys, Table table, List<MineFilter> filters) {
		String tableName = table.name();
		String pattern = table.keyPattern();
//		String key = RowKeyUtil.createRowkey(colum, mix, param);
		SelectColum selectColum = (SelectColum) annotations.get(SelectColum.class);
		if (null != keys && keys.size() != 0) {
			if (keys.size() != 1) throw new HbaseException("查询单行数据不可定义多个key");
			String rowkey = keys.get(0);
			if (null == selectColum) {
				return dataSource.getConnection(tableName).getString(rowkey, null, null, pattern, filters);
			} else {
				String[][] colum = createColum(selectColum);
				return dataSource.getConnection(tableName).getString(rowkey, colum[0], colum[1], pattern, filters);
			}
		} else {
			if (null == selectColum) {
				List<LinkedHashMap<String, String>> list = dataSource.getConnection(tableName).scanString(null, null, null, null, pattern, filters);
				if(null != list && list.size() != 1) {
					throw new HbaseException("期待返回一行, 结果返回多行");
				}
				return list == null ? null : list.get(0);
			} else {
				String[][] colum = createColum(selectColum);
				List<LinkedHashMap<String, String>> list = dataSource.getConnection(tableName).scanString(null, null, colum[0], colum[1], pattern, filters);
				if(null != list && list.size() != 1) {
					throw new HbaseException("期待返回一行, 结果返回多行");
				}
				return list == null ? null : list.get(0);
			}
		}
	}
	
	public String[][] createColum(SelectColum selectColum) {
		String[] values = selectColum.value();
		String[] familay = new String[values.length];
		String[] qualifier = new String[values.length];
		int i = 0;
		for (String value : values) {
			if (value.indexOf(":") < 0) {
				continue;
			}
			String[] fq = value.split(":");
			int l = i++;
			familay[l] = fq[0];
			qualifier[l] = fq[1];
		}
		String[][] result = new String[2][];
		result[0] = familay;
		result[1] = qualifier;
		return result;
	}

	/**
	 * 
	 * @param annotations
	 * @param param
	 * @param keys
	 * @param table
	 * @return
	 * @creatTime 上午11:50:47
	 * @author XuYi
	 */
	public Object selectMany(Map<Class<? extends Annotation>, Annotation> annotations, Map<String, Object> param, List<String> keys, Table table, List<MineFilter> filters) {
		String tableName = table.name();
		String pattern = table.keyPattern();
		SelectColum selectColum = (SelectColum) annotations.get(SelectColum.class);
		if (keys != null && keys.size() != 0) {
			String[] startEnd = createKeys(keys);
			if (null == selectColum) {
				return dataSource.getConnection(tableName).scanString(startEnd[0], startEnd[1], null, null, pattern, filters);
			} else {
				String[][] colum = createColum(selectColum);
				return dataSource.getConnection(tableName).scanString(startEnd[0], startEnd[1], colum[0], colum[1], pattern, filters);
			}
		} else {
			if (null == selectColum) {
				return dataSource.getConnection(tableName).scanString(null, null, null, null, pattern, filters);
			} else {
				String[][] colum = createColum(selectColum);
				return dataSource.getConnection(tableName).scanString(null, null, colum[0], colum[1], pattern, filters);
			}
		}
	}

	/**
	 * 
	 * @param keys
	 * @return
	 * @creatTime 下午2:01:17
	 * @author XuYi
	 */
	private String[] createKeys(List<String> keys) {
		String[] result = new String[2];
		if (keys.size() == 1) {
			result[0] = keys.get(0);
			result[1] = null;
		} else {
			result[0] = keys.get(0);
			result[1] = keys.get(1);
		}
		return result;
	}

	/**
	 * 
	 * @param annotations
	 * @param param
	 * @param keys
	 * @param table
	 * @creatTime 下午2:17:35
	 * @author XuYi
	 */
	public void put(List<String> keys, Map<String, Object> insertColums, Table table) {
		String tableName = table.name();
		String pattern = table.keyPattern();
		if (null == keys || keys.size() != 1) {
			throw new HbaseException("不正确的行键, keys:" + keys);
		}
		if (null == insertColums || insertColums.size() < 1) {
			throw new HbaseException("不正确的待插入列");
		}
		String key = keys.get(0);
		String[] familay = new String[insertColums.size()];
		String[] qualifier = new String[insertColums.size()];
		String[] values = new String[insertColums.size()];
		int i = 0;
		for (Entry<String, Object> entry : insertColums.entrySet()) {
			if (entry.getKey().indexOf(":") < 0) {
				continue;
			}
			int j = i++;
			String[] kv = entry.getKey().split(":");
			familay[j] = kv[0];
			qualifier[j] = kv[1];
			if (entry.getValue() == null) {
				throw new HbaseException("插入值为空");
			}
			if (entry.getValue() instanceof Date) values[j] = ((Date)entry.getValue()).getTime() + "";
			else values[j] = entry.getValue().toString();
			
		}
		dataSource.getConnection(tableName).put(key, familay, qualifier, values, pattern);
	}

}
