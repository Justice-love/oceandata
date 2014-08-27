/**
 * 
 * @creatTime 上午10:30:19
 * @author XuYi
 */
package org.eddy.register;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.eddy.annotation.Equal;
import org.eddy.annotation.InsertColum;
import org.eddy.annotation.IsNull;
import org.eddy.annotation.Key;
import org.eddy.annotation.Like;
import org.eddy.annotation.Param;
import org.eddy.annotation.Table;
import org.eddy.exception.HbaseException;
import org.eddy.filter.EqualFilter;
import org.eddy.filter.LikeFilter;
import org.eddy.filter.MineFilter;
import org.eddy.filter.NullFilter;

/**
 * @author XuYi
 * 
 */
public class MethodSignature {

	private Method method;

	/**
	 * 构造函数
	 * 
	 * @param method
	 * @creatTime 上午10:51:34
	 * @author XuYi
	 */
	public MethodSignature(Method method) {
		super();
		this.method = method;
	}

	/**
	 * 
	 * @param args
	 * @return
	 * @creatTime 上午10:54:08
	 * @author XuYi
	 */
	public Map<String, Object> convertArgsToCommandParam(Object[] args) {
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		Annotation[][] paramAnno = method.getParameterAnnotations();
		for (int i = 0; i < paramAnno.length; i++) {
			if (paramAnno[i] == null || paramAnno[i].length != 1 || !Param.class.isAssignableFrom(paramAnno[i][0].getClass())) {
				continue;
			}
			Param param = (Param) paramAnno[i][0];
			String key = param.value();
			result.put(key, args[i]);
		}
		return result;
	}

	public Map<String, Object> convertArgsToInsertColum(Map<String, Object> param) {
		if (!method.isAnnotationPresent(InsertColum.class)) {
			return null;
		}
		InsertColum insertColum = method.getAnnotation(InsertColum.class);
		String[] colums = insertColum.colum();
		String[] paramKey = insertColum.paramKey();
		if (colums.length != paramKey.length) {
			throw new HbaseException("InsertColum 配置不正确");
		}
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		for (int i = 0; i < colums.length; i++) {
			check(param, paramKey[i]);
			result.put(colums[i], param.get(paramKey[i]));
		}
		return result;
	}

	/**
	 * 
	 * @param args
	 * @param table
	 * @return
	 * @creatTime 上午9:23:56
	 * @author XuYi
	 */
	public List<String> convertArgsToKey(Table table, Map<String, Object> param) {
		if (!method.isAnnotationPresent(Key.class)) {
			return null;
		}
		Key key = method.getAnnotation(Key.class);
		String mix = table.mix();
		String[] values = key.value();
		List<String> keys = new ArrayList<String>(values.length);
		for (String value : values) {
			if (value.indexOf(mix) < 0) {
				convertKeys(new String[]{value}, keys, param, mix);
			} else {
				String[] ks = value.split(mix);
				convertKeys(ks, keys, param, mix);
			}
		}
		
		return keys;
	}

	private void convertKeys(String[] keys, List<String> list, Map<String, Object> param, String mix) {
		if (keys.length == 1) {
			check(param, keys[0]);
			String v = object2String(param.get(keys[0]));
			list.add(v);
		} else {
			StringBuilder buffer = new StringBuilder();
			for (String k : keys) {
				check(param, k);
				buffer.append(object2String(param.get(k))).append(mix);
			}
			buffer.delete(buffer.lastIndexOf(mix), buffer.length());
			list.add(buffer.toString());
		}
	}

	private String object2String(Object o) {
		if (o instanceof Date) {
			return "" + ((Date) o).getTime();
		} else {
			return o.toString();
		}
	}

	private void check(Map<String, Object> param, String key) {
		if (!param.containsKey(key)) {
			throw new HbaseException("未定义key值");
		}
	}

	/**
	 * 
	 * @param result
	 * @param key
	 * @param object
	 * @param table
	 * @creatTime 上午9:32:08
	 * @author XuYi
	 */
	@SuppressWarnings("unused")
	private void convert(Map<String, String> result, String key, Object object, Table table) {
		String mix = table.mix();
		StringBuilder buffer = new StringBuilder();
		if (result.containsKey(key)) {
			String old = result.get(key);
			if (object instanceof Date)
				buffer.append(old).append(mix).append(((Date) object).getTime());
			else
				buffer.append(old).append(mix).append(object.toString());
		} else {
			if (object instanceof Date)
				buffer.append(((Date) object).getTime());
			else
				buffer.append(object.toString());
		}
		result.put(key, buffer.toString());
	}

	/**
	 * 
	 * @param annotations
	 * @param param
	 * @return
	 * @creatTime 下午4:51:01
	 * @author XuYi
	 */
	public List<MineFilter> createFilters(Map<Class<? extends Annotation>, Annotation> annotations, Map<String, Object> param) {
		List<MineFilter> filters = new ArrayList<MineFilter>();
		for (Entry<Class<? extends Annotation>, Annotation> entry : annotations.entrySet()) {
			if (entry.getKey().equals(IsNull.class)) {
				IsNull isNull = (IsNull) entry.getValue();
				MineFilter filter = new NullFilter(isNull.colum());
				filters.add(filter);
			} else if (entry.getKey().equals(Equal.class)) {
				Equal eq = (Equal) entry.getValue();
				MineFilter filter = new EqualFilter(param, eq);
				filters.add(filter);
			} else if (entry.getKey().equals(Like.class)) {
				Like like = (Like) entry.getValue();
				MineFilter filter = new LikeFilter(param, like);
				filters.add(filter);
			}
		}
		return filters;
	}
	
	public static void main(String[] args) {
		StringBuilder buffer = new StringBuilder("123abc");
		System.out.println(buffer.delete(buffer.lastIndexOf("abc"), buffer.length()).toString());
	}

}
