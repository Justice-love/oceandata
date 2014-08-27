/**
 * 
 * @creatTime 下午5:14:49
 * @author XuYi
 */
package org.eddy.register;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.eddy.annotation.HbaseCommond;
import org.eddy.annotation.Table;
import org.eddy.datasource.ConnectionTemplate;
import org.eddy.exception.HbaseException;
import org.eddy.filter.MineFilter;

/**
 * @author XuYi
 * 
 */
public class ProxyMethod {

	private final MethodSignature method;
	private HbaseCommond commond;
	private Map<Class<? extends Annotation>, Annotation> annotations;
	private Table table;
	
	public ProxyMethod(HbaseCommond commond, Method method, Map<Class<? extends Annotation>, Annotation> annotations, Table table) {
		super();
		this.method = new MethodSignature(method);
		this.commond = commond;
		this.annotations = annotations;
		this.table = table;
	}

	/**
	 * 
	 * @param connection
	 * @param args
	 * @return
	 * @creatTime 上午10:27:48
	 * @author XuYi
	 */
	public Object execute(ConnectionTemplate connection, Object[] args) {
		Map<String, Object> param, insertColums;
		List<String> keys;
		switch (commond) {
		case INSERT:
			param = method.convertArgsToCommandParam(args);
			keys = method.convertArgsToKey(table, param);
			insertColums = method.convertArgsToInsertColum(param);
			connection.put(keys, insertColums, table);
			return null;
		case SELECT_ONE:
			param = method.convertArgsToCommandParam(args);
			keys = method.convertArgsToKey(table, param);
			List<MineFilter> filters = method.createFilters(annotations, param);
			return connection.selectOne(annotations, param, keys, table, filters);
		case SELECT_MANY:
			param = method.convertArgsToCommandParam(args);
			keys = method.convertArgsToKey(table, param);
			List<MineFilter> fi = method.createFilters(annotations, param);
			return connection.selectMany(annotations, param, keys, table, fi);
		default:
			throw new HbaseException("不正确的执行类型, commond: " + commond);
		}
	}

}
