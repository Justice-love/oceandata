/**
 * 
 * @creatTime 下午4:29:17
 * @author XuYi
 */
package org.eddy.util;

import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.eddy.exception.HbaseException;

/**
 * @author XuYi
 *
 */
public class RowKeyUtil {

	/**
	 * 行键匹配器
	 * @param key
	 * @param pattern
	 * @return false:不匹配 ; true : 匹配
	 * @creatTime 下午4:35:41
	 * @author XuYi
	 */
	public static boolean rowKeyMatch(String key, String pattern) {
		if (StringUtils.isEmpty(pattern) || StringUtils.isEmpty(key)) {
			return true;
		}
		Pattern p = Pattern.compile(pattern);
		Matcher match = p.matcher(key);
		return match.matches();
	}
	
	/**
	 * 生成行键
	 * @param colum
	 * @param mix
	 * @param param
	 * @return
	 * @creatTime 下午3:58:00
	 * @author XuYi
	 */
	public static String createRowkey(String[] colum, String mix, Map<String, Object> param) {
		if (colum.length == 1) {
			Object key = param.get(colum[0]);
			if (null == key) return null;
			if (key instanceof Date) return "" + ((Date)key).getTime();
			else if (key instanceof String) return key.toString();
			else return key.toString();
		} else {
			Object[] keys = new Object[colum.length];
			for (int i = 0; i < colum.length; i++) {
				if (!param.containsKey(colum[i])) {
					return null;
				}
				keys[i] = param.get(colum[i]);
			}
			if (StringUtils.isEmpty(mix)) throw new HbaseException("未定义row key的连接字符");
			StringBuilder buffer = new StringBuilder();
			for (int i = 0; i < keys.length; i++) {
				if (keys[i] instanceof Date) buffer.append(((Date)keys[i]).getTime());
				else if (keys[i] instanceof String) buffer.append(keys[i].toString());
				else buffer.append(keys[i].toString());
				buffer.append(mix);
			}
			buffer.delete(buffer.lastIndexOf(mix), buffer.length() - 1);
			return buffer.toString();
		}
	}
	
	public static void main(String[] args) {
		System.out.println(StringUtils.isEmpty(""));
	}
	
}
