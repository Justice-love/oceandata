/**
 * 
 * @creatTime 上午10:29:46
 * @author XuYi
 */
package org.eddy.datasource;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.exception.HbaseException;
import org.eddy.execute.Executable;

/**
 * @author XuYi
 *
 */
public class HandleExceute {

	/**
	 * 执行函数
	 * @param executable
	 * @param connection
	 * @return
	 * @creatTime 上午11:52:45
	 * @author XuYi
	 */
	protected Object excute(Executable executable, HbaseConnection connection) {
		try {
			return executable.execute(connection);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new HbaseException("执行时I/O读写异常", e);
		}
	}
	
	/**
	 * 获取唯一值
	 * @param list
	 * @return
	 * @creatTime 上午11:52:55
	 * @author XuYi
	 */
	protected LinkedHashMap<String, byte[]> returnOneElementFromCollection(List<LinkedHashMap<String, byte[]>> list) {
		if (list == null || list.size() != 1) {
			throw new HbaseException("返回集合值不为1 result: " + list);
		}
		return list.get(0);
	}
	
	/**
	 * 将字节map转成字符串map
	 * @param map
	 * @return
	 * @creatTime 下午1:48:45
	 * @author XuYi
	 */
	protected LinkedHashMap<String, String> byteMap2StringMap(LinkedHashMap<String, byte[]> map) {
		LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
		for (Entry<String, byte[]> entry : map.entrySet()) {
			result.put(entry.getKey(), Bytes.toString(entry.getValue()));
		}
		return result;
	}
}
