/**
 * 
 * @creatTime 下午3:25:58
 * @author XuYi
 */
package org.eddy.execute;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import org.eddy.datasource.HbaseConnection;
import org.eddy.datasource.HbaseDataSource;

/**
 * @author XuYi
 *
 */
public interface Executable {

	
	Object execute(HbaseConnection connection) throws IOException;
	
	HbaseDataSource getDataSource();
	
	List<LinkedHashMap<String, byte[]>> manageResult(Object obj) throws IOException;
}
