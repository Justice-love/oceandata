/**
 * 
 * @creatTime 下午3:21:30
 * @author XuYi
 */
package org.eddy.cache;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.eddy.execute.Executable;

/**
 * @author XuYi
 *
 */
public class LRUCache extends LinkedHashMap<String, Executable> {

	private static final long serialVersionUID = 1L;
	public LRUCache(int maxSize) {
		super(maxSize, 0.75f, true);
	}

	protected boolean removeEldestEntry(Entry<String, Executable> eldest) {
		return size() > eldest.getValue().getDataSource().getMaxSize();
	}
}
