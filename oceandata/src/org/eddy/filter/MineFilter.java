/**
 * 
 * @creatTime 下午3:28:48
 * @author XuYi
 */
package org.eddy.filter;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * @author XuYi
 *
 */
public interface MineFilter {

	Filter getFilter();
}
