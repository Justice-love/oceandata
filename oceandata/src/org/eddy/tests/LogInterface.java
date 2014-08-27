/**
 * 
 * @creatTime 上午11:41:48
 * @author XuYi
 */
package org.eddy.tests;

import org.eddy.annotation.Key;
import org.eddy.annotation.SelectColum;
import org.eddy.annotation.Table;

/**
 * @author XuYi
 *
 */
@Table(name="manageLog", mix="_")
public interface LogInterface {

//	@SelectColum({"info:message", "info:monitor"})
//	Object getOne(@Key("key1") String k1, @Key("key1")String v1);
}
