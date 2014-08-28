/**
 * 
 * @creatTime 上午11:08:03
 * @author XuYi
 */
package org.eddy.exsample;

import java.util.List;

import org.eddy.annotation.Key;
import org.eddy.annotation.Param;
import org.eddy.annotation.Table;

/**
 * @author XuYi
 *
 */
@Table(name="today", mix="_")
public interface EddyTest {

//	@Key({"key1","key2"})
//	@SelectColum({"f1:d1"})
//	List<Object> selectAll(@Param("key1") String key1, @Param("key2") String key2);
//	
//	@InsertColum(colum={"f1:d1", "f2:d2", "f3:d3"}, paramKey={"param1", "param2", "param3"})
//	@Key({"key1"})
//	void insert(@Param("param1") String v1, @Param("param2") String v2, @Param("param3") String v3, @Param("key1") String key1);
//	
//	@Key({"key1","key2"})
//	@SelectColum({"f1:d1"})
//	@Like(colum="f1:d1", paramKey="like")
//	List<Object> selectAll2(@Param("key1") String key1, @Param("key2") String key2, @Param("like") String like);
//	
//	@Key({"key1","key2"})
//	@SelectColum({"f1:d1"})
//	@Equal(colum="f1:d1", paramKey="equal")
//	List<Object> selectAll3(@Param("key1") String key1, @Param("key2") String key2, @Param("equal") String equal);
	
	@Key({"key1"})
	List<Object> getOne(@Param("key1") String key1);
}
