package com.cpinfo.his.web.etlMangager.utils;
import com.cpinfo.his.web.etlMangager.db.DBOperator;
import com.cpinfo.his.web.etlMangager.model.FiledMapper;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class DbUtils {
	public static void mapParseObject(Object object, Map map) throws Exception {
		List<Field> fields = new ArrayList<Field>();
		Class clazz = object.getClass();
		while(clazz!=null){
			fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
			clazz = clazz.getSuperclass();
		}
		for (Field field : fields) {
			if (map.get(field.getName()) != null) {
				BeanUtils.setProperty(object,
						field.getName(), map.get(field.getName()));
			} else {
				boolean hasAnnotation = field.isAnnotationPresent(FiledMapper.class);
				if (hasAnnotation) {
					FiledMapper annotation = field.getAnnotation(FiledMapper.class);
					org.apache.commons.beanutils.BeanUtils.setProperty(object, field.getName(), map.get(annotation.filed()));
				}
			}

		}
	}

	public static List<Object[]> ListMapToListObject(List<Map<String, Object>> list) {
		List<Object[]> result = new ArrayList<Object[]>();
		for (Map<String, Object> map : list) {
			Object[] objects = new Object[map.size()];
			int i = 0;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				objects[i++] = entry.getValue();
			}
			result.add(objects);
		}
		return result;
	}
//	public static List<Object[]> ListMapToListObject(List<Map<String, Object>> list) {
//		List<Object[]> result = new ArrayList<Object[]>();
//		for (int i=0 ; i<list.size();i++) {
//			Object[] objects = new Object[((Map)list.get(i)).size()];
//			for (Map.Entry<String, Object> entry : ((Map<String ,Object>)list.get(i)).entrySet()) {
//				objects[i++] = entry.getValue();
//			}
//			result.add(objects);
//		}
//		return result;
//	}
	

	public static String generateGets(String input, String prefix) {
		String[] fields = input.split(",");
		StringBuilder sb = new StringBuilder("");
		for (String field : fields) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			if (prefix != null && !"".equals(prefix)) {
				sb.append(prefix);
				sb.append(".");
			}
			sb.append("get");
			String s = field.toLowerCase();
			s = s.replaceFirst(s.substring(0, 1), s.substring(0, 1)
					.toUpperCase());
			sb.append(s);
			sb.append("()");
		}
		return sb.toString();
	}
	
	/**
	 * 生成一个get方法
	 * @param str
	 * @return
	 */
	private static String changeGetStr(String str){
		if(StrUtil.strIsNotEmpty(str)){
			return "get" + str.substring(0,1).toUpperCase() +str.substring(1).toLowerCase() + "()";
		}else{
			return str;
		}
	}
	
	/**
	 * 生成并打印插入或修改的执行语句
	 * @param tableName
	 * @param objName
	 * @param dbName
	 */
	@SuppressWarnings("unchecked")
	public void printInsertSql(String tableName, String objName, String dbName){
		DBOperator db = null;
		List tableList = null;
		try{
			db = new DBOperator();
			String sql = "select table_name, column_name from user_tab_cols where table_name = upper(?)";
			tableList = db.find(sql, new Object[]{tableName});
		}catch(Exception e){
			db.rollback();
		}finally{
			db.freeCon();
		}
		if(ListUtil.listIsNotEmpty(tableList)){
			StringBuffer insertFront = new StringBuffer(dbName+".excute(\"insert into " + tableName + "(");
			StringBuffer insertBack = new StringBuffer(" values(");
			StringBuffer insertObj = new StringBuffer(",new Object[]{");
			
			StringBuffer updateFront = new StringBuffer(dbName+".excute(\"update " + tableName + " set ");
			int size = tableList.size();
			for(int i = 0; i < size; i ++){
				Map map = (Map)tableList.get(i);
				String columnName = (String)map.get("column_name");
				if(i == size -1){
					insertFront.append(columnName + ")");							//新增--SQL列
					insertBack.append("?)\"");										//新增--SQL匹配符
					updateFront.append(columnName + "=? where 1!= 1\"");					//修改--SQL列
					insertObj.append(objName+"."+changeGetStr(columnName)+"});");		//对应的列取值方法
				}else{
					insertFront.append(map.get("column_name") + ",");				//新增--SQL列
					insertBack.append("?,");										//新增--SQL匹配符
					updateFront.append(columnName + "=?,");							//修改--SQL列
					insertObj.append(objName+"."+changeGetStr(columnName)+",");		//对应的取值
				}
			}
			insertFront.append(insertBack).append(insertObj);
			updateFront.append(insertObj);
			System.out.println("新增执行语句：");
			System.out.println(insertFront.toString());
			System.out.println("修改执行语句：");
			System.out.println(updateFront.toString());
		}else{
			System.out.println("没有查询到对应的表");
		}
		
	}

	public static void main(String[] args) {
		DbUtils db = new DbUtils();
		db.printInsertSql("preg_assign", "pa", "db");
		//System.out.println(generateGets("hosnum,chgdetailid,invoiceid,itemcode,itemname,recipeid,parentid,qty,unit,unitprice,amt,medlevel,payrate,toself,selfcare,selfpay,discount,gentype,patientid,accountitem,invoiceitem,sheetdept,deptname,sheetward,sheetwardname,sheetdoctor,doctorname,excdept,excdeptname,giventime,givenempid,givenemp,givenwindow,insid","chgDetails"));
	}
}
