package com.cpinfo.his.web.etlMangager.utils;

import java.util.List;

public class ListUtil {
	
	/**
	 * 判断list不为空
	 * @param list
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static boolean listIsNotEmpty(List list){
		if(list != null && list.size() >= 1){
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * 提取list的第一行数据
	 * @param list
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Object distillFirstRow(List list){
		if(listIsNotEmpty(list)){
			return list.get(0);
		}else{
			return null;
		}
	}
}
