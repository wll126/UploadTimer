package com.cpinfo.his.web.etlMangager.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Clob;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class StrUtil {
	
	/**
	 * 如果str为目标值goal，则转为默认值def
	 * @param str
	 * @param goal
	 * @param def
	 * @return
	 */
	public static String strGoalToDef(String str, String goal, String def){
		if(goal == null){
			return str == null ? def : str;
		}else{
			return str.equals(goal) ? def : str;
		}
	}
	
	/**
	 * 如果str为null，转为默认值def
	 * @param str
	 * @param def
	 * @return
	 */
	public static String strNullToDef(String str, String def){
		return str == null ? def : str;
	}
	
	/**
	 * 如果str为null，转为空值
	 * @param str
	 * @return
	 */
	public static String strNullToEmpty(String str){
		return strNullToDef(str, "");
	}
	
	/**
	 * 判断字符串不为空
	 * @param str
	 * @return
	 */
	public static boolean strIsNotEmpty(String str){
		if(str == null || str.equals("")){
			return false;
		}else{
			return true;
		}
	}
	
	/**
	 * 判断一个数组不为空
	 * @param array
	 * @return
	 */
	public static boolean strArrayIsNotEmpty(Object[] array){
		return array == null || array.length == 0 ? false : true;
	}
	
	/**
	 * 判断一个字符串是否在数组里面
	 * @param str
	 * @param array
	 * @return
	 */
	public static boolean strIsInArrary(String str, String[] array){
		if(StrUtil.strIsNotEmpty(str) && StrUtil.strArrayIsNotEmpty(array)){
			for(int i = 0; i < array.length; i ++){
				if(str.equals(array[i]))
					return true;
			}
		}
		return false;
	}
	
	/**
	 * 如果str在list中存在，则返回def，否则返回other
	 * @param str
	 * @param list
	 * @param def
	 * @param other
	 * @return
	 */
	public static String strInListDef(String str, List<String> list, String def, String other){
		if(StrUtil.strIsNotEmpty(str) && ListUtil.listIsNotEmpty(list)){
			for(Iterator<String> iterator = list.iterator(); iterator.hasNext();){
				if(str.equals(iterator.next()))
					return def;
			}
		}
		return other;
	}
	
	/**
	 * 如果str在list中存在，则返回def，否则返回other
	 * @param str
	 * @param list
	 * @return
	 */
	public static boolean strCheckInList(String str, List<String> list){
		if(StrUtil.strIsNotEmpty(str) && ListUtil.listIsNotEmpty(list)){
			for(Iterator<String> iterator = list.iterator(); iterator.hasNext();){
				if(str.equals(iterator.next()))
					return true;
			}
		}
		return false;
	}
	
	/**
	 * 汉字解码（utf-8）
	 * @param str
	 * @return
	 * @throws java.io.UnsupportedEncodingException
	 */
	public static String strDecodeUTF8(String str) throws UnsupportedEncodingException {
		if(StrUtil.strIsNotEmpty(str)){
			return URLDecoder.decode(str, "utf-8");
		}
		return str;
	}
	
	/**
	 * 将str根据format格式转化成时间
	 * @param str
	 * @param format
	 * @return
	 */
	public static Date strToDate(String str, String format){
		if(str == null){
			return null;
		}
		SimpleDateFormat sf = new SimpleDateFormat(format);
		try {
			return sf.parse(str);
		} catch (ParseException e) {
			return null;
		}
	}
	
	/**
	 * yyyy-MM-dd格式转成时间
	 * @param str
	 * @return
	 */
	public static Date strToDate(String str){
		return strToDate(str, "yyyy-MM-dd");
	}
	
	/**
	 * 将date根据format格式转化成字符串
	 * @param date
	 * @param format
	 * @return
	 */
	public static String dateToStr(Date date, String format){
		if(date == null){
			return null;
		}
		SimpleDateFormat sf = new SimpleDateFormat(format);
		return sf.format(date);
	}
	
	/**
	 * 转成yyyy-MM-dd格式字符串
	 * @param date
	 * @return
	 */
	public static String dateToStr(Date date){
		return dateToStr(date, "yyyy-MM-dd");
	}
	
	/**
	 * 转成yyyyMMdd格式字符串
	 * @param date
	 * @return
	 */
	public static String dateYYMMDD(Date date){
		return dateToStr(date, "yyyyMMdd");
	}
	
	/**
	 * format1格式的时间字符串转成format2格式
	 * @param str
	 * @param format1
	 * @param format2
	 * @return
	 */
	public static String dateFormat(String str, String format1, String format2){
		if(str == null){
			return null;
		}
		SimpleDateFormat sf = new SimpleDateFormat(format1);
		SimpleDateFormat sf1 = new SimpleDateFormat(format2);
		try {
			Date date = sf.parse(str);
			return sf1.format(date);
		} catch (ParseException e) {
			return null;
		}
	}
	
	/**
	 * yyyyMMdd格式转成yyyy-MM-dd
	 * @param str
	 * @return
	 */
	public static String dateFormat(String str){
		return dateFormat(str, "yyyyMMdd", "yyyy-MM-dd");
	}
	
	/**
	 * 地址编码去0
	 * @param s
	 * @return
	 */
	public static String delZero(String s){
		s=strNullToEmpty(s);
		s=s.trim();
		if(s.length() < 12){
			return s;
		}
		if(s.substring(2, 12).equals("0000000000")){
			return s.substring(0,2);
		}else if(s.substring(4, 12).equals("00000000")){
			return s.substring(0,4);
		}else if(s.substring(6, 12).equals("000000")){
			return s.substring(0,6);
		}else if(s.substring(9, 12).equals("000")){
			return s.substring(0,9);
		}else{
			return s;
		}
	}
	
	public static String dateMonthDelZero(String str){
		if(str == null || "".equals(str)){
			return "1";
		}
		if(str.length() >= 2){
			str = str.substring(1, 2);
		}
		return str;
	}
	
	public static String dateMonthAddZero(String str){
		if(str == null || "".equals(str)){
			return "";
		}
		if(str.length() <= 1){
			str = "0" + str;
		}
		return str;
	}
	
	public static String dateDayDelZero(String str){
		if(str == null || "".equals(str)){
			return "1";
		}
		if(str.length() >= 2){
			str = str.substring(1, 2);
		}
		return str;
	}
	
	public static String dateDayAddZero(String str){
		if(str == null || "".equals(str)){
			return "";
		}
		if(str.length() <= 1){
			str = "0" + str;
		}
		return str;
	}
	
	/**
	 * Clob 转String
	 * @param clob
	 * @return
	 * @throws Exception
	 */
	public static String oracleClob2Str(Clob clob) throws Exception {
		return (clob != null ? clob.getSubString(1, (int) clob.length()) : null);
	}
}
