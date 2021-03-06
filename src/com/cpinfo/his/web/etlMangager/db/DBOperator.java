package com.cpinfo.his.web.etlMangager.db;
import com.cpinfo.his.web.etlMangager.utils.DbUtils;
import com.cpinfo.his.web.etlMangager.utils.UUIDGenerator;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class DBOperator {
	private DBConnectionManager db = null;
	private java.sql.PreparedStatement ps = null;
	private java.sql.ResultSet rs = null;
	private Connection con = null;
	private static Map<String, DBOperator> regDBOperators = new HashMap<String, DBOperator>(); // 在用的所有db集合
	private Date regDate = null; // db使用开始时间
	private String uuid = null; // db唯一标识
	private long timeout = 1200; // db使用最长时间 单位：秒
	private boolean active = false;
	private boolean showSql = true; // 是否显示SQL语句
	private String dbname;

	private java.sql.CallableStatement cst = null;

	public DBOperator() throws Exception {
		this("appdb");
	}

	public DBOperator(String dbname) throws Exception {
		this.dbname = dbname;
		initialize();
		//System.out.println("clients数量===========>"+db.getClients());
	}

	/**
	 * 初始化链接
	 *
	 * @throws Exception
	 */
	protected synchronized void initialize() throws Exception {
		db = DBConnectionManager.getInstance();
		con = db.getConnection(this.dbname);
		if (con != null) {
			con.setAutoCommit(false);
			regDate = new Date();
			uuid = ((new UUIDGenerator()).generate()).toString();
			regDBOperators.put(uuid, this);
			active = true;
		}else
		{
			throw new Exception("连接已经释放！  当前连接池数量："+db.getClients());
		}
		//System.out.println("当前连接池数量："+db.getClients());
	}

	/**
	 * 调用存储过程和函数
	 * @param sql
	 * @param params
	 * @param types
	 * @param isOut
	 * @throws Exception
	 */
	public void oracleCall(String sql, Object[] params, int[] types, boolean[] isOut) throws Exception {
		long startTime= System.currentTimeMillis();
		if (showSql) {
//			System.out.print(sql);
			int i = 0;
			if (params != null) {
				String sqls[]=sql.split("\\?");
				StringBuffer sb=new StringBuffer();
				for (int j = 0; j < params.length; j++) {
					sb.append(sqls[j]).append("'").append(params[j]).append("'");
				}
				sb.append(sqls[sqls.length-1]);
				System.out.print("--->"+sb.toString());
				System.out.print(" 参数值 ");
				for (Object obj : params) {
					i++;
					if (obj != null) {
						System.out.print(i + ":" + obj.toString() + " ");
					} else {
						System.out.print(i + ":null ");
					}
				}
			}else{
				System.out.print(sql);
			}
			System.out.println("");
		}
//		if (active == false)
//			throw new Exception("连接已经释放！");
		cst = con.prepareCall(sql);
		if (params != null) {
			int n = params.length;
			if (n != this.getStrNum(sql, "?")) {
				throw new IllegalArgumentException("参数与?号个数不一致");
			}
			cstSet(params, types, isOut);
		}
		cst.execute();
		cstOutSet(params, isOut);
		cst.close();//关闭
		long endTime= System.currentTimeMillis();
		System.out.println("耗时:"+(endTime-startTime)+"毫秒");
	}

	/**
	 * 取得出参的值
	 * @param params
	 *
	 * @param isOut
	 * @throws Exception
	 */
	void cstOutSet(Object[] params, boolean[] isOut) throws Exception {
		for (int i = 0; i < params.length; i++) {
			if(isOut[i] == true){
				if (params[i] instanceof String) {
					params[i] = cst.getString(i + 1);
				} else if (params[i] instanceof Short) {
					params[i] = cst.getShort(i + 1);
				} else if (params[i] instanceof Integer) {
					params[i] = cst.getInt(i + 1);
				} else if (params[i] instanceof Long) {
					params[i] = cst.getLong(i + 1);
				} else if (params[i] instanceof Float) {
					params[i] = cst.getFloat(i + 1);
				} else if (params[i] instanceof Double) {
					params[i] = cst.getDouble(i + 1);
				} else if (params[i] instanceof Timestamp) {
					params[i] = cst.getTimestamp(i + 1);
				} else if (params[i] instanceof java.util.Date) {
					java.sql.Date sqlDate = new java.sql.Date(((java.util.Date) params[i]).getTime());
					params[i] = cst.getDate(i + 1);
				} else{
					System.out.println("error:" + params[i].getClass());
				}
			}
		}
	}

	/**
	 * 设定存储过程和函数的，入参和出参
	 * @param params
	 * @param types
	 * @param isOut
	 * @throws Exception
	 */
	void cstSet(Object[] params, int[] types, boolean[] isOut) throws Exception {
		for (int i = 0; i < params.length; i++) {
			Object obj = params[i];
			if(isOut[i] == true){
				cst.registerOutParameter(i + 1, types[i]);
			}
			if (obj == null) {
				cst.setObject(i + 1, null);
			} else if (obj instanceof String) {
				cst.setString(i + 1, (String) params[i]);
			} else if (obj instanceof Short) {
				cst.setShort(i + 1, (Short) params[i]);
			} else if (obj instanceof Integer) {
				cst.setInt(i + 1, (Integer) params[i]);
			} else if (obj instanceof Long) {
				cst.setLong(i + 1, (Long) params[i]);
			} else if (obj instanceof Float) {
				cst.setFloat(i + 1, (Float) params[i]);
			} else if (obj instanceof Double) {
				cst.setDouble(i + 1, (Double) params[i]);
			} else if (obj instanceof Timestamp) {
				cst.setTimestamp(i + 1, (Timestamp) obj);
			} else if (obj instanceof java.util.Date) {
				java.sql.Date sqlDate = new java.sql.Date(((java.util.Date) params[i]).getTime());
				cst.setDate(i + 1, sqlDate);
			} else if (obj instanceof BigDecimal) {
				cst.setBigDecimal(i + 1, (BigDecimal) params[i]);
			} else {
				System.out.println("error:" + obj.getClass());
			}
		}
	}


	/**
	 * 根据SQL语句得到结果集ResultSet
	 *
	 * @param sql
	 *            查询语句
	 * @return
	 * @throws Exception
	 */
	protected ResultSet select(String sql) throws Exception {
		return select(sql, null);
	}

	/**
	 * 根据SQL语句及条件参数得到结果集ResultSet
	 *
	 * @param sql
	 *            查询语句
	 * @param params
	 *            条件参数
	 * @return
	 * @throws Exception
	 */
	private ResultSet select(String sql, Object[] params) throws Exception {
		long startTime=new java.util.Date().getTime();
		if (showSql) {
//			System.out.println("-->"+sql);
			int i = 0;
			if (params != null) {
				String sqls[]=sql.split("\\?");
					StringBuffer sb=new StringBuffer();
					for (int j = 0; j < params.length; j++) {
						sb.append(sqls[j]).append("'").append(params[j]).append("'");
					}
					if(sqls.length>1){
						sb.append(sqls[sqls.length-1]);
					}
					System.out.print(sb.toString());
				System.out.print(" 参数值 ");
				for (Object obj : params) {
					i++;
					if (obj != null) {
						System.out.print(i + ":" + obj.toString() + " ");
					} else {
						System.out.print(i + ":null ");
					}
				}
			}else{
				System.out.println(sql);
			}
			System.out.println("");
		}
//		if (active == false)
//			throw new Exception("连接已经释放！");
		rs = null;
		ps = con.prepareStatement(sql);
		if (params != null) {
			int n = params.length;
			if (n != this.getStrNum(sql, "?")) {
				throw new IllegalArgumentException("参数与?号个数不一致");
			}
			psSet(params);
		}
		rs = ps.executeQuery();
		long endTime=new java.util.Date().getTime();
		System.out.println("耗时:"+(endTime-startTime)+"毫秒");
		return rs;
	}

	void psSet(Object[] params) throws Exception {
     Object[] parameters=new Object[params.length] ;
		for (int i = 0; i < params.length; i++) {
			Object obj = params[i];
			if (obj == null) {
				ps.setObject(i + 1, null);
                parameters[i]=  null;
			} else if (obj instanceof String) {
				ps.setString(i + 1, (String) params[i]);
                parameters[i]=  (String) params[i];
			} else if (obj instanceof Short) {
				ps.setShort(i + 1, (Short) params[i]);
			} else if (obj instanceof Integer) {
				ps.setInt(i + 1, (Integer) params[i]);
			} else if (obj instanceof Long) {
				ps.setLong(i + 1, (Long) params[i]);
			} else if (obj instanceof Float) {
				ps.setFloat(i + 1, (Float) params[i]);
			} else if (obj instanceof Double) {
				ps.setDouble(i + 1, (Double) params[i]);
                parameters[i]= (Double) params[i];
            } else if (obj instanceof Timestamp) {
				ps.setTimestamp(i + 1, (Timestamp) obj);
			} else if (obj instanceof java.util.Date) {
				java.sql.Date sqlDate = new java.sql.Date(
						((java.util.Date) params[i]).getTime());
				ps.setDate(i + 1, sqlDate);
                parameters[i]= sqlDate;
			} else if (obj instanceof BigDecimal) {
				ps.setBigDecimal(i + 1, (BigDecimal) params[i]);
			} else {
				System.out.println("error:" + obj.getClass());
			}
		}
    }

	/**
	 * 根据条件对数据进行insert、update、delete操作
	 *
	 * @param sql
	 *            操作语句
	 * @param params
	 *            操作条件
	 * @return
	 * @throws Exception
	 */
	private int update(String sql, Object[] params) throws Exception {
		if (showSql) {
			System.out.print(sql);
			int i = 0;
			if (params != null) {
				System.out.print(" 参数值 ");
				for (Object obj : params) {
					i++;
					if (obj != null) {
						System.out.print(i + ":" + obj.toString() + " ");
					} else {
						System.out.print(i + ":null ");
					}
				}
			}
			System.out.println("");
		}
//		if (active == false)
//			throw new Exception("连接已经释放！");
		int num = 0;
		ps = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
		if (params != null) {
			int n = params.length;
			if (n != this.getStrNum(sql, "?")) {
				throw new IllegalArgumentException("参数与?号个数不一致");
			}
			psSet(params);
		}
		num = ps.executeUpdate();
		return num;
	}

	private int[] updateBatch(String sql, Object[][] params) throws Exception {
		if (showSql) {
			System.out.print(sql);
			int i = 0;
			if (params != null) {
				System.out.print(" 参数值 ");
				for (Object obj : params) {
					i++;
					if (obj != null) {
						System.out.print(i + ":" + obj.toString() + " ");
					} else {
						System.out.print(i + ":null ");
					}
				}
			}
			System.out.println("");
		}
//		if (active == false)
//			throw new Exception("连接已经释放！");
		ps = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
		for (Object[] obj : params) {
			if (obj != null) {
				int n = obj.length;
				if (n != this.getStrNum(sql, "?")) {
					throw new IllegalArgumentException("参数与?号个数不一致");
				}
				psSet(obj);
			}
			ps.addBatch();
		}
		return ps.executeBatch();
	}

	/**
	 * 对数据进行insert、update、delete操作
	 *
	 * @param sql
	 *            操作语句
	 * @return
	 * @throws Exception
	 */
	private int update(String sql) throws Exception {
		return update(sql, null);
	}

	public int excute(String sql, Object[] paras) throws Exception {
		long startTime=new java.util.Date().getTime();
		int num = update(sql, paras);
		ps.close();
		long endTime=new java.util.Date().getTime();
		System.out.println("耗时:"+(endTime-startTime)+"毫秒");
		return num;
	}

	public int excute(String sql) throws Exception {
		int num = update(sql);
		ps.close();
		return num;

	}

	public int[] excuteBatch(String sql, Object[][] params) throws Exception {
		int[] num = updateBatch(sql, params);
		ps.close();
		return num;
	}

	/**
	 * 条件参数检验
	 *
	 * @param sql
	 *            原来的字符串
	 * @param str
	 *            其中包含的字符串
	 * @return 原字符从串中包含的字符串的个数
	 */
	private int getStrNum(String sql, String str) {
		int num = 0;
		int index = sql.indexOf(str);
		while (index != -1) {
			num++;
			index = sql.indexOf(str, index + str.length());
		}
		return num;
	}

	public void rollback() {
		try {
			con.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void commit() throws Exception {
		if (!con.getAutoCommit()) {
			con.commit();
		}
	}

	public synchronized void freeCon() {
	   // System.out.println("=======》按里说应该走这个方法的");
		regDBOperators.remove(uuid);
		active = false;
		if (con != null) {
			db.freeConnection(this.dbname, con);
			db.release();
		}

	}

	Map RsToMap(ResultSet rs) throws Exception {
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			String columnname = metaData.getColumnName(i).toLowerCase();
			String keyanme = columnname;
			String temp = keyanme;
			while (true) {
				int index = 1;
				if (map.get(temp) == null) {
					break;
				}
				temp = keyanme + "_" + index;
				index++;
			}
			keyanme = temp;
			int colType = metaData.getColumnType(i);
			switch (colType) {
			case Types.BLOB:
				map.put(keyanme, rs.getBlob(i));
				break;
			case Types.CHAR:
				map.put(keyanme, rs.getString(i));
				break;
			case Types.CLOB:
				map.put(keyanme, rs.getClob(i));
				break;
			case Types.DATE:
//				java.sql.Date sqlDate = rs.getDate(i);// 将java.sql.Date转为java.util.Date
//				if (sqlDate != null) {
//					map.put(keyanme, new java.util.Date(sqlDate.getTime()));
//				}
				Timestamp timestamp = rs.getTimestamp(i);
				if (timestamp != null) {
					java.util.Date sqlDate = new java.util.Date(timestamp.getTime());
					map.put(keyanme,sqlDate);
				}
				break;
			case Types.DECIMAL:
				if (metaData.getScale(i) == 0) {
					map.put(keyanme, rs.getObject(i));
				} else {
					map.put(keyanme, rs.getObject(i) != null ? rs.getDouble(i)
							: rs.getObject(i));
				}
				break;
			case Types.FLOAT:
				map.put(keyanme,
						rs.getObject(i) != null ? rs.getFloat(i) : rs
								.getObject(i));
				break;
			case Types.NUMERIC:
				if (metaData.getScale(i) == 0) {
					map.put(keyanme, rs.getObject(i));
				} else {
					map.put(keyanme, rs.getObject(i) != null ? rs.getDouble(i)
							: rs.getObject(i));
				}
				break;
			case Types.NVARCHAR:
				map.put(keyanme, rs.getString(i));
				break;
			case Types.TIMESTAMP:
				map.put(keyanme, rs.getTimestamp(i));
				break;
			case Types.VARCHAR:
				map.put(keyanme, rs.getString(i));
				break;
			default:
				map.put(keyanme, rs.getString(i));
				break;
			}
		}
		return map;
	}

	public List find(String sql) throws Exception {
		return find(sql, null, 0, null);
	}

	public Object findOne(String sql) throws Exception {
		List result = find(sql, null, 0, null);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Class cla) throws Exception {
		return find(sql, null, 0, cla);
	}

	public Object findOne(String sql, Class cla) throws Exception {
		List result = find(sql, null, 0, cla);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Object[] paras, Class cla) throws Exception {
		return find(sql, paras, 0, cla);
	}

	public Object findOne(String sql, Object[] paras, Class cla)
			throws Exception {
		List result = find(sql, paras, 0, cla);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Object[] paras) throws Exception {
		return find(sql, paras, 0, null);
	}

	public Object findOne(String sql, Object[] paras) throws Exception {
		List result = find(sql, paras, 0, null);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, int size) throws Exception {
		return find(sql, null, size, null);
	}

	public Object findOne(String sql, int size) throws Exception {
		List result = find(sql, null, size, null);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Object para) throws Exception {
		return find(sql, new Object[] { para });
	}

	public Object findOne(String sql, Object para) throws Exception {
		List result = find(sql, new Object[] { para });
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Object para, int size) throws Exception {
		return find(sql, new Object[] { para }, size);
	}

	public Object findOne(String sql, Object para, int size) throws Exception {
		List result = find(sql, new Object[] { para }, size);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Object para, Class cla) throws Exception {
		return find(sql, new Object[] { para }, cla);
	}

	public Object findOne(String sql, Object para, Class cla) throws Exception {
		List result = find(sql, new Object[] { para }, cla);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Object para, int size, Class cla)
			throws Exception {
		return find(sql, new Object[] { para }, size, cla);
	}

	public Object findOne(String sql, Object para, int size, Class cla)
			throws Exception {
		List result = find(sql, new Object[] { para }, size, cla);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Object[] paras, int size) throws Exception {
		return find(sql, paras, size, null);
	}

	public Object findOne(String sql, Object[] paras, int size)
			throws Exception {
		List result = find(sql, paras, size, null);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, int size, Class cla) throws Exception {
		return find(sql, null, size, cla);
	}

	public Object findOne(String sql, int size, Class cla) throws Exception {
		List result = find(sql, null, size, cla);
		if (result != null && result.size() > 0) {
			return result.get(0);
		} else {
			return null;
		}
	}

	public List find(String sql, Object[] paras, int size, Class cla)
			throws Exception {
		List list = new ArrayList();
		ResultSetMetaData metaData = null;
		ResultSet rs;
		if (paras == null) {
			rs = select(sql);
		} else {
			rs = select(sql, paras);
		}
		while (rs.next()) {
			if (cla != null) {
				Object object = cla.newInstance();
				DbUtils.mapParseObject(object, RsToMap(rs));
				list.add(object);
			} else {
				list.add(RsToMap(rs));
			}

			if (size != 0 && list.size() == size) {
				break;
			}

		}// 这里添加释放游标
		rs.close();
		ps.close();
		return list;

	}

	public void printTablesInfo(String user) throws Exception {
		ResultSetMetaData metaData = null;
		ResultSet rs;
		rs = select(
				"select t.table_name from dba_all_tables t where t.owner = ? order by t.table_name",
				new Object[] { user });
		while (rs.next()) {
			System.out.println("表名：" + rs.getString(1).toLowerCase());
			ResultSet resultset = select("select * from "
					+ rs.getString(1).toLowerCase());
			metaData = resultset.getMetaData();
			int columnCount = metaData.getColumnCount();
			System.out.println("属性信息：");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				int colType = metaData.getColumnType(i);
				switch (colType) {
				case Types.BLOB:
					System.out.println("private Blob " + columnname + ";");
					break;
				case Types.CHAR:
					System.out.println("private String " + columnname + ";");
					break;
				case Types.CLOB:
					System.out.println("private Clob " + columnname + ";");
					break;
				case Types.DATE:
					System.out.println("private Date " + columnname + ";");
					break;
				case Types.DECIMAL:
					if (metaData.getScale(i) == 0) {
						System.out.println("private Integer " + columnname
								+ ";");
					} else {
						System.out
								.println("private Double " + columnname + ";");
					}
					break;
				case Types.FLOAT:
					System.out.println("private Float " + columnname + ";");
					break;
				case Types.NUMERIC:
					if (metaData.getScale(i) == 0) {
						System.out.println("private Integer " + columnname
								+ ";");
					} else {
						System.out
								.println("private Double " + columnname + ";");
					}
					break;
				case Types.NVARCHAR:
					System.out.println("private String " + columnname + ";");
					break;
				case Types.TIMESTAMP:
					System.out.println("private Timestamp " + columnname + ";");
					break;
				case Types.VARCHAR:
					System.out.println("private String " + columnname + ";");
					break;
				default:
					System.out.println("private String " + columnname + ";");
					break;
				}
			}
			System.out.println("SQL语句：");
			System.out.println("select * from " + rs.getString(1).toLowerCase()
					+ " t");
			System.out.print("select ");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				if (i == columnCount) {
					System.out.print("t." + columnname);
				} else {
					System.out.print("t." + columnname + ",");
				}
			}
			System.out.print("from " + rs.getString(1).toLowerCase() + " t");
			System.out.println("");
			System.out.print("insert into " + rs.getString(1).toLowerCase()
					+ " (");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				if (i == columnCount) {
					System.out.print(columnname);
				} else {
					System.out.print(columnname + ",");
				}
			}
			System.out.print(") values (");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				if (i == columnCount) {
					System.out.print("?");
				} else {
					System.out.print("?" + ",");
				}
			}
			System.out.print(")");
			System.out.println("");
			System.out.print("update " + "t." + rs.getString(1).toLowerCase()
					+ " set ");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				if (i == columnCount) {
					System.out.print(columnname + "=?");
				} else {
					System.out.print(columnname + "=?,");
				}
			}
			System.out.println(" where = ?");
			System.out.println("");
			System.out.println("delete from " + rs.getString(1).toLowerCase());
		}
	}

	public static void autoFreeCon() {
		for (String key : regDBOperators.keySet()) {
			DBOperator db = regDBOperators.get(key);
			if (((new Date()).getTime() - db.getRegDate().getTime()) / 1000 > db.timeout) {
				try {
					db.freeCon();
					regDBOperators.remove(key);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	public void glBatch(String sql, Object[][] params) throws Exception {
		if(params==null||params.length==0){
			throw new Exception("二维数组不能为空");
		}
		int length=getStrNum(sql, "?");
		for(int i=0;i<params.length;i++){
			if(length!=params[i].length){
				throw new Exception("参数与?个数不一致");
			}
		}
		ps=con.prepareStatement(sql);
		for(int i=0;i<params.length;i++){
			psSet(params[i]);
			ps.addBatch();
		}
		ps.executeBatch();
		ps.clearBatch();
		ps.close();
	}
	public Date getRegDate() {
		return regDate;
	}
	public void printTablesInfo2(String user) throws Exception {
		ResultSetMetaData metaData = null;
		ResultSet rs;
		rs = select(
				"select t.table_name from dba_all_tables t where t.owner = ? order by t.table_name",
				new Object[] { user });
		while (rs.next()) {
			System.out.println("表名：" + rs.getString(1).toLowerCase());
			String tableName=rs.getString(1).toLowerCase();
			String className =tableName.replaceFirst(tableName.substring(0, 1),tableName.substring(0, 1).toUpperCase()) ;
			System.out.println(className+" "+rs.getString(1).toLowerCase()+"= new "+className+"()");
			ResultSet resultset = select("select * from "
					+ rs.getString(1).toLowerCase());
			metaData = resultset.getMetaData();
			int columnCount = metaData.getColumnCount();
			System.out.println("属性信息：");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				String sxName =columnname.replaceFirst(columnname.substring(0, 1),columnname.substring(0, 1).toUpperCase()) ;
				//System.out.println(tableName+".set" + sxName + "();");
				int colType = metaData.getColumnType(i);
				switch (colType) {
				case Types.BLOB:
					//System.out.println("rs.getString(1).toLowerCase() " + columnname + ";");
					System.out.println(tableName+".set" + sxName + "();");
					break;
				case Types.CHAR:
					System.out.println("private String " + columnname + ";");
					break;
				case Types.CLOB:
					System.out.println("private Clob " + columnname + ";");
					break;
				case Types.DATE:
					System.out.println("private Date " + columnname + ";");
					break;
				case Types.DECIMAL:
					if (metaData.getScale(i) == 0) {
						System.out.println("private Integer " + columnname
								+ ";");
					} else {
						System.out
								.println("private Double " + columnname + ";");
					}
					break;
				case Types.FLOAT:
					System.out.println("private Float " + columnname + ";");
					break;
				case Types.NUMERIC:
					if (metaData.getScale(i) == 0) {
						System.out.println("private Integer " + columnname
								+ ";");
					} else {
						System.out
								.println("private Double " + columnname + ";");
					}
					break;
				case Types.NVARCHAR:
					System.out.println("private String " + columnname + ";");
					break;
				case Types.TIMESTAMP:
					System.out.println("private Timestamp " + columnname + ";");
					break;
				case Types.VARCHAR:
					System.out.println("private String " + columnname + ";");
					break;
				default:
					System.out.println("private String " + columnname + ";");
					break;
				}
			}
			System.out.println("SQL语句：");
			System.out.println("select * from " + rs.getString(1).toLowerCase()
					+ " t");
			System.out.print("select ");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				if (i == columnCount) {
					System.out.print("t." + columnname);
				} else {
					System.out.print("t." + columnname + ",");
				}
			}
			System.out.print("from " + rs.getString(1).toLowerCase() + " t");
			System.out.println("");
			System.out.print("insert into " + rs.getString(1).toLowerCase()
					+ " (");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				if (i == columnCount) {
					System.out.print(columnname);
				} else {
					System.out.print(columnname + ",");
				}
			}
			System.out.print(") values (");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				if (i == columnCount) {
					System.out.print("?");
				} else {
					System.out.print("?" + ",");
				}
			}
			System.out.print(")");
			System.out.println("");
			System.out.print("update " + "t." + rs.getString(1).toLowerCase()
					+ " set ");
			for (int i = 1; i <= columnCount; i++) {
				String columnname = metaData.getColumnName(i).toLowerCase();
				if (i == columnCount) {
					System.out.print(columnname + "=?");
				} else {
					System.out.print(columnname + "=?,");
				}
			}
			System.out.println(" where = ?");
			System.out.println("");
			System.out.println("delete from " + rs.getString(1).toLowerCase());
		}
	}
	
	
	public static void main(String[] args) throws Exception {
	    

	}
}
