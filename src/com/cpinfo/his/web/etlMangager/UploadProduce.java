package com.cpinfo.his.web.etlMangager;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: qingpu
 * Date: 16-6-20
 * Time: 上午11:20
 * 调用存储过程进行上传的方法
 */
public class UploadProduce {
      private  Properties dbProps=new Properties();//数据库配置信息
      private  Properties uploadProps=new Properties();//上传的配置信息
      private   List<Map<String,String>> hisDbLiist=new ArrayList<Map<String,String>>();//数据来源的数据库信息
            //1从his库获取数据
           //2 检查数据是否已经存在
           //3  根据检查结果执行新增或者更新操作

        //加载配置文件，获取我们需要的数据库参数 ，获取his库的信息
    private void getProperties(){
        InputStream dbIs = getClass().getResourceAsStream("/db.properties");        //db
        InputStream uploadIs=getClass().getResourceAsStream("/upload.properties");//upload
        try {

            dbProps.load(dbIs);  //加载配置文件
            uploadProps.load(uploadIs);//加载上传信息配置文件
        } catch (IOException e) {
            System.err.println("不能读取属性文件. " +
                    "请确保db.properties在CLASSPATH指定的路径中");
        }
        Enumeration propNames = dbProps.propertyNames();              //获取所有的属性名
        Set<String> poolSet=new HashSet<String>();
        //遍历所有名称返回his数据库的信息
        while (propNames.hasMoreElements()) {
            String name = (String) propNames.nextElement();
            if(name.startsWith("his")){
                String poolName = name.substring(0, name.lastIndexOf("."));
                if(poolSet.contains(poolName))continue;
                String url = dbProps.getProperty(poolName + ".url");
                if (url == null) {
                    System.out.println("没有为连接池" + poolName + "指定URL");
                    continue;
                }
                Map<String,String> hisMap=new HashMap<String, String>();
                String user = dbProps.getProperty(poolName + ".user");
                String password = dbProps.getProperty(poolName + ".password");
                //String maxconn = dbProps.getProperty(poolName + ".maxconn", "0");
                hisMap.put("url",url);
                hisMap.put("user",user);
                hisMap.put("password",password);
                hisDbLiist.add(hisMap);
                poolSet.add(poolName);
            }
        }

    }


}
