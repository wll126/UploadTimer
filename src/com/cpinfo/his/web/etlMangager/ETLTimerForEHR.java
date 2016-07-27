package com.cpinfo.his.web.etlMangager;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import javax.swing.*;
import javax.swing.border.TitledBorder;

public class ETLTimerForEHR

{
    private static JTextArea area;  //显示区
    private static JTextField hourText;//时
    private static JTextField minuteText;//分
    private static JButton jb_input;//输入
    //  public static JButton jb_test;//测试数据合法性
   private  static int hour=03;
   private static int minute=30;
    private PrintWriter log;
	 private String his_db="yhhis";    //his在ETL中连接名称
//	 private DatabaseMeta yunhis  ;//his 数据库
	 private JTextArea jTextArea;//显示区

    public ETLTimerForEHR() {
       area= getTextArea();
        jTextArea=area;
    }

    public ETLTimerForEHR(JTextArea jTextArea) {
        this.jTextArea = jTextArea;
    }
    /**
     * 创建窗口可视化
     * @return
     */
    public   JTextArea getTextArea(){
        JFrame frame = new JFrame("数据上传");
        frame.setSize(800, 600); // 设置大小
        frame.setAlwaysOnTop(true); // 设置其总在最上
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); // 默认关闭操作
        frame.setIconImage(new ImageIcon("images/icon.jpg").getImage()); // 设置窗体的图标
        frame.setLocationRelativeTo(null); // 设置窗体初始位置
        frame.setLayout(new BorderLayout());
        JLabel jPanel=new JLabel();
        jPanel.setBackground(Color.pink);
        JLabel jp_top=new JLabel();
        jp_top.setLayout(new GridLayout(1,7));
        JLabel jLabel=new JLabel("   ",SwingUtilities.LEFT);
        jLabel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 20));
        hourText=new JTextField("03");
        JLabel hourLabel=new JLabel("时  ",SwingUtilities.LEFT);
        minuteText=new JTextField("30");
        JLabel minuteLabel=new JLabel("分：",SwingUtilities.LEFT);
        jb_input=new JButton("确定");
//        jb_test=new JButton(" 测试");
        jb_input.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String hours=hourText.getText().trim();
                String minutes=minuteText.getText().trim();
                String reg="([01][0-9])|([2][0-3])";
                String minuteReg="[0-5][0-9]";
                if(hours.matches(reg)) {
                    hour = Integer.parseInt(hours);
                    if (minutes.matches(minuteReg)) {
                        minute = Integer.parseInt(minutes);
                        area.setText("定时器将在"+hour+"："+minute+"执行");
                        Date dateUp=new Date();
                        dateUp.setHours(hour);
                        dateUp.setMinutes(minute);
                        long upTimes=dateUp.getTime();
                        long nowTimes=new Date().getTime();
                        long dtime=(upTimes-nowTimes)/60000l;
                        area.append("距离开始执行时间"+dtime+"分");
                        getData();
                    }else{
                        area.setText("时间分格式不正确");
                    }
                }else{
                    area.setText("时间格式不正确");
                }
            }
        });
//        jb_test.addActionListener(new ActionListener() {
//            @Override
//            public void actionPerformed(ActionEvent e) {
//                TransDataTest test=new TransDataTest();
//                 test.getData();
//            }
//        });
        jp_top.add(jLabel);
        jp_top.add(hourText);
        jp_top.add(hourLabel);
        jp_top.add(minuteText);
        jp_top.add(minuteLabel);
        jp_top.add(jb_input);
//        jp_top.add(jb_test) ;
        jp_top.setSize(600,30);
        JTextArea jTextArea=new JTextArea();
        JScrollPane jScrollPane=new JScrollPane(jTextArea);
        jScrollPane.setBorder(new TitledBorder("数据上传信息"));
        jTextArea.setEditable(false);
        jPanel.setLayout(new BorderLayout(1,3));
        jPanel.add(jp_top);
        jPanel.add(jScrollPane);
//		frame.getContentPane().add(jp_top,BorderLayout.NORTH);
        frame.getContentPane().add(jPanel,BorderLayout.CENTER);
        frame.setVisible(true); // 显示窗口
        return jTextArea;
    }
    public void start()
	  {
	    String jobName = "hisUpdateJob";
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    try
	    {
	  	  //初始化环境
    	      KettleEnvironment.init();
    	    //选择作业
    	      //创建DB资源库
    	      KettleDatabaseRepository repository = new KettleDatabaseRepository();
    	      //（数据库连接名称，资源库类型，连接方式，IP，数据库名，端口，用户名，密码）
//    	      	      DatabaseMeta databaseMeta = new DatabaseMeta("ehr", "Oracle", "Native", "115.28.172.54", "orcl", "1521", "his_bz", "123456");
    	      DatabaseMeta databaseMeta = new DatabaseMeta("upload", "Oracle", "Native", "127.0.0.1", "orcl", "1521", "hospitals", "123456");
    	      //选择资源库(ID,名称，描述)
    	      KettleDatabaseRepositoryMeta kettleDatabaseMeta = new KettleDatabaseRepositoryMeta("upload", "upload", "upload", databaseMeta);
    	   	  repository.init(kettleDatabaseMeta);
    	      //连接资源库（用户名，密码）
    	      repository.connect("admin", "admin");
    	      //资源库目录
    	      RepositoryDirectoryInterface directory = repository.loadRepositoryDirectoryTree();
//    	      JobMeta jobMeta = repository.loadJob(jobName, directory, null, null);
//	         Job job ;
	         List<Map<String,String>> hisDb= getHisDBProperties();
	        if(hisDb!=null&&hisDb.size()>0){
	            for(Map<String,String> hismap:hisDb){
	                //改连接，执行其他地方的
	                System.out.println("ETL ehryh will be start");
	                //获取连接
	                //执行默认的地区his
	                System.out.println("ETL "+hismap.get("user")+" will be start");
         if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL "+hismap.get("user")+" will be start\n");
	                String trans="HIS_ClinicCharge,HIS_ClinicRecord,Ipt_Record,Opt_Other,Ipt_Advicedetail,HIS_Recipe,his_cliniclabdetail,Bas_Identifier,Pt_Information,Opt_OtherDetail,HIS_RecipeDetail_new,HIS_ClinicDiag,HIS_ClinicLab,Opt_Register,his_cliniccharperstat_zy,HIS_CLINICCHARPERSTAT,Ipt_Admissionnote,Ipt_MedicalRecordPage_New,Ipt_SignsRecord,Cu_Register,Bas_Location,Cu_Summary,PT_EXAMREPORT,HIS_RecipeDetail,Ipt_Leaverecord,Pt_Information227,cu_summary,Cu_Detail";
	                trans="Pt_Information,Bas_Identifier,HIS_ClinicRecord,HIS_ClinicCharge,HIS_RecipeDetail_new,HIS_Recipe,Opt_OtherDetail,Opt_Other,Opt_Register,HIS_ClinicDiag,his_cliniclabdetail,HIS_ClinicLab,Ipt_Record,Ipt_Advicedetail,HIS_CLINICCHARPERSTAT,his_cliniccharperstat_zy,Ipt_Admissionnote";
	                String [] transNames=trans.split(",");
//                    //开启多线程模式
//                    ExecutorService cachedThreadPool  = Executors.newCachedThreadPool();//创建线程池获取连接;
	                 for(String transName:transNames){
           if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL "+transName+" will be start\n");
	                	TransMeta transMeta=repository.loadTransformation(transName,directory,null,true,null);
	                	//获取连接
	                	List<DatabaseMeta> dmlist =transMeta.getDatabases();
	                	for(DatabaseMeta dm:dmlist){
	                		String connection_name=dm.getName();
	                		if(his_db.equals(connection_name)){	                			
	                			dm.setUsername(hismap.get("user")); //用户
	                			System.out.println("用户名:"+dm.getUsername());
	                			dm.setPassword(hismap.get("password")); //密码
            if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"用户名 ："+dm.getUsername()+"\n");
	                		}
	                	}
                        final TransMeta finalTransMeta =transMeta;
//                         cachedThreadPool.submit(new Runnable() {
//                             @Override
//                             public void run() {
                                 Trans transs=new Trans(finalTransMeta);
//                                 try {
                                     transs.execute(null);
//                                 } catch (KettleException e) {
//                                     e.printStackTrace();
//                                 }
                                 transs.waitUntilFinished();
//                             }
//                         }) ;
             if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL  "+transName+" will be end\n");
	                }
//                    cachedThreadPool.shutdown();
//                    boolean loop=true;
//                    do{
//                        loop=!cachedThreadPool.awaitTermination(2, TimeUnit.SECONDS);  //等待线程任务完成
//                    }while (loop);
	                System.out.println("ETL  "+hismap.get("user")+" will be end");
            if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL  "+hismap.get("user")+" will be end\n");
	            }
	        }
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
            if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL发生异常 "+e.getMessage()+"\n");
	    }
	  }

	    /**
	     * 读取属性完成初始化
	     */
	    private List<Map<String,String>> getHisDBProperties() {
	        InputStream is = getClass().getResourceAsStream("/db.properties");
	        Properties dbProps = new Properties();
	        try {
	            dbProps.load(is);
	        }
	        catch (Exception e) {
	            System.err.println("不能读取属性文件. " +
	                    "请确保db.properties在CLASSPATH指定的路径中");
	            return null;
	        }
	        Enumeration propNames = dbProps.propertyNames();
	        List<Map<String,String>> hisDbLiist=new ArrayList<Map<String,String>>();
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
	                //  String maxconn = dbProps.getProperty(poolName + ".maxconn", "0");
	                  hisMap.put("url",url);
	                  hisMap.put("user",user);
	                  hisMap.put("password",password);
	                  hisDbLiist.add(hisMap);
	                  poolSet.add(poolName);
	              }
	        }

	       return hisDbLiist;
	    }

    /**
     * 定时执行代码
     *
     */
    public  void getData(){
        if(hour==0){
            hour= 03;
        }
        if(minute==0){
            minute= 30;
        }
        //得到时间类
        Calendar date = Calendar.getInstance();
        //设置时间为 xx-xx-xx 00:00:00
        date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DATE), hour, minute, 0);
        //一天的毫秒数
        long daySpan = 24*60* 60 * 1000;
        area.setText("定时器将在"+hour+"："+minute+"执行");
        //得到定时器实例
        Timer t = new Timer();
        //使用匿名内方式进行方法覆盖
        t.schedule(new TimerTask() {
            public void run() {
                //run中填写定时器主要执行的代码块
                System.out.println("定时器执行..");
                    area.append("开始执行上传程序");
                    start();
            }
        }, date.getTime(), daySpan); //daySpan是一天的毫秒数，也是执行间隔
    }
    public static void main(String[] args) {
        new ETLTimerForEHR();

    }


}