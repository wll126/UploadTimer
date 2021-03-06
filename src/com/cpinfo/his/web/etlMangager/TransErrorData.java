package com.cpinfo.his.web.etlMangager;

import com.cpinfo.his.web.etlMangager.db.DBOperator;
import com.cpinfo.his.web.etlMangager.utils.UUIDGenerator;

import javax.swing.*;
import javax.swing.border.TitledBorder;
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

/**
 * 采集数据的方法    从前置机到中间库的错误数据传输
 */
public class TransErrorData {
	public static final String sigleJGs="340000002148,340000002150,340000002176,485957459,340000002180,48599188-4,340000002184,340000002162,340000002158,340000002182";
	public static JTextArea area;  //显示区
	public static JTextField hourText;//时
	public static JTextField minuteText;//分
	public static JButton jb_input;//输入
//  public static JButton jb_test;//测试数据合法性
	public  static int hour=03;
	public static int minute=30;
    private PrintWriter log;
    private static boolean upflag=true;    //上传判断标识
	/**
	 * 创建窗口可视化
	 * @return
     */
	public  JTextArea getTextArea(){
		JFrame frame = new JFrame("错误数据上传");
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
//      jb_test=new JButton(" 测试");
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
						getData();
					}else{
						area.setText("时间分格式不正确");
					}
				}else{
						area.setText("时间格式不正确");
				}
			}
		});
		jp_top.add(jLabel);
		jp_top.add(hourText);
		jp_top.add(hourLabel);
		jp_top.add(minuteText);
		jp_top.add(minuteLabel);
		jp_top.add(jb_input);
//      jp_top.add(jb_test) ;
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
	public void dataTrans(){
		area.setText("开始上传数据\n");
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("第一步开始"+sdf.format(new Date()));
        area.append("\n开始第一部分"+sdf.format(new Date())+"\n");
        upflag=false;//进入上传状态
//        upErrorData();        //  提取基础数据
		area.append("上传完了");
        upflag=true;//将上传标识置回
	}
    public void upErrorData(){
	 	DBOperator mdb= null;
		int rownum=200;
		try {
		    mdb = new DBOperator("middledb");
			String sql="select u.tableename,u.tablename,u.tableid from uploadtable u where u.tableename is not null    and u.isshow='Y'  order by u.no ";
		     List<Map> tableList=mdb.find(sql);
            //获取所有要采集的表名
            /**遍历所有表，逐一获取表结构*/
            ExecutorService cachedThreadPool  = Executors.newCachedThreadPool();//创建线程池获取连接;
            for(int i=0;i<tableList.size();i++){
             try{
                final  Map tableListMap=tableList.get(i);
                cachedThreadPool.submit(new Runnable() {
                    public void run() {
                        area.append("-----------------------"+tableListMap.get("tablename").toString()+"------------------------\n");
                        DBOperator edb=null   ;
                        try{
                            edb =new DBOperator("middledb") ;
                            String tableename=tableListMap.get("tableename").toString();
                            String  sql=" select * from "+tableename+" where uploadflag not in('0','1') and uploaderrflag is null   and jgdm is not null and createdate>sysdate-1   ";        //and jgdm not in('340000002148','340000002150','340000002176','485957459','340000002180','48599188-4','340000002184','340000002162','340000002158','340000002182')
                            String   insertSql="insert into "+tableename+"@bz "+sql ;
                            String medUpSql="update "+tableename+" set uploaderrflag=ysc";
                            edb.excute(insertSql) ;
                            edb.excute(medUpSql);
                            edb.commit();
                            area.append(tableename+"  上传提交*****************************************\n");
                        }catch(Exception e){
                            e.printStackTrace();
                            area.append("***************************异常："+e.getMessage()+"********************");
                            log("异常："+e.getMessage());
                            edb.rollback();

                        } finally {
                            edb.freeCon();

                        }
                         }
                    } );
                }  catch(Exception e2){
                   log("异常信息:"+e2.getMessage());
                }

            }
              cachedThreadPool.shutdown();
            boolean loop=true;
            do{
                loop=!cachedThreadPool.awaitTermination(2, TimeUnit.SECONDS);  //等待线程任务完成
            }while (loop);
        } catch (Exception e) {
			e.printStackTrace();
			area.append("异常信息:"+e.getMessage());
		}finally{
			mdb.freeCon();
		}
       area.append("第一部分执行完了");

	}
    /**
     *    检查数据格式并存入正式库的方法
     * @param tableListMap      *      *
     * @param rownum
     * @throws Exception
     */
    private  void checkData(Map tableListMap,int rownum)  {

    }

	/**
	 * 定时执行代码
	 *
     */
	public  void getData(){
		InputStream is = getClass().getResourceAsStream("/db.properties");
		Properties dbProps = new Properties();
		try {
			dbProps.load(is);
		}catch (Exception e) {
			System.err.println("不能读取属性文件. " +
					"请确保db.properties在CLASSPATH指定的路径中");
			return;
		}

		if(hour==0){
			 hour= Integer.parseInt(dbProps.getProperty("hour","03"));
		}
		if(minute==0){
			 minute= Integer.parseInt(dbProps.getProperty("minutes","30"));
		}
        int intervalMinute=Integer.parseInt(dbProps.getProperty("interval","60")) ;
		//得到时间类
		Calendar date = Calendar.getInstance();
		//设置时间为 xx-xx-xx 00:00:00
		date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DATE), hour, minute, 0);
         Calendar now=Calendar.getInstance();
        int compare=now.compareTo(date);       //判断设定时间是否到来
        if(compare==1){
           date.add(Calendar.DATE,1);//加一天
        }
         //日志
        String logFile = dbProps.getProperty("logfile", "DBConnectionManager.log");
        logFile= date.get(Calendar.YEAR)+"error"+ (date.get(Calendar.MONTH)+1)+ date.get(Calendar.DATE)+logFile;
        try {
            log = new PrintWriter(new FileWriter(logFile, true), true);
        }
        catch (IOException e) {
            System.err.println("无法打开日志文件: " + logFile);
            log = new PrintWriter(System.err);
        }
		//一天的毫秒数
		long daySpan = intervalMinute* 60 * 1000;
		area.setText("定时器将在"+hour+"："+minute+"执行\n");
		//得到定时器实例
		Timer t = new Timer();
		//使用匿名内方式进行方法覆盖
		t.schedule(new TimerTask() {
			public void run() {
				//run中填写定时器主要执行的代码块
				System.out.println("定时器执行..");
				if(upflag){
                    area.append("开始执行上传程序\n");
                    dataTrans();
                }else{
                    area.append("上传程序正在执行中。。。\n");
                }
			}
		}, date.getTime(), daySpan); //daySpan是一天的毫秒数，也是执行间隔
	}

    /**
     * 将文本信息写入日志文件
     */
    private void log(String msg) {
        log.println(new Date() + ": " + msg);
    }

    /**
     * 将文本信息与异常写入日志文件
     */
    private void log(Throwable e, String msg) {
        log.println(new Date() + ": " + msg);
        e.printStackTrace(log);
    }

	public static void main(String[] args) {
		try {
			area=new TransErrorData().getTextArea();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	}
	

