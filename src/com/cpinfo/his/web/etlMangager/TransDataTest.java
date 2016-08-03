package com.cpinfo.his.web.etlMangager;

import com.cpinfo.his.web.etlMangager.db.DBOperator;
import com.cpinfo.his.web.etlMangager.utils.UUIDGenerator;

import javax.swing.*;
import javax.swing.Timer;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 测试上传数据质量的方法
 */

public class TransDataTest {
    public static final String sigleJGs="340000002148,340000002150,340000002176,485957459,340000002180,48599188-4,340000002184,340000002162,340000002158,340000002182";
    public static JTextArea area;  //显示区
    public static JTextField hourText;//时
    public static JTextField minuteText;//分
    public static JButton jb_input;//输入
    public  static int hour=03;
    public static int minute=30;
    private PrintWriter log;
    int dataCount=10;
    public TransDataTest() {
        area=getTextArea();
    }

    /**
     * 创建窗口可视化
     * @return
     */
    public  JTextArea getTextArea(){
        JFrame frame = new JFrame("请注意，这里只是测试数据质量，并不提交。");
        frame.setSize(800, 600); // 设置大小
        frame.setAlwaysOnTop(true); // 设置其总在最上
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); // 默认关闭操作
        frame.setIconImage(new ImageIcon("images/icon.jpg").getImage()); // 设置窗体的图标
        frame.setLocationRelativeTo(null); // 设置窗体初始位置
        frame.setLayout(new BorderLayout());
        JLabel jPanel=new JLabel();
        jPanel.setBackground(Color.pink);
        JTextArea jTextArea=new JTextArea();
        JScrollPane jScrollPane=new JScrollPane(jTextArea);
        jScrollPane.setBorder(new TitledBorder("数据上传信息"));
        jTextArea.setEditable(false);
        jPanel.setLayout(new BorderLayout(1,3));
        jPanel.add(jScrollPane);
        frame.getContentPane().add(jPanel,BorderLayout.CENTER);
        frame.setVisible(true); // 显示窗口
        return jTextArea;
    }

    public void dataTrans(int dataCount){
        area.setText("开始上传数据\n");
        area.selectAll();
        area.setCaretPosition(area.getSelectedText().length());
        area.requestFocus();
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        area.append("开始测试"+sdf.format(new Date())+"\n");
        TransData(dataCount);        //  提取基础数据
        area.append("上传完了");
    }
    public void TransData(int rownum){
        DBOperator db= null;
        DBOperator mdb=null;
         rownum=20;
        try {
            db = new DBOperator();
            mdb = new DBOperator("middledb");
            String sql="select u.tableename,u.tablename,u.tableid from uploadtable u where u.tableename is not null and u.isshow='Y'  order by u.no ";
            java.util.List<Map> tableList=db.find(sql);
            //获取所有要采集的表名
            /**遍历所有表，逐一获取表结构*/
            for(int i=0;i<tableList.size();i++){
                Map tableListMap=tableList.get(i);
                checkData(tableListMap, db, mdb, rownum);  //去执行数据检查
            }
        } catch (Exception e) {
            e.printStackTrace();
            area.append("异常信息:"+e.getMessage());
        }finally{
            db.freeCon();
            mdb.freeCon();
        }
        area.append("测试执行完了");

    }
    /**
     *    检查数据格式方法
     * @param tableListMap       *
     * @param db
     * @param mdb
     * @param rownum
     * @throws Exception
     */
    private  void checkData(Map tableListMap,DBOperator db,DBOperator mdb,int rownum)  {
        try{
            String tablename=tableListMap.get("tablename").toString();
//        area.append("上传表名："+tablename+"\n");
            String tableid=tableListMap.get("tableid").toString();
            String tableename=tableListMap.get("tableename").toString();
            String sql="select u.columname,u.columename,u.columlength,u.columtype,u.columisnull,u.dictnekey,u.columispk,u.rowdate from uploadtableinfo u where u.tableid=?";
            java.util.List<Map> tableInfoList=db.find(sql,new Object[]{tableid});    //获取该表的字段信息
            String addSql="";
            String wenStr="";
            String pkStr="";
            String upStr="";
            int pksize=0;
            String dodate=null;
            /**遍历所有字段，获取字段信息*/
            for(int j=0;j<tableInfoList.size();j++){
                Map colMap=tableInfoList.get(j);
                String columename=((String)colMap.get("columename"));
                String columispk=((String)colMap.get("columispk"));
                String rowdate=((String)colMap.get("rowdate"));
                if("Y".equals(columispk)){
                    if(pkStr.equals("")){
                        pkStr=" where "+columename+"=?";
                    }else{
                        pkStr=pkStr+" and "+columename+"=?";
                    }
                    pksize++;
                }
                if("Y".equals(rowdate)){
                    dodate=columename;
                }
                addSql=addSql+columename+",";
                wenStr=wenStr+"?,";
                upStr=upStr+columename+"=?,";
            }
            upStr=upStr+"uploaddate=sysdate";
            if("Pt_Information".equals(tableename)){
                upStr=upStr+",empiid=null";
            }
            sql="select * from "+tableename+" where uploadflag ='0'   and jgdm is not null   and rownum<='"+rownum+"'";
            String kematchSql="select e.centervalue,e.centername from dictcompare e where e.jgdm=? and e.centerkey=? and e.hosvalue=?";
            String nekySql="select s.contents from bas_dicts s where s.nekey=? and s.nevalue=?";
            java.util.List<Map> list=mdb.find(sql);
            area.append(tablename+"  共获取"+list.size()+"条记录\n");
            log("-------------------------"+tablename+"  共获取"+list.size()+"条记录---------------------------");   //写入日志
//            while(list.size()>0){
            java.util.List<Object[]> errobjs=new ArrayList<Object[]>();
            java.util.List<Object[]> updateobjs=new ArrayList<Object[]>();
            Map<Map,List<String>> mainErrorMaps=new HashMap<Map,List<String>>();  //存放错误信息的映射
            /**遍历查询到的记录，逐条进行分析*/
            for(int j=0;j<list.size();j++){
                Map tmap=list.get(j);             //获取一条记录
                area.append((j+1)+":"+tablename+"  正在校验数据。。。"+tmap+"\n");
                boolean errorflag=true;
                java.util.List<String> errorInfoList =new ArrayList<String>();
                Object[] obj=new Object[tableInfoList.size()];        //存储insert所需参数值的数组
                Object[] pkobj=new Object[pksize];        //存储主键值的数组
                int k=0;int pkct=0;String isfalse="";
                /**遍历该行每一列*/
                for(k=0;k<tableInfoList.size();k++){
                    Map colMap=tableInfoList.get(k);
                    String columname=(String)colMap.get("columname");
                    if(null!=columname)columname=columname.trim();
                    String columename=(String)colMap.get("columename");
                    if(null!=columename)columename=columename.trim();
                    String columlength=colMap.get("columlength")==null?"":colMap.get("columlength").toString();
                    if(null!=columlength)columlength=columlength.trim();
                    String columtype=(String)colMap.get("columtype");
                    if(null!=columtype)columtype=columtype.trim().toUpperCase();
                    String columisnull=(String)colMap.get("columisnull");
                    if(null!=columisnull)columisnull=columisnull.trim().toUpperCase();
                    String dictnekey=(String)colMap.get("dictnekey");
                    if(null!=dictnekey)dictnekey=dictnekey.trim();
                    String columispk=(String)colMap.get("columispk");
                    if(null!=columispk)columispk=columispk.trim().toUpperCase();
                    if("Y".equals(columispk)){        //判断是否主键
                        pkobj[pkct]=tmap.get(columename);
                        pkct++;
                    }
                    if(tmap.get(columename)==null||"".equals(tmap.get(columename).toString().trim())){
                        if("Y".equals(columisnull)){
                            if("sfzh".equals(columename)){
                                isfalse="为空";
                            }else{
                                String uuid=new UUIDGenerator().generate().toString();
                                errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"002",columname+"("+columename+")为空",tmap.get(dodate)});
                                errorInfoList.add(columname+"("+columename+")为空") ;
                                errorflag=false;    //判断是否错误
                                updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                            }
                        }else{
                            obj[k]=tmap.get(columename);
                            continue;
                        }
                    }else{
                        if("DATE".equals(columtype)){
                            obj[k]=tmap.get(columename);
                            continue;
                        }else{
                            String dateStr=tmap.get(columename).toString().trim();
                            if("INTEGER".equals(columtype)){
                                try{
                                    int data= Integer.parseInt(dateStr);
                                    obj[k]=data;
                                    continue;
                                }catch(Exception e){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"004",columname+"("+columename+")不符合INTEGER类型",tmap.get(dodate)});
                                    errorInfoList.add(columname+"("+columename+")不符合INTEGER类型") ;
                                    errorflag=false;    //判断是否错误
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});

                                }
                            }else if("NUMBER".equals(columtype)){
                                try{
                                    double data= Double.parseDouble(dateStr);
                                    obj[k]=data;
                                    continue;
                                }catch(Exception e){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"004",columname+"("+columename+")不符合DOUBLE类型",tmap.get(dodate)});
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    errorInfoList.add(columname+"("+columename+")不符合DOUBLE类型") ;
                                    errorflag=false;    //判断是否错误
                                }
                            }

                            if(!"".equals(dictnekey)&&dictnekey!=null){
                                String[] nekeyS=dictnekey.split(",");
                                int n=0;
                                for(n=0;n<nekeyS.length;n++){
                                    String nekey=nekeyS[n];
                                    java.util.List<Map> tlist=new ArrayList<Map>();
                                    String jgdm=(String)tmap.get("jgdm");
                                    if(sigleJGs.indexOf(jgdm)==-1){
                                        jgdm="0000";
                                    }
                                    tlist=db.find(kematchSql,new Object[]{jgdm,nekey,dateStr});
                                    if(tlist.size()>0&&tlist.get(0).get("centervalue")!=null){
                                        dateStr=tlist.get(0).get("centervalue").toString();
                                        break;
                                    }else{
                                        java.util.List<Map> tlist2=new ArrayList<Map>();
                                        tlist2=db.find(nekySql,new Object[]{nekey,dateStr});
                                        if(tlist2.size()>0){
                                            break;
                                        }
                                    }
                                }
                                if(n>=nekeyS.length){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"005",columname+"("+columename+")未与字典匹配",tmap.get(dodate)});
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    errorInfoList.add(columname+"("+columename+")未与字典匹配") ;
                                    errorflag=false;    //判断是否错误
                                }

                            }
                            if(!"".equals(columlength)&&columlength!=null){
                                int length=dateStr.getBytes().length;
                                if(length> Double.parseDouble(columlength)){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"003",columname+"("+columename+")数据长度过长",tmap.get(dodate)});
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    errorInfoList.add(columname+"("+columename+")数据长度过长") ;
                                    errorflag=false;    //判断是否错误
                                }
                            }
                            if("sfzh".equals(columename)){
                                Pattern idNumPattern = Pattern.compile("(\\d{14}[0-9a-zA-Z])|(\\d{17}[0-9a-zA-Z])");
                                Matcher idNumMatcher = idNumPattern.matcher(dateStr);
                                if(!idNumMatcher.matches()){
                                    isfalse="验证不符";
                                    errorInfoList.add("身份证号码验证不符");
                                }
                            }
                            obj[k]=dateStr;
                            continue;
                        }
                    }
                }
                if(errorflag){
                    area.append(" 检查通过 \n");
                }else{
                    mainErrorMaps.put(tmap,errorInfoList);
                    area.append(tmap+"\n:"+errorInfoList+"\n");
                }
            }
            Set<Map.Entry<Map,List<String>>>  errSet=  mainErrorMaps.entrySet()  ;
            //遍历所有错误信息
            for(Map.Entry entry:errSet){
                log(entry.getKey()+" 对应错误信息："+entry.getValue());
            }
        }catch(Exception e){
            e.printStackTrace();
            area.append("***************************异常："+e.getMessage()+"********************");
            log("异常："+e.getMessage());
        } finally {

        }
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
        //得到时间类
        Calendar date = Calendar.getInstance();
        //设置时间为 xx-xx-xx 00:00:00
        date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DATE), hour, minute, 0);
        //日志
        String logFile = dbProps.getProperty("logfile", "DBConnectionManager.log");
        logFile= date.get(Calendar.YEAR)+"Y"+ (date.get(Calendar.MONTH)+1)+"M"+ date.get(Calendar.DATE)+"D"+logFile;
        try {
            log = new PrintWriter(new FileWriter(logFile, true), true);
             dataCount=  Integer.parseInt(dbProps.getProperty("data_count","1"));
        }
        catch (Exception e) {
            System.err.println("无法打开日志文件: " + logFile);
            log = new PrintWriter(System.err);
            dataCount=10;
        }
        //一天的毫秒数
        dataTrans(dataCount);
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
           TransDataTest test=new TransDataTest();
            test.getData();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }
}
