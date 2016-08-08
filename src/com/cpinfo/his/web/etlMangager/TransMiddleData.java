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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * �ɼ����ݵķ���    ��ǰ�û����м��Ĵ����ݴ���
 */
public class TransMiddleData {
	public static final String sigleJGs="340000002148,340000002150,340000002176,485957459,340000002180,48599188-4,340000002184,340000002162,340000002158,340000002182";
	public static JTextArea area;  //��ʾ��
	public static JTextField hourText;//ʱ
	public static JTextField minuteText;//��
	public static JButton jb_input;//����
//  public static JButton jb_test;//�������ݺϷ���
	public  static int hour=03;
	public static int minute=30;
    private PrintWriter log;
    private static boolean upflag=true;    //�ϴ��жϱ�ʶ
	/**
	 * �������ڿ��ӻ�
	 * @return
     */
	public  JTextArea getTextArea(){
		JFrame frame = new JFrame("�����ϴ�");
		frame.setSize(800, 600); // ���ô�С
		frame.setAlwaysOnTop(true); // ��������������
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); // Ĭ�Ϲرղ���
		frame.setIconImage(new ImageIcon("images/icon.jpg").getImage()); // ���ô����ͼ��
		frame.setLocationRelativeTo(null); // ���ô����ʼλ��
		frame.setLayout(new BorderLayout());
		JLabel jPanel=new JLabel();
		jPanel.setBackground(Color.pink);
		JLabel jp_top=new JLabel();
		jp_top.setLayout(new GridLayout(1,7));
		JLabel jLabel=new JLabel("   ",SwingUtilities.LEFT);
		jLabel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 20));
		hourText=new JTextField("03");
		JLabel hourLabel=new JLabel("ʱ  ",SwingUtilities.LEFT);
		minuteText=new JTextField("30");
		JLabel minuteLabel=new JLabel("�֣�",SwingUtilities.LEFT);
		jb_input=new JButton("ȷ��");
//      jb_test=new JButton(" ����");
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
						area.setText("��ʱ������"+hour+"��"+minute+"ִ��");
						getData();
					}else{
						area.setText("ʱ��ָ�ʽ����ȷ");
					}
				}else{
						area.setText("ʱ���ʽ����ȷ");
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
		jScrollPane.setBorder(new TitledBorder("�����ϴ���Ϣ"));
		jTextArea.setEditable(false);
		jPanel.setLayout(new BorderLayout(1,3));
		jPanel.add(jp_top);
		jPanel.add(jScrollPane);
//		frame.getContentPane().add(jp_top,BorderLayout.NORTH);
		frame.getContentPane().add(jPanel,BorderLayout.CENTER);
		frame.setVisible(true); // ��ʾ����
		return jTextArea;
	}
	public void dataTrans(){
		area.setText("��ʼ�ϴ�����\n");
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("��һ����ʼ"+sdf.format(new Date()));
        area.append("\n��ʼ��һ����"+sdf.format(new Date())+"\n");
        upflag=false;//�����ϴ�״̬
        TransData();        //  ��ȡ��������
		area.append("�ϴ�����");
        upflag=true;//���ϴ���ʶ�û�
	}
    public void TransData(){
	 	DBOperator mdb= null;
		int rownum=200;
		try {
		    mdb = new DBOperator("middledb");
			String sql="select u.tableename,u.tablename,u.tableid from uploadtable u where u.tableename is not null    and u.isshow='Y'  order by u.no ";
		     List<Map> tableList=mdb.find(sql);
            //��ȡ����Ҫ�ɼ��ı���
            /**�������б���һ��ȡ��ṹ*/
            ExecutorService cachedThreadPool  = Executors.newCachedThreadPool();//�����̳߳ػ�ȡ����;
            for(int i=0;i<tableList.size();i++){
             try{
                final  Map tableListMap=tableList.get(i);
                cachedThreadPool.submit(new Runnable() {
                    public void run() {
                        area.append("-----------------------"+tableListMap.get("tablename").toString()+"------------------------\n");
                            checkData(tableListMap, 200);  //ȥִ�����ݼ��
                         }
                    } );
                }  catch(Exception e2){
                   log("�쳣��Ϣ:"+e2.getMessage());
                }

            }
              cachedThreadPool.shutdown();
            boolean loop=true;
            do{
                loop=!cachedThreadPool.awaitTermination(2, TimeUnit.SECONDS);  //�ȴ��߳��������
            }while (loop);
        } catch (Exception e) {
			e.printStackTrace();
			area.append("�쳣��Ϣ:"+e.getMessage());
		}finally{
			mdb.freeCon();
		}
       area.append("��һ����ִ������");

	}
    /**
     *    ������ݸ�ʽ��������ʽ��ķ���
     * @param tableListMap      *      *
     * @param rownum
     * @throws Exception
     */
    private  void checkData(Map tableListMap,int rownum)  {
        DBOperator db=null   ;
        DBOperator mdb=null ;
      try{
          db =new DBOperator() ;
          mdb= new DBOperator("middledb") ;
        String tablename=tableListMap.get("tablename").toString();
        String tableid=tableListMap.get("tableid").toString();
        String tableename=tableListMap.get("tableename").toString();
        String sql="select u.columname,u.columename,u.columlength,u.columtype,u.columisnull,u.dictnekey,u.columispk,u.rowdate from uploadtableinfo u where u.tableid=?";
        List<Map> tableInfoList=mdb.find(sql,new Object[]{tableid});    //��ȡ�ñ���ֶ���Ϣ
        String addSql="";
        String wenStr="";
        String upStr="";
        int pksize=0;
        /**���������ֶΣ���ȡ�ֶ���Ϣ*/
        for(int j=0;j<tableInfoList.size();j++){
            Map colMap=tableInfoList.get(j);
            String columename=((String)colMap.get("columename"));
            addSql=addSql+columename+",";
            wenStr=wenStr+"?,";
            upStr=upStr+columename+"=?,";
        }
        addSql=addSql+"uploaddate";
        wenStr=wenStr+"sysdate";
        sql="select * from "+tableename+" where uploadflag ='0'   and jgdm is not null   and rownum<='"+rownum+"'";        //and jgdm not in('340000002148','340000002150','340000002176','485957459','340000002180','48599188-4','340000002184','340000002162','340000002158','340000002182')
        String insertSql="insert into "+tableename+"("+addSql+") values("+wenStr+")";
        String medUpSql="update "+tableename+" set uploadflag=?,uploaddate=sysdate where dataid=?";
      List<Map> list=mdb.find(sql);
     area.append(tablename+"  ����ȡ"+list.size()+"����¼\n");
     log("-------------------------"+tablename+"  ����ȡ"+list.size()+"����¼---------------------------");   //д����־
        while(list.size()>0){
            List<Object[]> updateobjs=new ArrayList<Object[]>();
            Set<Map>  errorMap=new HashSet<Map>();  //��Ŵ������ݵļ���
            /**������ѯ���ļ�¼���������з���*/
            for(int j=0;j<list.size();j++){
                Map tmap=list.get(j);             //��ȡһ����¼
                area.append((j+1)+":"+tablename+"  �����ϴ����ݡ�����"+tmap+"\n");
                Object[] obj=new Object[tableInfoList.size()];        //�洢insert�������ֵ������
                Object[] pkobj=new Object[pksize];        //�洢����ֵ������
                int k=0;int pkct=0;String isfalse="";
                /**��������ÿһ��*/
                for(k=0;k<tableInfoList.size();k++){
                    Map colMap=tableInfoList.get(k);
                    String columname=(String)colMap.get("columname");
                    if(null!=columname)columname=columname.trim();
                    String columename=(String)colMap.get("columename");
                    if(null!=columename)columename=columename.trim();
                        String uuid=new UUIDGenerator().generate().toString();
                                updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                            obj[k]=tmap.get(columename);
                }
                        area.append(tablename+"  ����\n");
                        db.excute(insertSql,obj);
            }
            Object[][] piMidUp=new Object[updateobjs.size()][2];
            for(int j=0;j<updateobjs.size();j++){
                piMidUp[j]=updateobjs.get(j);
            }
            mdb.excuteBatch(medUpSql, piMidUp);
            db.commit();
            mdb.commit();
            area.append(tablename+"  �ϴ��ύ*****************************************\n");
            list=mdb.find(sql);
        }
        }catch(Exception e){
             e.printStackTrace();
          area.append("***************************�쳣��"+e.getMessage()+"********************");
          log("�쳣��"+e.getMessage());
            db.rollback();
            mdb.rollback();
        } finally {
             db.freeCon();
            mdb.freeCon();
        }
    }

	/**
	 * ��ʱִ�д���
	 *
     */
	public  void getData(){
		InputStream is = getClass().getResourceAsStream("/db.properties");
		Properties dbProps = new Properties();
		try {
			dbProps.load(is);
		}catch (Exception e) {
			System.err.println("���ܶ�ȡ�����ļ�. " +
					"��ȷ��db.properties��CLASSPATHָ����·����");
			return;
		}

		if(hour==0){
			 hour= Integer.parseInt(dbProps.getProperty("hour","03"));
		}
		if(minute==0){
			 minute= Integer.parseInt(dbProps.getProperty("minutes","30"));
		}
        int intervalMinute=Integer.parseInt(dbProps.getProperty("interval","60")) ;
		//�õ�ʱ����
		Calendar date = Calendar.getInstance();
		//����ʱ��Ϊ xx-xx-xx 00:00:00
		date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DATE), hour, minute, 0);
         Calendar now=Calendar.getInstance();
        int compare=now.compareTo(date);       //�ж��趨ʱ���Ƿ���
        if(compare==1){
           date.add(Calendar.DATE,1);//��һ��
        }
         //��־
        String logFile = dbProps.getProperty("logfile", "DBConnectionManager.log");
        logFile= date.get(Calendar.YEAR)+""+ (date.get(Calendar.MONTH)+1)+ date.get(Calendar.DATE)+logFile;
        try {
            log = new PrintWriter(new FileWriter(logFile, true), true);
        }
        catch (IOException e) {
            System.err.println("�޷�����־�ļ�: " + logFile);
            log = new PrintWriter(System.err);
        }
		//һ��ĺ�����
		long daySpan = intervalMinute* 60 * 1000;
		area.setText("��ʱ������"+hour+"��"+minute+"ִ��\n");
		//�õ���ʱ��ʵ��
		Timer t = new Timer();
		//ʹ�������ڷ�ʽ���з�������
		t.schedule(new TimerTask() {
			public void run() {
				//run����д��ʱ����Ҫִ�еĴ����
				System.out.println("��ʱ��ִ��..");
				if(upflag){
                    area.append("��ʼִ���ϴ�����\n");
                    dataTrans();
                }else{
                    area.append("�ϴ���������ִ���С�����\n");
                }
			}
		}, date.getTime(), daySpan); //daySpan��һ��ĺ�������Ҳ��ִ�м��
	}

    /**
     * ���ı���Ϣд����־�ļ�
     */
    private void log(String msg) {
        log.println(new Date() + ": " + msg);
    }

    /**
     * ���ı���Ϣ���쳣д����־�ļ�
     */
    private void log(Throwable e, String msg) {
        log.println(new Date() + ": " + msg);
        e.printStackTrace(log);
    }
	public static void main(String[] args) {
		try {
			area=new TransMiddleData().getTextArea();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	}
	

