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
 * �ɼ����ݵķ���    ��ǰ�û����м��Ĵ������ݴ���
 */
public class TransErrorData {
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
		JFrame frame = new JFrame("���������ϴ�");
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
//        upErrorData();        //  ��ȡ��������
		area.append("�ϴ�����");
        upflag=true;//���ϴ���ʶ�û�
	}
    public void upErrorData(){
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
                            area.append(tableename+"  �ϴ��ύ*****************************************\n");
                        }catch(Exception e){
                            e.printStackTrace();
                            area.append("***************************�쳣��"+e.getMessage()+"********************");
                            log("�쳣��"+e.getMessage());
                            edb.rollback();

                        } finally {
                            edb.freeCon();

                        }
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
        logFile= date.get(Calendar.YEAR)+"error"+ (date.get(Calendar.MONTH)+1)+ date.get(Calendar.DATE)+logFile;
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
			area=new TransErrorData().getTextArea();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	}
	

