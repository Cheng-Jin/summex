package mysql;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;


public class DBCPManager {

//	private static final Logger logger = Logger.getLogger(mysql.DBCPManager.class);
//	public static final String WORKPATH = "/home/jincheng/";
	public static final String WORKPATH = "";
	static private DBCPManager instance; 
	static private DataSource ds;
	private DBCPManager() {
  		ds = setupDataSource();
		if(ds == null){
			System.out.println("ds == null");
		}
	}
	private static DataSource setupDataSource() {
		try{
			Properties prop = new Properties();
			InputStream inputStream = new FileInputStream(WORKPATH + "dbcp.properties");
			//System.out.println(new FileInputStream("D:\\workspace\\DoubleEntitySummary1\\src\\mysql\\dbcp.properties"));
			prop.load(inputStream);
			ds = BasicDataSourceFactory.createDataSource(prop);
		} catch (IOException e) {
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
			//logger.info(e.toString());
		}
		if(ds!=null){

		}
		return ds;
	}


	static synchronized public DBCPManager getInstance() {
		if (instance == null) {
			instance = new DBCPManager();
		}
		return instance;
	}
	public void free(ResultSet rs, Statement st, Connection conn) {
		try{
			if (rs != null) rs.close();
		}catch (SQLException e){
			e.printStackTrace();
		}finally{
			try{
				if (st != null) 
					st.close();
			}catch (SQLException e){
				e.printStackTrace();
			}finally{
				try{
					if (conn != null) conn.close();
				} catch (SQLException e){
					e.printStackTrace();
				}
			}
		}
	}
	public Connection getConnection() {
		if(ds!=null){
			try {
				Connection conn=ds.getConnection();
				return conn;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	 }

	public static void main(String[] args) {
		Connection connection = DBCPManager.getInstance().getConnection();
		System.out.println(connection);

		Connection connection1 = DBCPManager.getInstance().getConnection();
		System.out.println(connection1);
	}
}
/*public class mysql.DBCPManager {
    private static final Log log = LogFactory.getLog(mysql.DBCPManager.class);
    private static final String configFile = "dbcp.properties";
 
    private static DataSource dataSource;
 
    static {
        Properties dbProperties = new Properties();
        try {
            dbProperties.load(mysql.DBCPManager.class.getClassLoader()
                    .getResourceAsStream(configFile));
            dataSource = BasicDataSourceFactory.createDataSource(dbProperties);
 
            Connection conn = getConn();
            DatabaseMetaData mdm = conn.getMetaData();
            log.info("Connected to " + mdm.getDatabaseProductName() + " "
                    + mdm.getDatabaseProductVersion());
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            log.error("鍒濆鍖栬繛鎺ユ睜澶辫触锛� + e);
        }
    }
 
    private mysql.DBCPManager() {
    }
    public static final Connection getConn() {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            log.error("鑾峰彇鏁版嵁搴撹繛鎺ュけ璐ワ細" + e);
        }
        return conn;
    }
    public static void closeConn(Connection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.setAutoCommit(true);
                conn.close();
            }
        } catch (SQLException e) {
            log.error("鍏抽棴鏁版嵁搴撹繛鎺ュけ璐ワ細" + e);
        }
    }
    public static void main(String[] args){
        long begin=System.currentTimeMillis();
        for(int i=0;i<10000;i++){
            Connection conn=mysql.DBCPManager.getConn();
            System.out.print(i+"   ");
            mysql.DBCPManager.closeConn(conn);
        }
        long end=System.currentTimeMillis();
        System.out.println("鐢ㄦ椂锛�+(end-begin));
    }
 
}*/