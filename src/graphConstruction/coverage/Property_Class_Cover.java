package graphConstruction.coverage;

import DataSources.GRAPHS;
import DataSources.INFOS;
import mysql.DBCPManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

public class Property_Class_Cover {
	private static HashMap<String, Double> property_covers; // TODO: 2016/11/19  property -> coverage
	private static HashMap<String, Double> class_covers; // TODO: 2016/11/19 class -> coverage
	private static Property_Class_Cover instance;
	
	static synchronized public Property_Class_Cover getInstance(GRAPHS tablename){
		if(instance == null){
			instance = new Property_Class_Cover(tablename);
		}
		return instance;
	}
	private Property_Class_Cover(GRAPHS tablename){
		property_covers = new HashMap<String, Double>();
		class_covers = new HashMap<String, Double>();
		getPCover(tablename);
		getClassCover(tablename);
	}

	// TODO: 2016/11/19 property为p的数目
	private int getPTripleNum(String p, GRAPHS tablename){
		int num = 0;
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select count(*) from "+ tablename + " where p = ?");
			stmt.setString(1, p);
			rs = stmt.executeQuery();
			while(rs.next()){
				num = rs.getInt(1);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
		return num;
	}

	// TODO: 2016/11/19 类型为o的实例数目
	private int getCEntityNum(String o, GRAPHS tablename){
		int num = 0;
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select count(distinct s) from "+ tablename + " where o = ?");
			stmt.setString(1, o);
			rs = stmt.executeQuery();
			while(rs.next()){
				num = rs.getInt(1);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
		return num;
	}

	// TODO: 2016/11/19 计算每个property的覆盖度
	private void getPCover(GRAPHS tablename){
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select distinct p from "+ tablename);
			rs = stmt.executeQuery();
			while(rs.next()){
				String p = rs.getString(1);
				int number = getPTripleNum(p, tablename);
//				double p_cover = (Math.log((double)number+1)/Math.log((double)2))/
//						(Math.log((double) INFOS.PROPERTIES.get(tablename)+1)/Math.log((double)2));
//				property_covers.put(p, p_cover);
				property_covers.put(p, (double) number);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
	}

	// TODO: 2016/11/19 计算每个类的覆盖度
	private void getClassCover(GRAPHS tablename){
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select distinct o from "+ tablename +" where p like '%type'");
			rs = stmt.executeQuery();
			while(rs.next()){
				String o = rs.getString(1);
				int number = getCEntityNum(o, tablename);
//				double class_cover = (Math.log((double)number+1)/Math.log((double)2))/
//						(Math.log((double)INFOS.CLASSES.get(tablename)+1)/Math.log((double)2));
//				class_covers.put(o, class_cover);
				class_covers.put(o, (double)number);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
	}
	
	public synchronized HashMap<String, Double> getPCovers(){
		return property_covers;
	}
	public synchronized HashMap<String, Double> getClassCovers(){
		return class_covers;
	}
}
