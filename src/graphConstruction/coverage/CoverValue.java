package graphConstruction.coverage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import DataSources.GRAPHS;
import DataSources.INFOS;
import mysql.DBCPManager;

public class CoverValue implements Runnable{

	private int start, end;
	private GRAPHS tablename;
	private ArrayList<CoverNode> nodes;
	private Property_Class_Cover covers;
	
	public CoverValue(int start, int end, GRAPHS tablename){
		this.start = start;
		this.end = end;
		this.tablename = tablename;
		nodes = new ArrayList<CoverNode>();
	}

	public void LoadNode(){
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select id, s, p, o from "+ tablename +" where id >= ? and id <= ?");
			stmt.setInt(1, start);
			stmt.setInt(2, end);
			rs = stmt.executeQuery();
			while(rs.next()){
				CoverNode node = new CoverNode(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4));
				nodes.add(node);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
	}
	
	public void Update(){
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("update "+tablename+" set p_cover = ?, class_cover = ? where id = ?");
			for(int i = 0; i != nodes.size(); i++){
				stmt.setDouble(1, nodes.get(i).getPCover());
				stmt.setDouble(2, nodes.get(i).getClassCover());
				stmt.setInt(3, nodes.get(i).getId());
				stmt.addBatch();
			}
			stmt.executeBatch();
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		covers = Property_Class_Cover.getInstance(tablename);
		LoadNode();
		for(int i = 0; i != nodes.size(); i++){
			String p = nodes.get(i).getP();
			String o = nodes.get(i).getO();
			if(p.endsWith("type")){
				double class_cover = covers.getClassCovers().get(o);
				nodes.get(i).setClassCover(class_cover);
			}
			else{
				double p_cover = covers.getPCovers().get(p);
				nodes.get(i).setPCover(p_cover);
			}
//			System.out.println(nodes.get(i).getId() + "	" + nodes.get(i).getPCover() + "	" + nodes.get(i).getClassCover());
		}
		Update();
	}
	
	public static void main(String[] args){
		BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();  
		ThreadPoolExecutor exec = new ThreadPoolExecutor(4, 5, 7, TimeUnit.DAYS, queue);
		GRAPHS graph = GRAPHS.myexperiment_graph;
		int taskNumber = 2933943;
//		GRAPHS graph = GRAPHS.dogfood_graph_modified;
//		int taskNumber = 304758;
		for (int i = 1504352; i<= taskNumber; i= i+ 5000) {
			int start = i;
			int end = i+ 4999;
			if(end > taskNumber)
				end = taskNumber;
			CoverValue a = new CoverValue(start, end, graph);
			exec.execute(a);
		}
		exec.shutdown();
	}
}
