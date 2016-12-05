package exp1;

import DataSources.GRAPHS;
import DataSources.INFOS;
import mysql.DBCPManager;
import summarization.FullNode;
import summarization.Greedy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.ArrayList;
import java.lang.Math;

public class exp1_summarization {
	int iterations;
	int R;
	double alpha, beta;
	int sortfunc;
	double score_90;
	double score_95;
	double score_99;
	double score_100;
	GRAPHS tablename;
	TreeMap<Double, ArrayList<Integer>> sort_id_list;
	TreeMap<Double, ArrayList<ArrayList<FullNode>>> graphs;
	HashMap<Integer, FullNode> totalgraph;
	HashMap<String, String> s_types;
	public exp1_summarization(int iterations, int R, double alpha, double beta, GRAPHS tablename, int sortfunc,
                              double score_90, double score_95, double score_99, double score_100){
		this.score_90 = score_90;
		this.score_95 = score_95;
		this.score_99 = score_99;
		this.score_100 = score_100;
		this.iterations = iterations;
		this.R = R;
		this.alpha = alpha;
		this.beta = beta;
		this.tablename = tablename;
		this.sortfunc = sortfunc;
		this.graphs = new TreeMap<Double, ArrayList<ArrayList<FullNode>>>();
		sort_id_list = SortAllNodes();
		getTotalGraph();
		SubjectTypes();
	}
	
	private void getTotalGraph(){
		totalgraph = new HashMap<Integer, FullNode>();
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select id, s, p, o, s_pagerank, o_pagerank, p_cover, class_cover, neighbors from "+ tablename);
			rs = stmt.executeQuery();
			while(rs.next()){
				int id = rs.getInt(1);
				String s = rs.getString(2);
				String p = rs.getString(3);
				String o = rs.getString(4);
				double s_pagerank = rs.getDouble(5);
				double o_pagerank = rs.getDouble(6);
//				double pagerank = 0.0;
//				if(s_pagerank == 0 || o_pagerank == 0){
//					pagerank = Math.max(s_pagerank, o_pagerank);
//				}
//				else{
//					pagerank = (s_pagerank + o_pagerank)/2;
//				}
				double cover = 0.0;
				double ccover_num = 0.0;
				double pcover_num = 0.0;
				if(p.contains("type")){
					ccover_num = rs.getDouble(8);
					cover = Math.log(ccover_num+1)/Math.log(INFOS.CLASSES.get(this.tablename)+1);
				}
				else{
					pcover_num = rs.getDouble(7);
					cover = Math.log(pcover_num+1)/Math.log(INFOS.PROPERTIES.get(this.tablename)+1);
				}
				FullNode node = new FullNode(id, s, p, o, s_pagerank, o_pagerank, cover, pcover_num, ccover_num);
				String[] neighbors = rs.getString(9).split(",");
				HashSet<Integer> node_neighbors = new HashSet<Integer>();
				for(int i = 0; i< neighbors.length; i++){
					if(neighbors[i].equals(""))
						break;
					node_neighbors.add(Integer.parseInt(neighbors[i]));
				}
				node.AddNeighbors(node_neighbors);
				totalgraph.put(id, node);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
	}
	
	private void SubjectTypes(){
		s_types = new HashMap<String, String>();
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select id, s, p, o from "+ tablename);
			rs = stmt.executeQuery();
			while(rs.next()){
				String stemp = rs.getString(2);
				String ptemp = rs.getString(3);
				String otemp = rs.getString(4);
				if(ptemp.endsWith("type")){
					s_types.put(stemp, otemp);
				}else{
					if(!s_types.containsKey(stemp)){
						s_types.put(stemp, "NOTYPE");
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
	}
	
	private double getScore(double pagerank, double class_cover, int indegree, int kind){
		if(kind == 1){
			return pagerank;
		}
		if(kind == 2){
			return class_cover;
		}
		else if(kind == 3){
			return (double)indegree;
		}
		else if(kind == 4){
			return this.alpha * pagerank + this.beta * class_cover;
		}
		else{
			return 0.5 * pagerank + 0.5 * class_cover;
		}
	}
	private TreeMap<Double, ArrayList<Integer>> SortAllNodes(){
		TreeMap<Double, ArrayList<Integer>> sortlists = new TreeMap<Double, ArrayList<Integer>>();
		DBCPManager db =  DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select id , s_pagerank, o_pagerank, p_cover, class_cover, neighbors from "+ tablename);
			rs = stmt.executeQuery();
			while(rs.next()){
				int id = rs.getInt(1);
				//double pagerank = Math.max(rs.getDouble(2), rs.getDouble(3));
				double s_pagerank = rs.getDouble(2);
				double o_pagerank = rs.getDouble(3);
				double pagerank = 0.0;
				if(s_pagerank == 0 || o_pagerank == 0){
					pagerank = Math.max(s_pagerank, o_pagerank);
				}
				else{
					pagerank = (s_pagerank + o_pagerank)/2;
				}
				double class_cover = Math.max(rs.getDouble(4), rs.getDouble(5));
				String[] neighbors = rs.getString(6).split(",");
				double score = getScore(pagerank, class_cover, neighbors.length, this.sortfunc);
				if(sortlists.containsKey(score)){
					ArrayList<Integer> temp = sortlists.get(score);
					temp.add(id);
					sortlists.put(score, temp);
				}
				else{
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(id);
					sortlists.put(score, temp);
				}
			}
					
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
		return sortlists;
	}
	
	public void getSummaryGraphs() throws Exception{
		int cur_num = 0;
		boolean is_stop = false;
		int step = 0;
		Iterator<Double> it = this.sort_id_list.descendingKeySet().iterator();
		while(it.hasNext() && cur_num < this.iterations && !is_stop){
			ArrayList<Integer> idlists = this.sort_id_list.get(it.next());
			for(int i = 0; i< idlists.size(); i++){
				cur_num++;
				if(cur_num < iterations){
					Greedy single = new Greedy(this.tablename, this.R, idlists.get(i), this.alpha, this.beta, this.totalgraph, this.s_types);
					if(!single.IfSucess()){
						continue;
					}
					double score_temp = single.getTotalScore();
					if((score_temp - this.score_90 > 0.000001) || (this.score_90 - score_temp < 0.0001)){
						if(step == 0){
							System.out.println("90 " + cur_num);
							step++;
						}
					}
					if((score_temp - this.score_95 > 0.000001) || (this.score_95 - score_temp < 0.0001)){
						if(step == 1){
							System.out.println("95 " + cur_num);
							step++;
						}
					}
					if((score_temp - this.score_99 > 0.000001) || (this.score_99 - score_temp < 0.0001)){
						if(step == 2){
							System.out.println("99 " + cur_num);
							step ++;
						}
					}
					if((score_temp - this.score_100 > 0.000001) || (this.score_100 - score_temp < 0.0001)){
						System.out.println("100 " + cur_num);
						is_stop = true;
						break;
					}
				}
				else{
					break;
				}
			}
		}
	}
	
	public double getPagerankScore(ArrayList<FullNode> single_summary){
		double pagerank = 0.0;
		for(int i = 0; i< single_summary.size(); i++){
			pagerank += single_summary.get(i).pagerank;
		}
		return pagerank/single_summary.size();
	}
	
	public double getPCover(ArrayList<FullNode> single_summary){
		double p_cover = 0.0;
		HashSet<String> properties = new HashSet<String>();
		for(int i = 0; i< single_summary.size(); i++){
			FullNode temp = single_summary.get(i);
			if(!temp.p.endsWith("type") && !properties.contains(temp.p)){
				properties.add(temp.p);
				double number = temp.getPCoverNum();
				p_cover += number;
			}
		}
		return p_cover/INFOS.PROPERTIES.get(this.tablename);
	}
	
	public double getCCover(ArrayList<FullNode> single_summary){
		double c_cover = 0.0;
		HashSet<String> classes = new HashSet<String>();
		for(int i = 0; i< single_summary.size(); i++){
			FullNode temp = single_summary.get(i);
			if(temp.p.endsWith("type")){
				if(!classes.contains(temp.o)){
					classes.add(temp.o);
					double number = temp.getCCoverNum();
					c_cover += number;
				}
			}
		}
		return c_cover/INFOS.CLASSES.get(this.tablename);
	}
	
	public static void main(String[] args) throws Exception{
		int iterations = Integer.MAX_VALUE;
		int R = 20;
		double alpha = 0.8;
		double beta =0.2;
		int func = 5;
		double score_90 = 10.795492 * 0.9;
		double score_95 = 10.795492 * 0.95;
		double score_99 = 10.795492 * 0.99;
		double score_100 = 10.795492;
		GRAPHS tablename = GRAPHS.dogfood_graph_modified;
		exp1_summarization summarization = new exp1_summarization(iterations, R, alpha, beta, tablename, func, score_90, score_95, score_99, score_100);
		summarization.getSummaryGraphs();
	}
}
