package xdy;

import DataSources.GRAPHS;
import DataSources.INFOS;
import mysql.DBCPManager;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class summarization {

    	public static final String WORKPATH = "C:\\Users\\cheng jin\\Desktop\\";
//    public static final String WORKPATH = "/home/jincheng/";
    int iterations;

    int R;
    double alpha, beta;
    int sortfunc;
    //	String tablename;
    GRAPHS tablename;
    TreeMap<Double, ArrayList<Integer>> sort_id_list;       //依照sortfiunc分数排序
    TreeMap<Double, ArrayList<ArrayList<FullNode>>> graphs;         //最终结果以及它们的分数
    HashMap<Integer, FullNode> totalgraph;      //整个完整的图，从数据库读取
    HashMap<String, String> s_types;        //每个实体对应的类型，不存在类型时, value值是"NOTYPE"
    HashMap<String, Set<Integer>> entityIdMap;
//    Map<String, Set<Integer>> s_map;
    Map<String, TreeMap<Double, Set<Integer>>> o_map;

    public summarization(int iterations, int R, double alpha, double beta, GRAPHS tablename, int sortfunc) {

        this.iterations = iterations;
        this.R = R;
        this.alpha = alpha;
        this.beta = beta;
        this.tablename = tablename;
        this.sortfunc = sortfunc;
        this.graphs = new TreeMap<Double, ArrayList<ArrayList<FullNode>>>();
//
//		sort_id_list = SortAllNodes();
//		getTotalGraph();
//		SubjectTypes();
        sort_id_list = new TreeMap<>();
        totalgraph = new HashMap<>();
        s_types = new HashMap<>();
        this.entityIdMap = new HashMap<>();
        loadEntity();
        loadData();
    }

    private void loadEntity() {
//        s_map = new HashMap<>();
        o_map = new HashMap<>();
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
//            con = db.getConnection();
//            pstmt = con.prepareStatement("select s from " + this.tablename);
//            rs = pstmt.executeQuery();
//            while (rs.next()) {
//                String s = rs.getString(1);
//                this.entityIdMap.put(s, new HashSet<>());
//            }
//            System.out.println("subjects loaded!");
//
//            pstmt = con.prepareStatement("select distinct o from " + this.tablename + " where p not like '%type' and o like 'http%'"); // TODO: 2016/12/5 只选择是实体
//            rs = pstmt.executeQuery();
//            while (rs.next()) {
//                String o = rs.getString(1);
//                this.entityIdMap.put(o, new HashSet<>());
//            }
//            System.out.println("objects loaded!");

            con = db.getConnection();
            pstmt = con.prepareStatement("SELECT id, s, p, o, s_pagerank, o_pagerank FROM " + this.tablename);
            rs = pstmt.executeQuery();
            while (rs.next()){
                int id = rs.getInt(1);
                String s = rs.getString(2);
                String p = rs.getString(3);
                String o = rs.getString(4);


//                this.entityIdMap.put(s, new HashSet<>());
                // TODO: 2016/12/18  s在左右三元组中出现的id
                Set<Integer> sids = this.entityIdMap.get(s);
                if (sids == null){
                    sids = new HashSet<>();
                    sids.add(id);
                    this.entityIdMap.put(s, sids);
                }
                else {
                    sids.add(id);
                }


                if (o.startsWith("http")) {
                    if (!p.endsWith("url") && !p.endsWith("page") && !p.endsWith("sameAs") && !p.endsWith("seeAlso") && !p.endsWith("type")) {
//                        this.entityIdMap.put(o, new HashSet<>());
                        double s_pagerank = rs.getDouble(5), o_pagerank = rs.getDouble(6);
                        double pagerank = 0.0;
                        if (s_pagerank == 0 || o_pagerank == 0) {
                            pagerank = Math.max(s_pagerank, o_pagerank);
                        } else {
                            pagerank = (s_pagerank + o_pagerank) / 2;
                        }

                        Map<Double, Set<Integer>> scoreIdMap = this.o_map.get(o);

                        if (scoreIdMap == null){
                            scoreIdMap = new TreeMap<>();
                            Set<Integer> ids = new HashSet<>();
                            ids.add(id);
                            scoreIdMap.put(pagerank, ids);
                        }
                        else {
                            Set<Integer> ids = scoreIdMap.get(pagerank);
                            if (ids == null){
                                ids = new HashSet<>();
                                ids.add(id);
                                scoreIdMap.put(pagerank, ids);
                            }
                            else ids.add(id);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }

        for (Map.Entry<String, TreeMap<Double, Set<Integer>>> entry : this.o_map.entrySet()){
            String o = entry.getKey();
            TreeMap<Double, Set<Integer>> scoreIds = entry.getValue();

            Set<Integer> ids = this.entityIdMap.get(o);
            if (ids == null){
                ids = new HashSet<>();
            }
            for (double score : scoreIds.descendingKeySet()){
                Set<Integer> idset = scoreIds.get(score);
                for (int id : idset){
                    if (ids.size() < 40)
                        ids.add(id);
                    else break;
                }
            }
            this.entityIdMap.put(o, ids);
        }
        System.out.println("neighbor inverted index over!");
    }

    private void loadData() {
        DBCPManager db = DBCPManager.getInstance();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = db.getConnection();
            stmt = conn.prepareStatement("select id, s, p, o, s_pagerank, o_pagerank, p_cover, class_cover from " + tablename);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt(1);
                String s = rs.getString(2);
                String p = rs.getString(3);
                String o = rs.getString(4);

                // TODO: 2016/12/15 设置邻居
//                this.entityIdMap.get(s).add(id);
//                if (this.entityIdMap.containsKey(o))
//                    this.entityIdMap.get(o).add(id);

                double s_pagerank = rs.getDouble(5);
                double o_pagerank = rs.getDouble(6);
                double cover = 0.0;
                double ccover_num = 0.0;
                double pcover_num = 0.0;

                if (p.contains("type")) {
                    ccover_num = rs.getDouble(8);
                    cover = Math.log(ccover_num + 1) / Math.log(INFOS.CLASSES.get(this.tablename) + 1);   //注意这里分母为图中实体个数
                } else {
                    pcover_num = rs.getDouble(7);
                    cover = Math.log(pcover_num + 1) / Math.log(INFOS.PROPERTIES.get(this.tablename) + 1);        //这里分母为跑去type类型的property总个数
                }
                FullNode node = new FullNode(id, s, p, o, s_pagerank, o_pagerank, cover, pcover_num, ccover_num);
                int neighborsNum = 1;
//				String[] neighbors = rs.getString(9).split(",");
//				HashSet<Integer> node_neighbors = new HashSet<Integer>();
//				for(int i = 0; i< neighbors.length; i++){
//					if(neighbors[i].equals(""))
//						break;
//					node_neighbors.add(Integer.parseInt(neighbors[i]));
//				}
//				node.AddNeighbors(node_neighbors);
                totalgraph.put(id, node);

                if (p.endsWith("type")) {
                    s_types.put(s, o);
                } else {
                    if (!s_types.containsKey(s)) {
                        s_types.put(s, "NOTYPE");
                    }
                }

                double pagerank = 0.0;
                if (s_pagerank == 0 || o_pagerank == 0) {
                    pagerank = Math.max(s_pagerank, o_pagerank);
                } else {
                    pagerank = (s_pagerank + o_pagerank) / 2;
                }
                double class_cover = Math.max(ccover_num, pcover_num);
                double score = getScore(pagerank, class_cover, neighborsNum, this.sortfunc);
                TreeMap<Double, ArrayList<Integer>> sortlists = this.sort_id_list;
                if (sortlists.containsKey(score)) {
                    ArrayList<Integer> temp = sortlists.get(score);
                    temp.add(id);
                    sortlists.put(score, temp);
                } else {
                    ArrayList<Integer> temp = new ArrayList<Integer>();
                    temp.add(id);
                    sortlists.put(score, temp);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            db.free(rs, stmt, conn);
        }
        System.out.println("all subject types loaded!");
        System.out.println("all nodes sorted!");
        System.out.println("total graph loaded!");

    }

    // 读取整个图
    /*private void getTotalGraph(){
		totalgraph = new HashMap<Integer, FullNode>();
		DBCPManager db = DBCPManager.getInstance();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			conn = db.getConnection();
			stmt = conn.prepareStatement("select id, s, p, o, s_pagerank, o_pagerank, p_cover, class_cover from "+ tablename);
			rs = stmt.executeQuery();
			while(rs.next()){
				int id = rs.getInt(1);
				String s = rs.getString(2);
				String p = rs.getString(3);
				String o = rs.getString(4);
				double s_pagerank = rs.getDouble(5);
				double o_pagerank = rs.getDouble(6);
				double cover = 0.0;
				double ccover_num = 0.0;
				double pcover_num = 0.0;

				if(p.contains("type")){
					ccover_num = rs.getDouble(8);
					cover = Math.log(ccover_num+1)/Math.log(INFOS.CLASSES.get(this.tablename)+1);   //注意这里分母为图中实体个数
				}
				else{
					pcover_num = rs.getDouble(7);
					cover = Math.log(pcover_num+1)/Math.log(INFOS.PROPERTIES.get(this.tablename)+1);		//这里分母为跑去type类型的property总个数
				}
				FullNode node = new FullNode(id, s, p, o, s_pagerank, o_pagerank, cover, pcover_num, ccover_num);
//				String[] neighbors = rs.getString(9).split(",");
//				HashSet<Integer> node_neighbors = new HashSet<Integer>();
//				for(int i = 0; i< neighbors.length; i++){
//					if(neighbors[i].equals(""))
//						break;
//					node_neighbors.add(Integer.parseInt(neighbors[i]));
//				}
//				node.AddNeighbors(node_neighbors);
				totalgraph.put(id, node);
			}
			System.out.println("total graph loaded!");
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
	}*/

    //	获取每个实体的类型
	/*private void SubjectTypes(){
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
			System.out.println("all subject types loaded!");
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
	}*/

    // 给定sortfunc, 返回对应的分数
    private double getScore(double pagerank, double class_cover, int indegree, int kind) {
        if (kind == 1) {
            return this.alpha * pagerank + this.beta * class_cover;
        }
        if (kind == 2) {
            return pagerank;
        } else if (kind == 3) {
            return class_cover;
        } else {
            return (double) indegree;
        }
    }

    //	依照sortfuc对每个三元组进行排序
	/*private TreeMap<Double, ArrayList<Integer>> SortAllNodes(){
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
			System.out.println("all nodes sorted!");
					
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			db.free(rs, stmt, conn);
		}
		return sortlists;
	}*/

    //	开始计算摘要
    public void getSummaryGraphs() throws Exception {
        int cur_num = 0;
        double score = 0.0;
        double pagerank = 0.0;
        double p_cover = 0.0;
        double c_cover = 0.0;
        double total_pagerank = 0.0;
        double total_entity_num = 0.0;
        long use_time = 0;
        ArrayList<FullNode> last_summary = new ArrayList<>();
        Iterator<Double> it = this.sort_id_list.descendingKeySet().iterator();
        File file = new File(WORKPATH + this.tablename + "/time_" + this.R + "_" + this.alpha + ".txt");
        BufferedWriter out = new BufferedWriter(new FileWriter(file));
        while (it.hasNext() && cur_num < this.iterations) {
            ArrayList<Integer> idlists = this.sort_id_list.get(it.next());
            for (int i = 0; i < idlists.size(); i++) {
                if (cur_num < iterations) {
                    Greedy single = new Greedy(this.tablename, this.R, idlists.get(i), this.alpha, this.beta, this.totalgraph, this.s_types, this.entityIdMap);
                    long temp_use_time = single.getComputeTime();
                    if (cur_num % 10000 == 0)
                        System.out.println(cur_num + "	" + score + "	" + pagerank + "	" + total_pagerank + "	" + total_entity_num + "	" + p_cover + "	" + c_cover);
                    use_time = use_time + temp_use_time;
                    if (cur_num % 10 == 0) {
                        out.write(cur_num + "	" + use_time);
                        out.newLine();
                    }
                    cur_num++;
                    if (!single.IfSucess()) {
                        continue;
                    }
                    double score_temp = single.getTotalScore();
                    HashMap<Integer, FullNode> temp = single.getSummary();
                    ArrayList<FullNode> single_summary = new ArrayList<FullNode>();
                    Iterator<Integer> temp_it = temp.keySet().iterator();
                    while (temp_it.hasNext()) {
                        single_summary.add(temp.get(temp_it.next()));
                    }
                    last_summary = single_summary;
                    if ((score_temp - score) > 0.0000000001) {
                        score = score_temp;
                        p_cover = getPCover(single_summary);
                        c_cover = getCCover(single_summary);
                        pagerank = getPagerankScore(single_summary);
                        total_pagerank = this.getTotalPagerankScore(single_summary);
                        total_entity_num = this.getEntities(single_summary);
//						this.writeResult(new Iterations(single_summary, score, total_entity_num, total_pagerank, p_cover, c_cover, pagerank));
                    } else if ((score - score_temp) > 0.0000000001) {

                    } else {
                        p_cover = getPCover(single_summary);
                        c_cover = getCCover(single_summary);
                        pagerank = getPagerankScore(single_summary);
                        total_pagerank = this.getTotalPagerankScore(single_summary);
                        total_entity_num = this.getEntities(single_summary);
//						this.writeResult(new Iterations(single_summary, score, total_entity_num, total_pagerank, p_cover, c_cover, pagerank));
                    }
                } else {
                    break;
                }
            }
        }
        System.out.println(cur_num + "	" + score + "	" + pagerank + "	" + total_pagerank + "	" + total_entity_num + "	" + p_cover + "	" + c_cover);
        this.writeResult(new summary(last_summary, score, total_entity_num, total_pagerank, p_cover, c_cover, pagerank));
        out.close();
    }

    public void writeResult(summary summary) {
        try {
            File file = new File(WORKPATH + this.tablename + "/summary_" + this.R + "_" + this.alpha + ".txt");
            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
            List<FullNode> nodes = summary.getSummary();

            for (int i = 0; i < nodes.size(); i++) {
                String s = nodes.get(i).getS();
                String p = nodes.get(i).getP();
                String o = nodes.get(i).getO();
                osw.write(s + "	" + p + "	" + o + "\r\n");
            }
            osw.write("score:	" + summary.getScore() + "\r\n");
            osw.write("pagerank:	" + summary.getPagerank() + "\r\n");
            osw.write("total_pagerank:	" + summary.getTotal_pagerank() + "\r\n");
            osw.write("entity_num:	" + summary.getEntityNum() + "\r\n");
            osw.write("p_cover:	" + summary.getP_cover() + "\r\n");
            osw.write("c_cover:	" + summary.getC_cover() + "\r\n");
            osw.write("\r\n");
            osw.flush();
            osw.close();
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //	给定一个摘要，计算pageRank均值
    public double getPagerankScore(ArrayList<FullNode> single_summary) {
        HashMap<String, Double> entities_pagerank = new HashMap<String, Double>();
        double pagerank = 0.0;
        for (int i = 0; i < single_summary.size(); i++) {
            String s = single_summary.get(i).getS();
            String o = single_summary.get(i).getO();
            double spagerank = single_summary.get(i).getSPagerank();
            double opagerank = single_summary.get(i).getOPagerank();
            if (!entities_pagerank.containsKey(s) && spagerank > 0) {
                entities_pagerank.put(s, spagerank);
            }
            if (!entities_pagerank.containsKey(o) && opagerank > 0) {
                entities_pagerank.put(o, opagerank);
            }
        }

        Iterator<String> it = entities_pagerank.keySet().iterator();
        while (it.hasNext()) {
            String entity = it.next();
            pagerank += entities_pagerank.get(entity);
        }
        return pagerank / entities_pagerank.size();
    }

    //	给定一个摘要，计算所有实体的pageRank总和
    public double getTotalPagerankScore(ArrayList<FullNode> single_summary) {
        HashMap<String, Double> entities_pagerank = new HashMap<String, Double>();
        double pagerank = 0.0;
        for (int i = 0; i < single_summary.size(); i++) {
            String s = single_summary.get(i).getS();
            String o = single_summary.get(i).getO();
            double spagerank = single_summary.get(i).getSPagerank();
            double opagerank = single_summary.get(i).getOPagerank();
            if (!entities_pagerank.containsKey(s) && spagerank > 0) {
                entities_pagerank.put(s, spagerank);
            }
            if (!entities_pagerank.containsKey(o) && opagerank > 0) {
                entities_pagerank.put(o, opagerank);
            }
        }

        Iterator<String> it = entities_pagerank.keySet().iterator();
        while (it.hasNext()) {
            String entity = it.next();
            pagerank += entities_pagerank.get(entity);
        }
        return pagerank;
    }

    //给定一个摘要， 计算实体总个数
    public double getEntities(ArrayList<FullNode> single_summary) {
        HashMap<String, Double> entities_pagerank = new HashMap<String, Double>();
        double pagerank = 0.0;
        for (int i = 0; i < single_summary.size(); i++) {
            String s = single_summary.get(i).getS();
            String o = single_summary.get(i).getO();
            double spagerank = single_summary.get(i).getSPagerank();
            double opagerank = single_summary.get(i).getOPagerank();
            if (!entities_pagerank.containsKey(s) && spagerank > 0) {
                entities_pagerank.put(s, spagerank);
            }
            if (!entities_pagerank.containsKey(o) && opagerank > 0) {
                entities_pagerank.put(o, opagerank);
            }
        }

        Iterator<String> it = entities_pagerank.keySet().iterator();
        while (it.hasNext()) {
            String entity = it.next();
            pagerank += entities_pagerank.get(entity);
        }
        return entities_pagerank.size();
    }

    //	给定一个摘要，计算property覆盖度
    public double getPCover(ArrayList<FullNode> single_summary) {
        double p_cover = 0.0;
        HashSet<String> properties = new HashSet<String>();
        for (int i = 0; i < single_summary.size(); i++) {
            FullNode temp = single_summary.get(i);
            if (!temp.p.endsWith("type") && !properties.contains(temp.p)) {
                properties.add(temp.p);
                double number = temp.getPCoverNum();
                p_cover += number;
            }
        }
        return p_cover / INFOS.PROPERTIES.get(this.tablename);
    }

    //	给定一个摘要，计算class覆盖度
    public double getCCover(ArrayList<FullNode> single_summary) {
        double c_cover = 0.0;
        HashSet<String> classes = new HashSet<String>();
        for (int i = 0; i < single_summary.size(); i++) {
            FullNode temp = single_summary.get(i);
            if (temp.p.endsWith("type")) {
                if (!classes.contains(temp.o)) {
                    classes.add(temp.o);
                    double number = temp.getCCoverNum();
                    c_cover += number;
                }
            }
        }
        return c_cover / INFOS.CLASSES.get(this.tablename);
    }

    public static void main(String[] args) throws Exception {
        int iterations = Integer.MAX_VALUE;
		int R = 40;
		double alpha = 0.4;
		double beta = 0.6;
		int func = 1;
		GRAPHS tablename = GRAPHS.sabiork_graph;
		summarization summarization = new summarization(iterations, R, alpha, beta, tablename, func);
		summarization.getSummaryGraphs();


//        int[] Rs = {20, 40};
//        double[] alphas = {0, 0.2, 0.4, 0.6, 0.8, 1};
////		double[] betas = {1, 0.8, 0,6, 0, 4, 0.2, 0};
//        int func = 1;
//        GRAPHS tablename = GRAPHS.sabiork_graph;
//        for (int R : Rs) {
//            for (double alpha : alphas) {
//                xdy s = new xdy(iterations, R, alpha, 1 - alpha, tablename, func);
//                s.iteratingExp2();
//            }
//        }
    }
}
