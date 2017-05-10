package xdy;

import DataSources.GRAPHS;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Greedy {
	private GRAPHS tablename;
	private int R;
	private double alpha, beta;
	private double total_score;
	private boolean sucessconsturct;		//摘要中三元组个数等于R即为成功构造
	private long ReadMysqlTime;
	private long ComputeTime;
	HashSet<Integer> candidateset; 		//可被选中的集合
	HashMap<Integer, FullNode> node_list;		// 已被选中的三元组 (id -> xdy.FullNode)
	HashSet<Integer> node_id_list; 	// 已被选中的三元组的id
	HashMap<Integer, FullNode> cache;
	HashMap<String, HashSet<String>> propertieswithclasses;		//每个三元组中实体对应的property和类型
	HashSet<String> classes;			//已被选中的实体的类型
	HashMap<String, String> s_types;		//	每个实体的类型
	HashMap<Integer, FullNode> totalgraph;   //所有三元组构成的图
	HashMap<String, Set<Integer>> entityIdMap;
//	LRUCache<Integer, HashSet<Integer>> neighborsCache;
	
	public Greedy(GRAPHS tablename, int R, int start, double alpha, double beta, HashMap<Integer, FullNode> totalgraph, HashMap<String, String> s_types, HashMap<String, Set<Integer>> entityIdMap){
		this.tablename = tablename;
		this.R = R;
		this.alpha = alpha;
		this.beta = beta;
		this.sucessconsturct = false;
		this.ReadMysqlTime = 0;
		this.ComputeTime = 0;
		this.totalgraph = totalgraph;
		this.s_types = s_types;
		this.entityIdMap = entityIdMap;
		
		node_id_list = new HashSet<Integer>();
		node_id_list.add(start);  //从当前start开始

//		neighborsCache = new LRUCache<>(100000);
		long temp1 = System.currentTimeMillis();
		FullNode start_node = LoadNodeFromId(start);
		long temp2 = System.currentTimeMillis();
		this.ReadMysqlTime += temp2 - temp1;
		
		this.total_score = this.alpha * start_node.getPageRank() + this.beta * start_node.getCover();
		node_list = new HashMap<Integer, FullNode>();
		node_list.put(start, start_node);
		
		candidateset = new HashSet<Integer>();
//		candidateset.addAll(start_node.getNeighbors());
		UpdateCandidateSet(start_node);
//        temp1 = System.currentTimeMillis();
//		candidateset.addAll(getNeighborsFromId(start));
//        temp2 = System.currentTimeMillis();
//        ReadMysqlTime = temp2 - temp1;

		cache = new HashMap<Integer, FullNode>();
		
		propertieswithclasses = new HashMap<String, HashSet<String>>();
		classes = new HashSet<String>();

		if(start_node.getP().endsWith("type")){
			classes.add(start_node.getO());
		}
		else{
			HashSet<String> tempclasses = new HashSet<String>();
			tempclasses.add(s_types.get(start_node.getS()));
			propertieswithclasses.put(start_node.getP(), tempclasses);
		}

		ConstructSummary();
	}

//	private HashSet<Integer> getNeighborsFromId(int id){
//		if (this.neighborsCache.containsKey(id)){
//			return this.neighborsCache.get(id);
//		}
//		DBCPManager db = DBCPManager.getInstance();
//		Connection conn = null;
//		PreparedStatement stmt = null;
//		ResultSet rs = null;
//		try {
//			conn = db.getConnection();
//			stmt = conn.prepareStatement("select neighbors from " + tablename + " where id = ?");
//			stmt.setInt(1, id);
//			rs = stmt.executeQuery();
//			while (rs.next()) {
//				String[] neighbors = rs.getString(1).split(",");
//				HashSet<Integer> neighborsSet = new HashSet<>(neighbors.length);
//				for (String nid : neighbors)
//					neighborsSet.add(Integer.parseInt(nid));
//				this.neighborsCache.put(id, neighborsSet);
//				return neighborsSet;
//			}
//		}catch (SQLException e){
//			e.printStackTrace();
//		}finally {
//			db.free(rs, stmt, conn);
//		}
//		return new HashSet<>();
//	}
	private FullNode LoadNodeFromId(int id){
		return this.totalgraph.get(id);
	}
	
	private void ConstructSummary(){
		long readtime = 0;
		long computestart = System.currentTimeMillis();
		while(node_id_list.size() < R){
			if(candidateset.size() == 0){
				break;
			}
			FullNode max_node = null;
			double max_score = -1.0;
			Iterator<Integer> it = candidateset.iterator();
			while(it.hasNext()){
				int id = it.next();
				FullNode temp = null;
				if(cache.containsKey(id)){
					temp = cache.get(id);
				}
				else{
					long temp1 = System.currentTimeMillis();
					temp = LoadNodeFromId(id);
					long temp2 = System.currentTimeMillis();
					readtime += temp2 - temp1;
					cache.put(id, temp);
				}
				double score = this.alpha * temp.getPageRank();
				if((!propertieswithclasses.containsKey(temp.getP()) || 
						!propertieswithclasses.get(temp.getP()).contains(s_types.get(temp.getS())))
						&& !classes.contains(temp.getO()))
					score += this.beta * temp.getCover();
				if(score > max_score){
					max_score = score;
					max_node = temp;
				}
			}
			this.total_score += max_score;
			node_id_list.add(max_node.getId());
			node_list.put(max_node.getId(), max_node);
			
			UpdateCandidateSet(max_node);
			
			if(max_node.getP().endsWith("type")){
				classes.add(max_node.getO());
			}
			else{
				if(propertieswithclasses.containsKey(max_node.getP())){
					HashSet<String> tempclass = propertieswithclasses.get(max_node.getP());
					tempclass.add(s_types.get(max_node.getS()));
					propertieswithclasses.put(max_node.getP(), tempclass);
				}
				else{
					HashSet<String> tempclass = new HashSet<String>();
					tempclass.add(s_types.get(max_node.getS()));
					propertieswithclasses.put(max_node.getP(), tempclass);
				}
			}
		}
		long computeend = System.currentTimeMillis();
		this.ComputeTime = computeend-computestart-readtime;
		this.ReadMysqlTime += readtime;
		
		if(node_id_list.size() == R){
			this.sucessconsturct = true;
		}
	}
	
	private void UpdateCandidateSet(FullNode node){
        int id = node.getId();
		String s = node.getS();
		String o = node.getO();
		candidateset.addAll(this.entityIdMap.get(s));
		if (this.entityIdMap.containsKey(o))
			candidateset.addAll(this.entityIdMap.get(o));
		candidateset.remove((Integer) id);
//		candidateset.addAll(getNeighborsFromId(id));
//        candidateset.addAll(node.getNeighbors());
		Iterator<Integer> it = node_id_list.iterator();
		while(it.hasNext()){
			int nid = it.next();
			candidateset.remove((Integer)nid);
		}
//		it = candidateset.iterator();
	}
	public HashMap<Integer, FullNode> getSummary(){
		return this.node_list;
	}
	
	public double getTotalScore(){
		return this.total_score;
	}
	public boolean IfSucess(){
		return this.sucessconsturct;
	}
	public long getReadTime(){
		return this.ReadMysqlTime;
	}
	public long getComputeTime(){
		return this.ComputeTime;
	}
}

