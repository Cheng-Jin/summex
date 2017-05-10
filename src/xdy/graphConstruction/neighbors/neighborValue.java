package xdy.graphConstruction.neighbors;

import DataSources.GRAPHS;
import mysql.DBCPManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by xdy on 2016/11/20.
 */
// TODO: 2016/11/20 计算每个三元组的邻居节点
public class neighborValue{
    GRAPHS tablename;

    Map<Integer, neighborNode> nodes;
    Map<String, Set<Integer>> s_map, o_map;

    public neighborValue(GRAPHS tablename) {
        this.tablename = tablename;
        this.nodes = new ConcurrentHashMap<>();
        this.s_map = new ConcurrentHashMap<>();
        this.o_map = new ConcurrentHashMap<>();

        loadSubjects();
        loadObjects();
        loadNodes();
    }

    public Map<String, Set<Integer>> getS_map() {
        return s_map;
    }

    public Map<String, Set<Integer>> getO_map() {
        return o_map;
    }

    public Map<Integer, neighborNode> getNodes() {
        return nodes;
    }
    // TODO: 2016/11/20 加载所有实体
    public void loadSubjects() {
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("select distinct s from " + this.tablename);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String s = rs.getString(1);
                this.s_map.put(s, new HashSet<>());
            }
            System.out.println("subjects loaded!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
    }

    // TODO: 2016/11/20 加载所有属性值
    public void loadObjects() {
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("select distinct o from " + this.tablename + " where p not like '%type' and o like 'http%'"); // TODO: 2016/12/5 只选择是实体
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String o = rs.getString(1);
                this.o_map.put(o, new HashSet<>());
            }
            System.out.println("objects loaded!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
    }

    // TODO: 2016/11/20 加载顶点
    public void loadNodes() {
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            db = DBCPManager.getInstance();
            con = db.getConnection();
            pstmt = con.prepareStatement("select id, s, o from " + this.tablename);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt(1);
                String s = rs.getString(2);
                String o = rs.getString(3);

                neighborNode node = new neighborNode(id, s, o);
                this.nodes.put(id, node);

                if (this.s_map.containsKey(s)) {
                    this.s_map.get(s).add(id);
                }
                if (this.s_map.containsKey(o)) {
                    this.s_map.get(o).add(id);
                }

                if (this.o_map.containsKey(s)) {
                    this.o_map.get(s).add(id);
                }
                if (this.o_map.containsKey(o)) {
                    this.o_map.get(o).add(id);
                }
            }
            System.out.println("triples loaded!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
    }


    //    // TODO: 2016/11/20 更新每个三元组的neighbors
   /* public void setNeighbors() {
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int count = 0;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("update " + this.tablename + " set neighbors = ? where id = ?");
            for (Map.Entry<Integer, neighborNode> entry : this.nodes.entrySet()) {
                neighborNode node = entry.getValue();
                int id = node.getId();
                String s = node.getS();
                String o = node.getO();

                Set<Integer> neighbors = new HashSet<>();
                if (this.s_map.containsKey(s))
                    neighbors.addAll(this.s_map.get(s));
                if (this.s_map.containsKey(o))
                    neighbors.addAll(this.s_map.get(o));
                if (this.o_map.containsKey(s))
                    neighbors.addAll(this.o_map.get(s));
                if (this.o_map.containsKey(o))
                    neighbors.addAll(this.o_map.get(o));
                neighbors.remove(id);

                StringBuilder sb = new StringBuilder();
                for (int nid : neighbors) {
                    sb.append(nid + ",");
                }
                if (sb.length() > 0)
                    sb.deleteCharAt(sb.length() - 1);
//                node.setNeighbors(sb.toString());
                pstmt.setString(1, sb.toString());
                pstmt.setInt(2, id);
                pstmt.addBatch();
                count++;
                if (count % 10000 == 0) {
                    pstmt.executeBatch();
                    System.out.println("updated " + count);
                }
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
        System.out.println("set neighbors over!");
    }*/




    public static void main(String[] args) {

        GRAPHS graph = GRAPHS.interpro_graph;
        int taskNumber = 2323317;
        neighborValue nv = new neighborValue(graph);
        Map<String, Set<Integer>> s_map = nv.getS_map(),
                                 o_map = nv.getO_map();
        Map<Integer, neighborNode> nodes = nv.getNodes();

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
        ThreadPoolExecutor exec = new ThreadPoolExecutor(4, 5, 7, TimeUnit.DAYS, queue);
        for (int i = 370000; i <= taskNumber; i= i+ 10000) {
            int start = i;
            int end = i+ 9999;
            if(end > taskNumber)
                end = taskNumber;
            Runnable task = new neighborsUpcdater(graph, start, end, nodes, s_map, o_map);
            exec.execute(task);
            System.out.println("执行从" + i + "到" + end + "的更新任务");
        }
        exec.shutdown();
    }
}

class neighborsUpcdater implements Runnable{
    GRAPHS tablename;
    int start, end;
    Map<Integer, neighborNode> nodes;
    Map<String, Set<Integer>> s_map, o_map;

    public neighborsUpcdater(GRAPHS tablename, int start, int end, Map<Integer, neighborNode> nodes, Map<String, Set<Integer>> s_map, Map<String, Set<Integer>> o_map) {
        this.tablename = tablename;
        this.start = start;
        this.end = end;

        this.nodes = nodes;
        this.s_map = s_map;
        this.o_map = o_map;
    }

    public void updateNeighbors() {
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int count = 0;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("update " + this.tablename + " set neighbors = ? where id = ?");

//            for (neighbors node: this.nodes){
            for (int i = start; i <= end; i++) {
                neighborNode node = nodes.get(i);
                int id = node.getId();
                String s = node.getS();
                String o = node.getO();
                Set<Integer> neighbors = new HashSet<>();
                if (this.s_map.containsKey(s))
                    neighbors.addAll(this.s_map.get(s));
                if (this.s_map.containsKey(o))
                    neighbors.addAll(this.s_map.get(o));
                if (this.o_map.containsKey(s))
                    neighbors.addAll(this.o_map.get(s));
                if (this.o_map.containsKey(o))
                    neighbors.addAll(this.o_map.get(o));
                neighbors.remove(id);

                StringBuilder sb = new StringBuilder();
                for (int nid : neighbors) {
                    sb.append(nid + ",");
                }
                if (sb.length() > 0)
                    sb.deleteCharAt(sb.length() - 1);
//
                pstmt.setString(1, sb.toString());
                pstmt.setInt(2, id);
                pstmt.addBatch();
                count++;

//                if (count % 10000 == 0) {
//                    pstmt.executeBatch();
//                    System.out.println("inserted" + count + " lines");
//                }
//                System.out.println(id + "   " + sb);
            }
            System.out.println("totally inserted " + count + " lines");
            pstmt.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
    }

    @Override
    public void run() {
        updateNeighbors();
    }
}