package xdy.graphConstruction.pagerank;

import DataSources.GRAPHS;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import mysql.DBCPManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by cheng jin on 2016/11/22.
 */
public class pagerankValue {
    GRAPHS tablename;
    public static Map<String, Double> entityScoreMap;
    DirectedGraph<String, String> pagerankGraph;
    public static List<pagerankTriple> triples; // TODO: 2016/11/22 记录所有三元组

    public static double maxPagerankScore;

    public pagerankValue(GRAPHS tablename) {
        this.tablename = tablename;
//        this.entityScoreMap = new HashMap<>();
        this.entityScoreMap = new ConcurrentHashMap<>();
        this.pagerankGraph = new DirectedSparseGraph<>();
        this.triples = Collections.synchronizedList(new ArrayList<>());
        this.maxPagerankScore = 0.0;

        getAllData();
        compute();
//        updatePagerank();
    }

    // TODO: 2016/11/22 填充数据
    public void getAllData() {
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("select id, s,p,o FROM " + this.tablename);
            rs = pstmt.executeQuery();
            int count = 0;
            while (rs.next()) {
                int id = rs.getInt(1);
                String s = rs.getString(2);
                String p = rs.getString(3);
                String o = rs.getString(4);

                this.entityScoreMap.put(s, 0.0);
                this.pagerankGraph.addVertex(s);
                this.triples.add(new pagerankTriple(id, s, p, o));

                // TODO: 2016/11/22 只有object也为实体时，才构成子图
                if (o.startsWith("http")) {
                    if (!p.endsWith("url") && !p.endsWith("page") && !p.endsWith("sameAs") && !p.endsWith("seeAlso") && !p.endsWith("type")) {
                        this.entityScoreMap.put(o, 0.0);
                        this.pagerankGraph.addEdge(p + s + Integer.toString(count), s, o);
                        count++;
//                        System.out.println(id + "   " + s + "   " + p + "   " + o);
                    }
                }
            }
            System.out.println("data loaded!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
    }

    public void compute() {
        PageRank ranker = new PageRank(this.pagerankGraph, 0.15);
        ranker.evaluate();
        for (Map.Entry<String, Double> entry : this.entityScoreMap.entrySet()) {
            String entity = entry.getKey();
            double score = (double) ranker.getVertexScore(entity);
            this.entityScoreMap.replace(entity, score);
            if (this.maxPagerankScore < score)
                this.maxPagerankScore = score;
        }
        System.out.println("pagerank computed!");

    }

    public List<pagerankTriple> getTriples() {
        return triples;
    }
/* public void updatePagerank() {
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int count = 0;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("update " + this.tablename + " set s_pagerank = ?, o_pagerank = ? where id = ?");
            for (pagerankTriple triple : this.triples) {
                int id = triple.getId();
                String s = triple.getS();
                String o = triple.getO();

                double s_pagernk = 0, o_pagerank = 0;
                if (this.entityScoreMap.containsKey(s))
                    s_pagernk = this.entityScoreMap.get(s) / this.maxPagerankScore;
                if (this.entityScoreMap.containsKey(o))
                    o_pagerank = this.entityScoreMap.get(o) / this.maxPagerankScore;
//                if (s_pagernk == 1 || o_pagerank == 1)
//                System.out.println(id + "   " + s + "   " + o + "   " + s_pagernk + "   " + o_pagerank);
                pstmt.setDouble(1, s_pagernk);
                pstmt.setDouble(2, o_pagerank);
                pstmt.setInt(3, id);
                pstmt.addBatch();
                count++;

                if (count % 10000 == 0) {
                    pstmt.executeBatch();
                    System.out.println("updated " + count + "triples!");
                }
            }
            pstmt.executeBatch();
            System.out.println("max pagerank score: " + this.maxPagerankScore);
            System.out.println("totally updated " + count + " triples");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
    }*/

    public static void main(String[] args) {
        GRAPHS graph = GRAPHS.interpro_graph;
        List<pagerankTriple> list = new pagerankValue(graph).getTriples();
        int taskNumber = list.size();
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
        ThreadPoolExecutor exec = new ThreadPoolExecutor(4, 5, 7, TimeUnit.DAYS, queue);
        for (int i = 1587700; i <= taskNumber; i = i + 10000) {
            int start = i;
            int end = i + 9999;
            if (end >= taskNumber)
                end = taskNumber - 1;

            Runnable task = new pagerankUpdater(graph, start, end);
            exec.execute(task);
            System.out.println("执行从" + i + "到" + end + "的更新任务");
        }
        exec.shutdown();
    }
}

class pagerankUpdater implements Runnable {
    GRAPHS tablename;
    int start, end;

    public pagerankUpdater(GRAPHS tablename, int start, int end ) {
        this.tablename = tablename;
        this.start = start;
        this.end = end;
    }

    public void write() {
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int count = 0;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("update " + this.tablename + " set s_pagerank = ?, o_pagerank = ? where id = ?");
//            for (pagerankTriple triple : this.triplesPart) {
            for (int i = start; i <= end; i++){
                pagerankTriple triple = pagerankValue.triples.get(i);
                int id = triple.getId();
                String s = triple.getS();
                String o = triple.getO();

                double s_pagernk = 0, o_pagerank = 0;
                if (pagerankValue.entityScoreMap.containsKey(s))
                    s_pagernk = pagerankValue.entityScoreMap.get(s) / pagerankValue.maxPagerankScore;
                if (pagerankValue.entityScoreMap.containsKey(o))
                    o_pagerank = pagerankValue.entityScoreMap.get(o) / pagerankValue.maxPagerankScore;
//                if (s_pagernk == 1 || o_pagerank == 1)
//                System.out.println(id + "   " + s + "   " + o + "   " + s_pagernk + "   " + o_pagerank);
                pstmt.setDouble(1, s_pagernk);
                pstmt.setDouble(2, o_pagerank);
                pstmt.setInt(3, id);
                pstmt.addBatch();
                count++;

                if (count % 10000 == 0) {
                    pstmt.executeBatch();
                    System.out.println("updated " + count + "triples!");
                }
            }
            pstmt.executeBatch();
            System.out.println("max pagerank score: " + pagerankValue.maxPagerankScore);
            System.out.println("totally updated " + count + " triples");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
    }

    @Override
    public void run() {

        write();
    }
}