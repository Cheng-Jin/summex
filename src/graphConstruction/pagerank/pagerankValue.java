package graphConstruction.pagerank;

import DataSources.GRAPHS;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import mysql.DBCPManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by cheng jin on 2016/11/22.
 */
public class pagerankValue {
    GRAPHS tablename;
    Map<String, Double> entityScoreMap;
    DirectedGraph<String, String> pagerankGraph;
    List<pagerankTriple> triples; // TODO: 2016/11/22 记录所有三元组

    double maxPagerankScore;

    public pagerankValue(GRAPHS tablename) {
        this.tablename = tablename;
        this.entityScoreMap = new HashMap<>();
        this.pagerankGraph = new DirectedSparseGraph<>();
        this.triples = new LinkedList<>();
        this.maxPagerankScore = 0.0;

        getAllData();
        compute();
        updatePagerank();
    }

    // TODO: 2016/11/22 填充数据
    public void getAllData(){
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("select id, s,p,o FROM " + this.tablename);
            rs = pstmt.executeQuery();
            int count = 0;
            while (rs.next()){
                int id = rs.getInt(1);
                String s = rs.getString(2);
                String p = rs.getString(3);
                String o = rs.getString(4);

                this.entityScoreMap.put(s, 0.0);
                this.pagerankGraph.addVertex(s);
                this.triples.add(new pagerankTriple(id, s, p, o));

                // TODO: 2016/11/22 只有object也为实体时，才构成子图
                if (o.startsWith("http")){
                    if (!p.endsWith("url") && !p.endsWith("page") && !p.endsWith("sameAs") && !p.endsWith("seeAlso") && !p.endsWith("type")){
                        this.entityScoreMap.put(o, 0.0);
                        this.pagerankGraph.addEdge(p + s + Integer.toString(count),s,o);
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

    public void updatePagerank(){
        DBCPManager db = DBCPManager.getInstance();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int count = 0;
        try {
            con = db.getConnection();
            pstmt = con.prepareStatement("update " + this.tablename + " set s_pagerank = ?, o_pagerank = ? where id = ?");
            for (pagerankTriple triple : this.triples){
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

                if (count % 1000 == 0){
                    pstmt.executeBatch();
                    System.out.println("updated " + count + "triples!" );
                }
            }
            pstmt.executeBatch();
            System.out.println("max pagerank score: "  + this.maxPagerankScore);
            System.out.println("totally updated " + count + " triples");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.free(rs, pstmt, con);
        }
    }

    public static void main(String[] args) {
        GRAPHS graph = GRAPHS.myexperiment_graph;
        new pagerankValue(graph);
    }
}
