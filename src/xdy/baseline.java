package xdy;

import DataSources.GRAPHS;
import DataSources.INFOS;
import mysql.DBCPManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by cheng jin on 2016/12/19.
 */
public class baseline {
    private GRAPHS tablename;
    private PriorityQueue<FullNode> triples;
    private ArrayList<FullNode> summary20;
    private ArrayList<FullNode> summary40;

    public baseline(GRAPHS tablename) {
        this.tablename = tablename;
        this.triples = new PriorityQueue<>();
        this.summary20 = new ArrayList<>();
        this.summary40 = new ArrayList<>();

        loadTriples();
    }

    public void loadTriples(){
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
                triples.offer(node);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            db.free(rs, stmt, conn);
        }
        for (FullNode triple : this.triples){
            if (this.summary20.size() < 20)
                this.summary20.add(triple);
            if (this.summary40.size() < 40)
                this.summary40.add(triple);
            else break;
        }
        System.out.println("k = 20");
        System.out.println("pcover: " + getPCover(summary20) + "    ccover: " +getCCover(summary20) + " vis: " + getPagerankScore(summary20));
        System.out.println();
        System.out.println("k = 40");
        System.out.println("pcover: " + getPCover(summary40) + "    ccover: " +getCCover(summary40) + " vis: " + getPagerankScore(summary40));
    }

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

    public static void main(String[] args) {
        GRAPHS tablename = GRAPHS.sabiork_graph;
        new baseline(tablename);
    }
}
