package graph;

import DataSources.GRAPHS;
import edu.uci.ics.jung.graph.DirectedGraph;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by cheng jin on 2016/12/15.
 */
// TODO: 2016/12/15 在导入数据的同时。计算coverage，PageRank和neighbors
public class Main {
    GRAPHS tablename;
    private
    // TODO: 2016/12/15 计算coverage
    private Map<String, Double> property_covers; // TODO: 2016/11/19  property -> coverage
    private Map<String, Double> class_covers; // TODO: 2016/11/19 class -> coverage

    // TODO: 2016/12/15 计算pagerank
    public static Map<String, Double> entityScoreMap;
    DirectedGraph<String, String> pagerankGraph;
    public static double maxPagerankScore;

    public Main(GRAPHS tablename) {
        this.tablename = tablename;

        this.property_covers = new ConcurrentHashMap<>();
        this.class_covers = new ConcurrentHashMap<>();



    }
}
