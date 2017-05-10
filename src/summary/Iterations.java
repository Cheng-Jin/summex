package summary;

import DataSources.GRAPHS;
import DataSources.INFOS;
import mysql.DBCPManager;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Iterations {

    public static String WORKPATH = "";
    public static final String WORKPATH_WINDOWS = "C:/Users/cheng jin/Desktop/";
    public static final String WORKPATH_LINUX = "/home/jincheng/";

    static {
        if (System.getProperty("os.name").startsWith("Win"))
            WORKPATH = WORKPATH_WINDOWS;
        else WORKPATH = WORKPATH_LINUX;
    }

    private GRAPHS tableName;
    private int totalIterations; // TODO: 2016/12/21 总迭代次数
    private int k;
    private double alpha, beta;

    private Map<Integer, FullNode> totalGraph;      //TODO: 2016/12/21 完整的图，图中每个三元组的id以及对应的节点
    //    Map<String, String> subjectTypes;        // TODO: 2016/12/21 每个实体对应的类型，不存在即为nontype
    private Map<String, SortedSet<FullNode>> entityNodesInvertedIndex; // TODO: 2016/12/21 每个实体以及它所在三元组的倒排索引
    //    Map<String, Integer> NumOfPropertyOrClassInstances; // TODO: 2016/12/21 每个property或者class对应的实例数目
//    private SortedSet<FullNode> rankingQueue;  // TODO: 2016/12/21 依照alpha * visibility + beta * coaverage排序构成队列
    private Map<String, SortedSet<FullNode>> topKPropertiesOrClasses; // TODO: 2016/12/22 每个property或class的前topk个三元组

    public Iterations(GRAPHS tableName, int totalIterations, int k, double alpha, double beta) {
        this.tableName = tableName;
        this.totalIterations = totalIterations;
        this.k = k;
        this.alpha = alpha;
        this.beta = beta;

        this.totalGraph = new HashMap<>();
        this.entityNodesInvertedIndex = new HashMap<>();
//        this.rankingQueue = new TreeSet<>();
        this.topKPropertiesOrClasses = new HashMap<>();
    }

    // TODO: 2016/12/21 判断o是否是实体
    public static boolean isEntity(String p, String o) {
        if (!o.startsWith("http"))
            return false;
        p = p.toLowerCase();
        if (p.endsWith("url"))
            return false;
        if (p.endsWith("page"))
            return false;
        if (p.endsWith("sameas"))
            return false;
        if (p.endsWith("seealso"))
            return false;
        if (p.endsWith("type"))
            return false;
        return true;
    }

    // TODO: 2016/12/21 判读property的类型，是否是type
    public static boolean isTypeProperty(String p) {
        return p.toLowerCase().endsWith("type");
    }

    public void recordTime(int NumOfCurrentIteration, long time, boolean hasFinished) {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(WORKPATH + this.tableName + "/time_" + this.k + "_" + this.alpha + ".time"))));
            bw.write(NumOfCurrentIteration + "  " + time);
            bw.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (hasFinished) {
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void loadGraph() {
        System.out.println("Graph Loading...");

        DBCPManager db = DBCPManager.getInstance();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = db.getConnection();
            stmt = conn.prepareStatement("SELECT id, s, p, o, s_pagerank, o_pagerank, p_cover, class_cover FROM " + tableName);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt(1);
                String s = rs.getString(2);
                String p = rs.getString(3);
                String o = rs.getString(4);


                double visibility;
                double s_visibility = rs.getDouble(5);
                double o_visibility = rs.getDouble(6);
                if (o_visibility == 0)
                    visibility = s_visibility;
                else visibility = (s_visibility + o_visibility) / 2;

                double coverage;
                double p_cover_num = 0;
                double c_cover_num = 0;
                if (isTypeProperty(p)) {
                    c_cover_num = rs.getDouble(8);
                    coverage = Math.log(c_cover_num + 1) / Math.log(INFOS.CLASSES.get(this.tableName) + 1);
                } else {
                    p_cover_num = rs.getDouble(7);
                    coverage = Math.log(p_cover_num + 1) / Math.log(INFOS.PROPERTIES.get(this.tableName) + 1);
                }

                double score = this.alpha * visibility + this.beta * coverage;

                // TODO: 2016/12/21 填充totalGraph
                FullNode node = new FullNode(id, s, p, o, visibility, s_visibility, o_visibility, coverage, p_cover_num, c_cover_num, score);
                this.totalGraph.put(id, node);

                // TODO: 2016/12/15 填充entity -> id倒排索引
                SortedSet<FullNode> ids = this.entityNodesInvertedIndex.get(s);
                if (ids == null) {
                    ids = new TreeSet<>();
                    this.entityNodesInvertedIndex.put(s, ids);
                }
                ids.add(node);

                if (isEntity(p, o)) {
                    ids = this.entityNodesInvertedIndex.get(o);
                    if (ids == null) {
                        ids = new TreeSet<>();
                        this.entityNodesInvertedIndex.put(o, ids);
                    }
                    ids.add(node);
                }

                // TODO: 2016/12/22 填充每个属性或类的topK个三元组
                if (isTypeProperty(p)) {
                    SortedSet<FullNode> nodes = this.topKPropertiesOrClasses.get(o);
                    if (nodes == null) {
                        nodes = new TreeSet<>();
                        this.topKPropertiesOrClasses.put(o, nodes);
                    }
                    nodes.add(node);
                } else {
                    SortedSet<FullNode> nodes = this.topKPropertiesOrClasses.get(p);
                    if (node == null) {
                        nodes = new TreeSet<>();
                        this.topKPropertiesOrClasses.put(p, nodes);
                    }
                    nodes.add(node);
                }
//                // TODO: 2016/12/21 填充排序的队列
//                this.rankingQueue.add(node);

                // TODO: 2016/12/21 填充实体对应的类型
                // TODO: 2016/12/21 填充每个property或者class的实例数目
//                if (isTypeProperty(p)) {
//                    this.subjectTypes.put(s, o);

//                    if (this.NumOfPropertyOrClassInstances.containsKey(o)) {
//                        this.NumOfPropertyOrClassInstances.replace(o, this.NumOfPropertyOrClassInstances.get(o) + 1);
//                    }
//                    else this.NumOfPropertyOrClassInstances.put(o, 1);
//                } else {
//                    if (!this.subjectTypes.containsKey(s)) {
//                        subjectTypes.put(s, "NOTYPE");
//                    }
//
//                    if (this.NumOfPropertyOrClassInstances.containsKey(p)) {
//                        this.NumOfPropertyOrClassInstances.replace(p, this.NumOfPropertyOrClassInstances.get(p) + 1);
//                    }
//                    else this.NumOfPropertyOrClassInstances.put(p, 1);
            }//while
        } //try
        catch (SQLException e) {
            e.printStackTrace();
        } finally {
            db.free(rs, stmt, conn);
        }
        System.out.println("Entity and ID Inverted Index Loaded!");
        System.out.println("Ranking Queue Loaded!");
        System.out.println("Total Graph Loaded!");
    }

    public void iterateFromEveryNode() {
        int NumOfCurrentIterations = 0;
        double score = 0.0;
        Set<FullNode> maxScoreSummary = null;
        double averageVisibility = 0.0;
        double coverageOfProperties = 0.0;
        double coverageOfClasses = 0.0;
        double totalVisibility = 0.0;
        double numOfCoveredEntities = 0.0;
        long usedTime = 0;
        boolean hasFinished = false;

        List<FullNode> currentSummary = new ArrayList<>();
//        while (it.hasNext() && NumOfCurrentIterations < this.iterations) {
        for (Map.Entry<Integer, FullNode> entry : this.totalGraph.entrySet()) {
            if (NumOfCurrentIterations < this.totalIterations) {

                int currentNodeId = entry.getKey();
                FullNode currentNode = entry.getValue();
                // TODO: 2016/12/21 构造当前节点的候选集合

                singleIteration single = new singleIteration(this.tableName, this.k, currentNode, this.alpha, this.beta, this.entityNodesInvertedIndex, this.topKPropertiesOrClasses);
                long singleIterationTime = single.getComputeTime();
                if (NumOfCurrentIterations % 10000 == 0)
                    System.out.println(NumOfCurrentIterations + "	" + score + "	" + averageVisibility + "	" + totalVisibility + "	" + numOfCoveredEntities + "	" + coverageOfProperties + "	" + coverageOfClasses);
                usedTime = usedTime + singleIterationTime;
                if (NumOfCurrentIterations % 10 == 0) // TODO: 2016/12/22 每10轮记录一次时间
                    recordTime(NumOfCurrentIterations, usedTime, hasFinished);
                NumOfCurrentIterations++;

                if (!single.isSuccessful()) {
                    continue;
                }
                double currentScore = single.getTotalScore();
                Set<FullNode> currentSummary = single.getSummary();

                if ((currentScore - score) > 0.0000000001) {
                    score = currentScore;
                    maxScoreSummary = currentSummary;
//                    coverageOfProperties = getPCover(single_summary);
//                    coverageOfClasses = getCCover(single_summary);
//                    averageVisibility = getPagerankScore(single_summary);
//                    totalVisibility = this.getTotalPagerankScore(single_summary);
//                    numOfCoveredEntities = this.getEntities(single_summary);
//						this.writeResult(new Iterations(single_summary, score, total_entity_num, total_pagerank, p_cover, c_cover, pagerank));
                }
            }
            else break;
        }
        this.writeResult(new Iterations(currentSummary, score, totalEntityNum, totalVisibility, p_coverage, c_coverage, visibility);
        System.out.println(NumOfCurrentIterations+"	"+score+"	"+visibility+"	"+totalVisibility+"	"+totalEntityNum+"	"+p_coverage+"	"+c_coverage);
}

    public void writeResult(Iterations summary) {
        try {
            File file = new File(WORKPATH + this.tableName + "/summary_" + this.R + "_" + this.alpha + ".txt");
            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
            List<FullNode> nodes = summary.iterateFromEveryNode();

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
        return p_cover / INFOS.PROPERTIES.get(this.tableName);
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
        return c_cover / INFOS.CLASSES.get(this.tableName);
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
//        GRAPHS tableName = GRAPHS.sabiork_graph;
//        for (int R : Rs) {
//            for (double alpha : alphas) {
//                summarization s = new summarization(iterations, R, alpha, 1 - alpha, tableName, func);
//                s.iterateFromEveryNode();
//            }
//        }
    }
}
