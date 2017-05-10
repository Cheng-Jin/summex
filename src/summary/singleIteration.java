package summary;


import DataSources.GRAPHS;
import DataSources.INFOS;

import java.util.*;

public class singleIteration {
    GRAPHS tableName;
    private int k;
    private int startNodeId;
    private double alpha, beta;
    private double totalScore;
    private boolean isSuccessful; // TODO: 2016/12/21 摘要中节点个数是否等于k
    private long computeTime;

    private Map<Integer, FullNode> totalGraph;
    private Queue<FullNode> candidateSet;

    private Set<Integer> selectedNodeIds;
    private Map<String,Double> coveredProperties;
    private Map<String, Double> coveredClasses;

    public singleIteration(GRAPHS tableName, int k, int startNodeId, double alpha, double beta,
                           Map<Integer, FullNode> totalGraph, Queue<FullNode> candidateSet) {
        this.tableName = tableName;
        this.k = k;
        this.startNodeId = startNodeId;
        this.alpha = alpha;
        this.beta = beta;
        this.totalScore = 0;
        this.isSuccessful = false;
        this.computeTime = 0;

        this.totalGraph = totalGraph;
        this.candidateSet = candidateSet;

        this.selectedNodeIds = new HashSet<>();
        this.coveredProperties = new HashMap<>();
        this.coveredClasses = new HashMap<>();
        ConstructSummary();
    }

    private void ConstructSummary() {
        long computationStartPoint = System.currentTimeMillis();

        FullNode startNode = this.totalGraph.get(this.startNodeId);
//        this.totalScore += this.alpha * startNode.getVisibility() + this.beta * startNode.getCoverage();
        this.selectedNodeIds.add(startNodeId);
        UpdateCandidateSetScores(startNode);
        updateCoveredPropertiesOrClasses(startNode);

        while (this.selectedNodeIds.size() < this.k) {
            if (this.candidateSet.size() == 0)
                break;
          /*  FullNode maxScoreNode = null;
            double maxScore = -1.0;
            for (FullNode node : this.candidateSet) {
                String p = node.getP(), o = node.getO();
                double score = this.alpha * node.getVisibility();
                if ((summary.isTypeProperty(p) && !this.coveredClasses.containsKey(o)) || (!summary.isTypeProperty(p) && !this.coveredProperties.containsKey(p)))
                    score += this.beta * node.getCoverage();
                if (score > maxScore) {
                    maxScore = score;
                    maxScoreNode = node;
                }
            }*/
            // TODO: 2016/12/21 直接从堆顶选出最大得分的顶点
            FullNode maxScoreNode = this.candidateSet.poll();
            int maxScoreNodeId = maxScoreNode.getId();

            this.selectedNodeIds.add(maxScoreNodeId);
            UpdateCandidateSetScores(maxScoreNode);
            updateCoveredPropertiesOrClasses(maxScoreNode);
        }
        updateTotalScore();
        restoreCandidateScores();

        long computationEndPoint = System.currentTimeMillis();
        this.computeTime = computationEndPoint - computationStartPoint;

        if (this.selectedNodeIds.size() == this.k)
            this.isSuccessful = true;
    }

    // TODO: 2016/12/21 更新候选集合的顶点分数
    private void UpdateCandidateSetScores(FullNode selectedNode) {
        String selectedP = selectedNode.getP(), selectedO = selectedNode.getO();
        for (FullNode node : this.candidateSet) {
            String p = node.getP(), o = node.getO();

            if (summary.isTypeProperty(p) && selectedO.equals(o))
                node.setScore(this.alpha * node.getVisibility());
            if (!summary.isTypeProperty(p) && selectedP.equals(p))
                node.setScore(this.alpha * node.getVisibility());
        }
    }

    private void updateCoveredPropertiesOrClasses(FullNode selectedNode) {
        String p = selectedNode.getP(), o = selectedNode.getO();
        if (summary.isTypeProperty(p) && !this.coveredClasses.containsKey(o))
            this.coveredClasses.put(o, selectedNode.getCcover_num());
        if (!summary.isTypeProperty(p) && !this.coveredProperties.containsKey(p))
            this.coveredProperties.put(p, selectedNode.getPcover_num());
    }

    private void updateTotalScore(){
        double totalVisibility = 0;
        for (int nodeId : this.selectedNodeIds)
            totalVisibility += this.totalGraph.get(nodeId).getVisibility();

        double coverageOfProperties = 0;
        for (Map.Entry<String, Double> entry: this.coveredProperties.entrySet()) {
            double NumOfCoveredProperties = entry.getValue();
            coverageOfProperties = Math.log(NumOfCoveredProperties + 1) / (INFOS.PROPERTIES.get(this.tableName) + 1);
        }

        double coverageOfClasses = 0;
        for (Map.Entry<String, Double> entry: this.coveredClasses.entrySet()) {
            double NumOfCoveredClasses = entry.getValue();
            coverageOfClasses = Math.log(NumOfCoveredClasses + 1) / (Math.log(INFOS.CLASSES.get(this.tableName) + 1));
        }

        this.totalScore = this.alpha * totalVisibility + this.beta * (coverageOfProperties + coverageOfClasses);
    }

    // TODO: 2016/12/21 将被修改过的后续集合中的score值修复
    public void restoreCandidateScores(){
        for (FullNode node : this.candidateSet)
            node.setScore(this.alpha * node.getVisibility() + this.beta * node.getCoverage());
    }

    public Set<Integer> getSummary() {
        return this.selectedNodeIds;
    }

    public double getTotalScore() {
        return this.totalScore;
    }

    public boolean isSuccessful() {
        return this.isSuccessful;
    }

    public long getComputeTime() {
        return this.computeTime;
    }
}

