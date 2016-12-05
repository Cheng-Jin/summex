package summarization;

import java.util.ArrayList;

/**
 * Created by cheng jin on 2016/11/25.
 */
public class summary {
    ArrayList<FullNode> summary;
    double score;
    double entityNum;
    double total_pagerank;
    double p_cover, c_cover, pagerank;

    public summary(ArrayList<FullNode> summary, double score, double entityNum, double total_pagerank, double p_cover, double c_cover, double pagerank) {
        this.summary = summary;
        this.score = score;
        this.entityNum = entityNum;
        this.total_pagerank = total_pagerank;
        this.p_cover = p_cover;
        this.c_cover = c_cover;
        this.pagerank = pagerank;
    }

    public ArrayList<FullNode> getSummary() {
        return summary;
    }

    public double getScore() {
        return score;
    }

    public double getEntityNum() {
        return entityNum;
    }

    public double getTotal_pagerank() {
        return total_pagerank;
    }

    public double getP_cover() {
        return p_cover;
    }

    public double getC_cover() {
        return c_cover;
    }

    public double getPagerank() {
        return pagerank;
    }
}
