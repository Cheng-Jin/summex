package graphConstruction.pagerank;

/**
 * Created by cheng jin on 2016/11/22.
 */
public class pagerankTriple {
    int id;
    String s, p, o;
    double s_pagerank, o_pagerank;

    public pagerankTriple(int id, String s, String p, String o) {
        this.id = id;
        this.s = s;
        this.p = p;
        this.o = o;
        this.s_pagerank = 0;
        this.o_pagerank = 0;
    }

    public void setS_pagerank(double s_pagerank) {
        this.s_pagerank = s_pagerank;
    }

    public void setO_pagerank(double o_pagerank) {
        this.o_pagerank = o_pagerank;
    }

    public int getId() {
        return id;
    }

    public String getS() {
        return s;
    }

    public String getP() {
        return p;
    }

    public String getO() {
        return o;
    }

    public double getS_pagerank() {
        return s_pagerank;
    }

    public double getO_pagerank() {
        return o_pagerank;
    }
}
