package graphConstruction.neighbors;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by xdy on 2016/11/20.
 */
// TODO: 2016/11/20 计算每个三元组时用到的数据结构
public class neighborNode {
    int id;
    String s, o;
    String neighbors;

    public neighborNode(int id, String s, String o) {
        this.id = id;
        this.s = s;
        this.o = o;
        this.neighbors = new String();
    }

    public int getId() {
        return id;
    }

    public String getS() {
        return s;
    }

    public String getO() {
        return o;
    }

    public void setNeighbors(String neighbors) {
        this.neighbors = neighbors;
    }

    public String getNeighbors() {
        return neighbors;
    }
}
