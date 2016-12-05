import mysql.DBCPManager;

import java.sql.*;

/**
 * Created by xdy on 2016/11/20.
 */
public class demo {

    public static void main(String[] args) {
        String[] datasets = {"geo_graph"};
        insert(datasets);
    }

    public static Connection getLocalConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/wsdm", "root", "jc8034");
            con.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

        }
        return con;
    }

    public static void insert(String[] daatasets) {

//        String tablename = "dogfood_graph_modified";
        for (String tablename : daatasets) {
            Connection con = getLocalConnection();
            int count = 0;
            try {
                PreparedStatement local = con.prepareStatement("insert into " + tablename + "(s, p, o, s_pagerank, o_pagerank, p_cover, class_cover, neighbors) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
                local.setString(1, tablename);

                DBCPManager db = DBCPManager.getInstance();
                Connection conn = null;
                PreparedStatement stmt = null;
                ResultSet rs = null;
                try {
                    conn = db.getConnection();
                    stmt = conn.prepareStatement("select id, s, p, o, s_pagerank, o_pagerank, p_cover, class_cover, neighbors from " + tablename);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        int id = rs.getInt(1);
                        String s = rs.getString(2);
                        String p = rs.getString(3);
                        String o = rs.getString(4);
                        double s_pagerank = rs.getDouble(5);
                        double o_pagerank = rs.getDouble(6);
                        double p_cover = rs.getDouble(7);
                        double class_cover = rs.getDouble(8);
                        String neighbors = rs.getString(9);

    //                    local.setInt(1, id);
                        local.setString(1, s);
                        local.setString(2, p);
                        local.setString(3, o);
                        local.setDouble(4, s_pagerank);
                        local.setDouble(5, o_pagerank);
                        local.setDouble(6,p_cover);
                        local.setDouble(7, class_cover);
                        local.setString(8, neighbors);
                        local.addBatch();
    //                    System.out.println(local);
                        if (count % 10000 == 0){
                            System.out.println(count + "triples inserted!");
                            local.executeBatch();
                        }
                        count++;
                    }
                    System.out.println("totally inserted " + count + "  triples!");
                    local.executeBatch();
                    con.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    db.free(rs, stmt, conn);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                }
            }
        }
    }
}
