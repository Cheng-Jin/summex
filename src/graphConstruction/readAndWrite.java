package graphConstruction;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;
import mysql.DBCPManager;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by xdy on 2016/11/18.
 */
public class readAndWrite {
    // TODO: 2016/11/18 读取文件成rdf model
    public static Model read(String filename, String format){
        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open(filename);
        if (in == null)
            throw new IllegalArgumentException("File " + filename + " not found!");
        model.read(in, "", format);
        return model;
    }

    // TODO: 2016/11/18 将model的三元组写入数据库中，数据库中得存在相应的表
    public static void write(Model model, String tablename){
        Connection con = DBCPManager.getInstance().getConnection();
        if (con == null)
            throw new IllegalArgumentException("database connected error!!!");
        System.out.println(con);
        try {
            PreparedStatement pstmt = con.prepareStatement("insert into " + tablename + "(s,p,o) values(?, ?, ?)");
            StmtIterator stmtIterator = model.listStatements();
            int count = 0;
            while (stmtIterator.hasNext()){
                Statement stmt = stmtIterator.nextStatement();
                String s = stmt.getSubject().toString();
                String p = stmt.getPredicate().toString();
                String o = stmt.getObject().toString();
                count ++;
//                System.out.println(count + "    " + s + "   " + p + "   " + o);
                pstmt.setString(1, s);
                pstmt.setString(2, p);
                pstmt.setString(3, o);
                pstmt.addBatch();
                if (count % 10000 == 0){
                    pstmt.executeBatch();
                }
            }
            System.out.println("inserted " + count + " triples");
            pstmt.executeBatch();
//            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // TODO: 2016/11/18 总和read和write两个函数
    public static void run(String file, String format, String tablename){
        write(read(file, format), tablename);
    }

    // TODO: 2016/11/18  读取和写入目录
    public static void runDir(String filename, String format, String tablename){
        File files = new File(filename);
        for (String f : files.list()){
            write(read(files.getAbsolutePath() + File.separator + f, format), tablename);
        }
    }
    public static void main(String[] args) {
        String filepath = "f:\\datasets\\muninn-Dump-Latest.nq";
        String format = "n-quads";
        String tablename = "muninn_graph";
//        runDir(filepath, format, tablename);
        run(filepath, format, tablename);
    }
}
