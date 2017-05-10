package xdy.graphConstruction;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;
import mysql.DBCPManager;

import java.io.*;
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


    // TODO: 2016/12/8 解析nq文件 
    public static void readAndWritreNQ(String filename, String tablename){
        Connection con = DBCPManager.getInstance().getConnection();
        if (con == null)
            throw new IllegalArgumentException("database connected error!!!");
        System.out.println(con);
        BufferedReader br;
        String line;
        int count = 0;
        try {
            PreparedStatement pstmt = con.prepareStatement("insert into " + tablename + "(s,p,o) values(?, ?, ?)");
            br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filename))));
            while ((line = br.readLine()) != null){
                int leftIndex = line.indexOf("<");
                int rightIndex = line.indexOf(">");

                String s = line.substring(leftIndex + 1, rightIndex);

                leftIndex = line.indexOf("<", rightIndex);
                rightIndex = line.indexOf(">", leftIndex);

                String p = line.substring(leftIndex + 1, rightIndex);

                leftIndex = rightIndex + 2;
                // TODO: 2016/12/8 literal
                if (line.charAt(leftIndex) == '"'){
                 rightIndex = line.indexOf('"', leftIndex + 1) + 1;
                }
                // TODO: 2016/12/8 url
                else {
                    leftIndex ++;
                    rightIndex = line.indexOf(">", leftIndex);
                }
                String o = line.substring(leftIndex, rightIndex);
                System.out.println(s + "    " + p + "   " + o);
                pstmt.setString(1, s);
                pstmt.setString(2, p);
                pstmt.setString(3, o);
                pstmt.addBatch();
                count++;
                if (count % 10000 == 0){
                    pstmt.executeBatch();
                    System.out.println("inserted   " + count);
                }
            }
            pstmt.executeBatch();
            System.out.println("totally inserted " + count);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e){
            e.printStackTrace();
        }
        finally {
        }

    }
    public static void main(String[] args) {
        String filepath = "C:\\Users\\cheng jin\\Desktop\\hgnc_complete_set.nq\\hgnc_complete_set.nq";
//        String format = "n-Triples";
        String tablename = "hgnc_graph";
//        runDir(filepath, format, tablename);
//        run(filepath, format, tablename);

        readAndWritreNQ(filepath, tablename);
    }
}
