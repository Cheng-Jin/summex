package summarization;

import java.util.HashSet;
/*
	* 	每个三元组的完整定义
 */
public class FullNode {
	public int id;
	public String s, p, o;
	public double pagerank, cover;
	public double spagerank, opagerank;
	public double pcover_num, ccover_num;
	HashSet<Integer> neighbors;

	public FullNode(int id, String s, String p, String o, 
			double spagerank, double opagerank, double cover, double pcover_num, double ccover_num){
		this.id = id;
		this.s = s;
		this.p = p;
		this.o = o;
		this.spagerank = spagerank;
		this.opagerank = opagerank;
		if(this.spagerank == 0 || this.opagerank == 0){
			this.pagerank = Math.max(spagerank, opagerank);
		} else {
			this.pagerank = (this.spagerank + this.opagerank) / 2;
		}
		this.cover = cover;
		this.pcover_num = pcover_num;
		this.ccover_num = ccover_num;
		neighbors = new HashSet<Integer>();
	}

	public void AddNeighbors(HashSet<Integer> neighbors){
		this.neighbors.addAll(neighbors);
	}
	public HashSet<Integer> getNeighbors(){
		return this.neighbors;
	}
	public int getId(){
		return this.id;
	}
	public String getS(){
		return this.s;
	}
	public String getP(){
		return this.p;
	}
	public String getO(){
		return this.o;
	}
	public double getPageRank(){
		return this.pagerank;
	}
	public double getCover(){
		return this.cover;
	}
	public double getPCoverNum(){
		return this.pcover_num;
	}
	public double getCCoverNum(){
		return this.ccover_num;
	}
	
	public double getSPagerank(){
		return this.spagerank;
	}
	
	public double getOPagerank(){
		return this.opagerank;
	}

	@Override
	public String toString() {
		return this.s + "	" + this.p + "	" + this.o + "	" + this.pagerank + "	" + this.spagerank + "	" + this.opagerank +"	" + this.cover + "	" + this.pcover_num + "	" + this.ccover_num;
	}
}

