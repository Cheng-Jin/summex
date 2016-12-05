package graphConstruction.coverage;

 
public class CoverNode {
	int id;
	String s, p, o;
	double p_cover, class_cover;
	public CoverNode(int id, String s, String p, String o){
		this.id = id;
		this.s = s;
		this.p = p;
		this.o = o;
		this.p_cover = 0;
		this.class_cover = 0;
	}
	public void setPCover(double p_cover){
		this.p_cover = p_cover;
	}
	public void setClassCover(double class_cover){
		this.class_cover = class_cover;
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
	public double getPCover(){
		return this.p_cover;
	}
	public double getClassCover(){
		return this.class_cover;
	}
}
