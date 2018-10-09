package flink;

import java.util.ArrayList;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class GeneSet implements Comparable{
	public ArrayList<Integer> genes;
	private int support;

	public GeneSet(){
		this.genes = new ArrayList<>();
		this.support = 0;
	}
	public GeneSet(Integer gene){
		this.genes = new ArrayList<>();
		this.genes.add(gene);
		this.support = 1;
	}
	public GeneSet(ArrayList<Integer> geneList){
		this.genes = geneList;
		this.support = 0;
	}
	public void setSupport(int support){
		this.support = support;
	}
	public int getSupport(){
		return support;
	}
	@Override
	public boolean equals(Object obj){
		if(obj == null)
			return false;
		if(obj == this)
			return true;
		if(obj.getClass() != getClass())
			return false;

		GeneSet s = (GeneSet) obj;
		return new EqualsBuilder().appendSuper(super.equals(obj)).append(genes, s.genes).append(support, s.support).isEquals();
	}
	@Override
	public int hashCode(){
		return new HashCodeBuilder(17, 31).append(genes).append(support).toHashCode();
	}
	@Override	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for (int gene : genes)
			sb.append(gene + "\t");
		sb.append(support);
		return sb.toString();
	}
	@Override
	public int compareTo(Object o2){
		GeneSet mr = (GeneSet) o2;
		for(Integer s0 : genes){
			for(Integer s1 : mr.genes){
				if(s0 < s1)
					return -1;
				if(s0 > s1)
					return 1;
			}
		}
		return 0;
	}

}
