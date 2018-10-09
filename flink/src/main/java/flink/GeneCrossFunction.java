package flink;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.ArrayList;

public class GeneCrossFunction implements CrossFunction<GeneSet, GeneSet, GeneSet>{
	private static final long serialVersionUID = 1L;

	@Override
	public GeneSet cross(GeneSet s0, GeneSet s1) throws Exception{
		ArrayList<Integer> genes = s0.genes;
		for(Integer gene : s1.genes){
			if(!genes.contains(gene))
				genes.add(gene);
		}

		GeneSet result = new GeneSet(genes);
		result.setSupport(s0.getSupport());
		return result;
	}
}
