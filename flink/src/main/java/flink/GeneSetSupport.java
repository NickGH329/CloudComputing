package flink;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.tuple.Tuple2;

public class GeneSetSupport extends RichMapFunction<GeneSet, GeneSet>{
	private static final long serialVersionUID = 1L;
	private Collection<Tuple2<String, ArrayList<Integer>>> compare;

	@Override
	public void open(Configuration parameters) throws Exception{
		this.compare = getRuntimeContext().getBroadcastVariable("compare");
	}
	@Override
	public GeneSet map(GeneSet s) throws Exception{
		GeneSet result = new GeneSet(s.genes);
		int support = 0;
		for(Tuple2<String, ArrayList<Integer>> c : compare){
			if(c.f1.containsAll(s.genes))
				support++;
		}
		result.setSupport(support);
		return result;
	}
}
