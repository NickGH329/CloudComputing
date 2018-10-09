package flink;

import org.apache.flink.api.common.functions.ReduceFunction;

public class GeneSetReduceFunction implements ReduceFunction<GeneSet>{
	private static final long serialVersionUID = 1L;
	@Override
	public GeneSet reduce(GeneSet s0, GeneSet s1) throws Exception{
		GeneSet result = new GeneSet(s0.genes);
		result.setSupport(s0.getSupport() + s1.getSupport());
		return result;
	}
}
