package flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Task2Map implements MapFunction<GeneSet, Tuple2<Integer, String>>{
	@Override
	public Tuple2<Integer, String> map(GeneSet s) throws Exception{
		StringBuffer sb = new StringBuffer();
		for(Integer gene : s.genes)
			sb.append(gene + "\t");
		return new Tuple2<Integer, String>(s.getSupport(), sb.toString());	
	}
}
