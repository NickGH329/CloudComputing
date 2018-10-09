package flink;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CompareGroupReduceFunction implements GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, ArrayList<Integer>>>{
	private static final long serialVersionUID = 1L;
	@Override
	public void reduce(Iterable<Tuple2<String, Integer>> s0, Collector<Tuple2<String, ArrayList<Integer>>> s1) throws Exception{
		ArrayList<Integer> genes = new ArrayList<>();
		String patient = null;
		for( Tuple2<String, Integer> s : s0){
			genes.add(s.f1);
			patient = s.f0;
		}
	s1.collect(new Tuple2<String, ArrayList<Integer>>(patient, genes));
	}
}
