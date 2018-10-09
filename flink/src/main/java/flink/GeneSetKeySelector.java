package flink;

import java.util.ArrayList;
import java.util.Collections;
import org.apache.flink.api.java.functions.KeySelector;

public class GeneSetKeySelector implements KeySelector<GeneSet, String>{
	private static final long serialVersionUID = 1L;
	@Override
	public String getKey(GeneSet s) throws Exception{
		String key = null;
		ArrayList<Integer> genes = s.genes;
		Collections.sort(genes);
		for (Integer gene : genes){
			key += gene.toString();
		}
		return key;
	}
}
