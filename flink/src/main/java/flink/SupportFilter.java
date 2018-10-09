package flink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SupportFilter implements FilterFunction<GeneSet>{
	private static final long serialVersionUID = 1L;
	private final long minSupport;

	public SupportFilter(long minSupport){
		this.minSupport = minSupport;
	}

	@Override
	public boolean filter(GeneSet s) throws Exception{
		return s.getSupport() >= this.minSupport; 
	}

}
