package flink;

import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.operators.IterativeDataSet;
import java.util.Collections;

public class Assessment3{
	public static void main(String[] args) throws Exception{
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		String inputDataPath = args[0];
		String outputDataPath = args[1];
		int ITERATE = 5;

		//GEO
		DataSet<Tuple2<String,String>> geo = env.readTextFile(inputDataPath + "GEO.txt").filter(row -> !row.contains("patientid")).map(line -> new Tuple2<String, String>(line.split(",")[0], new String(line.split(",")[1] + "," + line.split(",")[2])));

		//PatientMetaData
		DataSet<String> patientMetaData = env.readTextFile(inputDataPath + "PatientMetaData.txt").filter(row -> !row.contains("id"));
		
		DataSet<Tuple2<String, String>> cancerPatient = patientMetaData.flatMap((line, out) -> {
			String[] values = line.split(",");
			String[] diseases = values[4].split("\\s+");
			int length = diseases.length;

			StringBuffer str = new StringBuffer();
			for(int i = 0; i < length; i++){
				if(diseases[i].equals("breast-cancer") || diseases[i].equals("prostate-cancer") || diseases[i].equals("leukemia") || diseases[i].equals("lymphoma") || diseases[i].contains("cancer"))
					str.append(diseases[i] + ",");
			}
			out.collect(new Tuple2<String, String>(values[0], str.toString()));
		});

		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> joinResult = 
			geo.join(cancerPatient).where(0).equalTo(0);
		//join Result
		DataSet<Tuple2<String, String>> cancerInfo = joinResult.flatMap((s, out) -> {
			if (!(s.f1.f1.equals("") || s.f1.f1 == null)){
				out.collect(new Tuple2<String, String>(s.f0.f0, new String(s.f0.f1 + "," + s.f1.f1)));
			}
		});

		//task1
		DataSet<Tuple2<String, Integer>> task1 = cancerInfo.flatMap((s, out) ->{
			String[] values = s.f1.split(",");
			int length = values.length;
			if(Integer.parseInt(values[0]) == 42 && Double.parseDouble(values[1]) > 1250000){
				for(int i = 2; i < length; i ++)
					out.collect(new Tuple2<String,Integer>(values[i],1));
			}
		});
		DataSet<String> task1Final = task1.groupBy(0).sum(1).setParallelism(1).sortPartition(1, Order.DESCENDING).map(line -> new String(line.f0 + "\t" + line.f1));;
		
		//cancerInfo.writeAsCsv(outputDataPath + "join");
		task1Final.writeAsText(outputDataPath + "task1");

		//task2
		DataSet<Tuple2<String,Integer>> cancerGenes = cancerInfo.flatMap((s, out) -> {
			String[] values = s.f1.split(",");
			if(Double.parseDouble(values[1]) >= 1250000)
				out.collect(new Tuple2<String, Integer>(s.f0, Integer.parseInt(values[0])));
		});

		//get the patient number
		cancerPatient = cancerPatient.flatMap((s, out) -> {
			if(!(s.f1.equals("") || s.f1 == null))
				out.collect(new Tuple2<String, String>(s.f0, s.f1));	
		});
		//cancerPatient.map(s -> s).setParallelism(1).writeAsCsv(outputDataPath + "patients");
		//cancerGenes.map(s -> s).setParallelism(1).writeAsCsv(outputDataPath + "cancerGenes");
		long patientNumber = (long) cancerPatient.count();
		//minSupport
		long minSupport = (long) (patientNumber * 0.3);
		
		//DataSet<Tuple2<String, Integer>> g1 = cancerGenes.map(s -> new Tuple2<String, Integer>(s.f1.toString(), 1)).groupBy(0).sum(1).filter(new SupportFilter(minSupport));

		//DataSet<Tuple2<String, Integer>> candidates = g1.cross(g1).with(new GeneCrossFunction());
		//g1.writeAsCsv(outputDataPath + "task2");
		//candidates.writeAsCsv(outputDataPath + "test");


		//compare
		DataSet<Tuple2<String, ArrayList<Integer>>> compare = cancerGenes.groupBy(0).reduceGroup(new CompareGroupReduceFunction());

		DataSet<GeneSet> gene1 = cancerGenes.map(s -> new GeneSet(s.f1)).groupBy(new GeneSetKeySelector()).reduce(new GeneSetReduceFunction()).filter(new SupportFilter(minSupport));
		DataSet<GeneSet> geneAll = cancerGenes.map(s -> new GeneSet(s.f1)).groupBy(new GeneSetKeySelector()).reduce(new GeneSetReduceFunction());
			
		//start the loop	
		IterativeDataSet<GeneSet> start = gene1.iterate(ITERATE);
		DataSet<GeneSet> crossData = start.cross(gene1).with(new GeneCrossFunction()).distinct(new GeneSetKeySelector());
		DataSet<GeneSet> geneSupport = crossData.map(new GeneSetSupport()).withBroadcastSet(compare, "compare").filter(new SupportFilter(minSupport));
		//DataSet<GeneSet> geneSupport = crossData.map(new GeneSetSupport()).withBroadcastSet(compare, "compare");
		DataSet<GeneSet> finalResult = start.closeWith(geneSupport, geneSupport);
		DataSet<Tuple2<Integer, String>> task2 = finalResult.map(new Task2Map()).setParallelism(1).sortPartition(0, Order.DESCENDING);
		DataSet<String> task2Output = task2.map(s -> new String(s.f0 + "\t" + s.f1));
		task2Output.writeAsText(outputDataPath + "task2");

		//task3
		DataSet<Tuple3<Integer, String, Integer>> task3Input = finalResult.flatMap((s, out) -> {
		if(s.genes.size() > 1){
			ArrayList<Integer> temp = s.genes;
			for(int i = 0; i < s.genes.size(); i++){
				StringBuffer sb = new StringBuffer();
				for(int j = 0; j < temp.size(); j++){
					if(temp.get(j) != s.genes.get(i))
						sb.append(temp.get(j) + ",");
				}	
				out.collect(new Tuple3<Integer, String, Integer>(s.genes.get(i), sb.toString(), s.getSupport()));
			}
		}
	});
		DataSet<Tuple2<Integer, Integer>> gene1Set = geneAll.map(s -> new Tuple2<Integer, Integer>(s.genes.get(0), s.getSupport()));
		DataSet<Tuple4<Integer, String, Integer, Integer>> task3Join = task3Input
		.join(gene1Set)
		.where(0)
		.equalTo(0)
		.projectFirst(0,1,2)
		.projectSecond(1);

		//task3Join.map( s -> s).setParallelism(1).sortPartition(0, Order.DESCENDING).writeAsCsv(outputDataPath + "task3Join");
		//gene1Set.map( s -> s).setParallelism(1).sortPartition(1, Order.DESCENDING).writeAsCsv(outputDataPath + "gene1Set");
		//task3Input.map( s -> s).setParallelism(1).sortPartition(0, Order.DESCENDING).writeAsCsv(outputDataPath + "task3Input");
		DataSet<Tuple3<Integer,String, Float>> task3 = task3Join.flatMap((s, out) -> {
			float confidence = 0;
			confidence = (float) s.f2/s.f3;
			String[] values = s.f1.split(",");
			StringBuffer sb = new StringBuffer();
			for(int i = 0; i < values.length; i++){
				if(i == 0)
					sb.append("(");
				if(i == values.length-1)
					sb.append(values[i]+ ")");
				else
					sb.append(values[i] + " ");
			}
			if(confidence >= 0.6)
			out.collect(new Tuple3<Integer, String, Float>(s.f0, new String(sb.toString()), confidence));
		});
		DataSet<Tuple3<Integer, String, Float>> task3Output = task3.map(s -> s).setParallelism(1).sortPartition(2, Order.DESCENDING);
		task3Output.map( s -> new String(s.f0 + "\t" + s.f1 + "\t" + s.f2)).writeAsText(outputDataPath + "task3");
		//task3Join.map( s -> s).setParallelism(1).writeAsText(outputDataPath + "task3J");
		env.execute();
	}
}
