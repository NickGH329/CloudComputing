package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Collections;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.*;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class Assessment2{
	public static void main(String[] args){
		final int K = Integer.parseInt(args[0]);
		String inputDataPath = args[1];
		String outputDataPath = args[2];
		SparkConf conf = new SparkConf();
		final int numIters = 10;

		conf.setAppName("find Number of measurements conducted per researcher");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> measurements = sc.textFile(inputDataPath+"/large/measurements_arcsin200_p*.csv"),
				experiments = sc.textFile(inputDataPath+"experiments.csv");
		measurements = measurements.filter(row -> !row.contains("sample"));
		experiments = experiments.filter(row -> !row.contains("sample"));
	
		JavaRDD<String> filterMeasurements = measurements.flatMap(s -> {
			List<String> result = new ArrayList<>();
			String[] values = s.split(",");
			int fsc = Integer.parseInt(values[1]);
			int ssc = Integer.parseInt(values[2]);
			if (fsc >= 1 && fsc <= 150000 && ssc >= 1 && ssc <= 150000)
				result.add(new String(values[0] + "," + values[11] + "," + values[6] + "," + values[7]));
			return result.iterator();
		});

		JavaPairRDD<String, Integer> numMeasurements = filterMeasurements.mapToPair(s -> 
		{
			String[] values = s.trim().split(",");
			return new Tuple2<String,Integer>(values[0], 1);
		}).reduceByKey((n1,n2) -> n1+n2);

		filterMeasurements.repartition(1).saveAsTextFile(outputDataPath);

		JavaPairRDD<String, String> expResearcher = experiments.filter(s -> s.split(",").length == 8 ).mapToPair(s -> {
			String[] values = s.trim().split(",");
			return new Tuple2<String,String>(values[0],values[7]);
		});

		JavaPairRDD<String, Tuple2<String,Integer>> joinResults = expResearcher.join(numMeasurements);

		JavaPairRDD<String, Integer> researcher = joinResults.values().flatMapToPair((v) -> {
			List<Tuple2<String, Integer>> results = new ArrayList<>();
			int num = v._2;
			String researcherNames[] = v._1.split(";");
			for(String researcherName : researcherNames){
				results.add(new Tuple2<>(researcherName.trim(), num));
			}
			return results.iterator();
		}).reduceByKey((n1,n2) -> n1+n2).mapToPair(s -> new Tuple2<Integer, String>(s._2, s._1)).sortByKey(true).mapToPair(s -> new Tuple2<String, Integer>(s._2, s._1));
		
		JavaRDD<String> researcherResult = researcher.map(s -> s._1 + "\t" + s._2);
		researcherResult.repartition(1).saveAsTextFile(outputDataPath + "researcher");
		
		Random random = new Random();
		double centroidX[] = new double[K];
		double centroidY[] = new double[K];
		double centroidZ[] = new double[K];
			for (int i = 0; i < K; i++){
				centroidX[i] = random.nextInt(5);
				centroidY[i] = random.nextInt(5);
				centroidZ[i] = random.nextInt(5);
			}
		

		JavaPairRDD<Integer, Iterable<String>> filterKMeans = filterMeasurements.mapToPair(s -> {
			String[] values = s.split(",");
			double x = Float.parseFloat(values[1]);
			double y = Float.parseFloat(values[2]);
			double z = Float.parseFloat(values[3]);
			double minDistance;
			double distance[] = new double[K];
	
			for ( int i = 0; i < K; i++){
                                distance[i] = Math.pow(centroidX[i] - x, 2) + Math.pow(centroidY[i] - y, 2) + Math.pow(centroidZ[i] - z, 2);
                        }
			minDistance = distance[0];
			int minNum = 0;
                        for(int i = 0; i < K; i++){
                                if ( minDistance > distance[i]){
                                        minDistance = distance[i];
                                        minNum = i;
                                }
                        }
			StringBuffer strBuf = new StringBuffer();
			for (int i = 0; i < K; i++)
				strBuf.append(centroidX[i] + "," + centroidY[i] + "," + centroidZ[i] + ",");	
			return new Tuple2<Integer, String>(minNum, new String(centroidX[minNum] + "," + centroidY[minNum] + "," + centroidZ[minNum] + "," + x + "," + y + "," + z + "," + strBuf.toString()));
		}).groupByKey();

		for(int numI = 0; numI < numIters; numI++){	
			JavaPairRDD<Integer, Iterable<String>> moveCentroid = filterKMeans.flatMapToPair((t) -> {	
			ArrayList<Tuple2<Integer,String>> results = new ArrayList<Tuple2<Integer, String>>();
			ArrayList<String> list =  new ArrayList<String>();
			double xTotal = 0;
			double yTotal = 0;
			double zTotal = 0;
			double cx = 0;
			double cy = 0;
			double cz = 0;
			for (String s : t._2){
				String[] values = s.split(",");
				xTotal += Double.parseDouble(values[3]); 
				yTotal += Double.parseDouble(values[4]); 
				zTotal += Double.parseDouble(values[5]); 
				list.add(new String(values[3] + "," + values[4] + "," + values[5]));
			}
		
			cx = xTotal/list.size();
			cy = yTotal/list.size();
			cz = zTotal/list.size();	
			centroidX[t._1] = cx;
			centroidY[t._1] = cy;
			centroidZ[t._1] = cz;
			
			for(String s : list)	
				results.add(new Tuple2<Integer, String>(1,new String(t._1 + "," + cx + "," + cy + "," + cz + "," + s)));
			return results.iterator();
		}).groupByKey();

			filterKMeans = moveCentroid.flatMapToPair( (t) -> {
				ArrayList<Tuple2<Integer, String>> results = new ArrayList<Tuple2<Integer, String>>();
				ArrayList<String> list = new ArrayList<String>();
				HashMap<Integer, String> centroidInfo = new HashMap<Integer, String>();
				for(String s : t._2){
					String[] values = s.split(",");
					list.add(new String(values[4] + "," + values[5] + "," + values[6]));
					centroidInfo.put(Integer.parseInt(values[0]), new String(values[1] + "," + values[2] + "," + values[3]));		
				} 
				double[] cx = new double[K];
				double[] cy = new double[K];
				double[] cz = new double[K];
				for(int i = 0; i < K; i++){
					String[] info = centroidInfo.get(i).split(",");
					cx[i] = Double.parseDouble(info[0]);
					cy[i] = Double.parseDouble(info[1]);
					cz[i] = Double.parseDouble(info[2]);
				}
				
				double minDistance;
				double distance[] = new double[K];

				for(String s : list){	
				String[] values = s.split(",");
				double x,y,z;
				x = Double.parseDouble(values[0]); 
				y = Double.parseDouble(values[1]); 
				z = Double.parseDouble(values[2]); 
				for ( int i = 0; i < K; i++){
                                	distance[i] = Math.pow(cx[i] - x, 2) + Math.pow(cy[i] - y, 2) + Math.pow(cz[i] - z, 2);
                        	}
				minDistance = distance[0];
				int minNum = 0;
                        	for(int i = 0; i < K; i++){
                                	if ( minDistance > distance[i]){
                                        	minDistance = distance[i];
                                        	minNum = i;
                                	}
                        	}
				results.add(new Tuple2<Integer, String>(minNum, new String(cx[minNum] + "," + cy[minNum] + "," + cz[minNum] + "," + x + "," + y + "," + z)));
				}
			
				return results.iterator();
			}).groupByKey();
		
		/*JavaRDD<String> resultKMeans = filterKMeans.flatMap((t) -> {
			LinkedList<String> results = new LinkedList<String>();
			for (String s : t._2)
				results.add(new String(t._1 + "," + t._2));
			return results.iterator(); 
		});*/
		}

		filterKMeans.repartition(1).saveAsTextFile(outputDataPath + "KMeans");
		sc.close();
	}
}
