import java.awt.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;
import com.mongodb.MongoClientOptions;

public class SequenceAlignmentProcess {
	
	public static void Main(String[] args){
		String process = args[0];
		// Creating Mongo Client instance and fetching the collection
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDatabase("organisms");
		DBCollection collection = db.getCollection("collection");
		DBCollection resultCollection = db.getCollection("result");
		
		ArrayList<BasicDBObject> dnaCollections = collection.find(eq("process", process).toArray();
		ArrayList<BasicDBObject> testCollections = collection.find(eq("class", "testbed")).toArray();
		
		int fraction = Integer.parseInt((String)((BasicDBObject)dnaCollections[0].get("fraction")));
		
		for(BasicDBObject testBedObject: testCollections){
			String testBedString = (String)((BasicDBObject)testBedObject.get("dna"));
			String currentClass = "";
			
			ArrayList<EditDistanceResult> editDistanceListComplete = new ArrayList<EditDistanceResult>();
			ArrayList<Double> maxDoubleCountListComplete = new ArrayList<Double>();
			ArrayList<EditDistanceResult> editDistanceListPerClass = new ArrayList<EditDistanceResult>();
			ArrayList<Double> maxDoubleCountListPerClass = new ArrayList<Double>();
			
			for (BasicDBObject trainBedString: dnaCollections){				
				if (currentClass.isEmpty()){
					currentClass = (String)((DBObject)trainBedString.get("class"));
				} else {
					if (!currentClass.equals((String)((DBObject)testBedObject.get("class")))){
						editDistanceListComplete.add(computeEditDistanceAggregate(editDistanceListPerClass));
						maxDoubleCountListComplete.add(aggregateMaxCount(maxDoubleCountListPerClass));
						editDistanceListPerClass = new ArrayList<EditDistanceResult>();
						maxDoubleCountListPerClass = new ArrayList<Double>();
						currentClass = (String)((DBObject)testBedObject.get("class"));
					}
				}
				String trainString = (String)((DBObject)trainBedString.get("dna"));
				editDistanceListPerClass.add(getEditDistanceForStringComparison(testBedString, trainString));
				maxDoubleCountListPerClass.add(getMaxCount(testBedString, trainString));
			}
			editDistanceListComplete.add(computeEditDistanceAggregate(editDistanceListPerClass));
			maxDoubleCountListComplete.add(aggregateMaxCount(maxDoubleCountListPerClass));
			Evaluation evaluation = getEvaluation(editDistanceListComplete, maxDoubleCountListComplete);
			insertDataIntoMongoCollection(evaluation.getClassName(), resultCollection, fraction, process, evaluation.getEdit(), 
					evaluation.getMaxCount(), (String)((BasicDBObject)testBedObject.get("filepath")));
		}
		
	}
	
	public static EditDistanceResult computeEditDistanceAggregate(ArrayList<EditDistanceResult> editdistancelist){
		EditDistanceResult edr = new EditDistanceResult(0, 0, 0, 0);
		for(EditDistanceResult editdistanceresult: editdistancelist){
			edr.aggregateEditDistanceElements(editdistanceresult);
		}
		return edr;
	}
	
	public static double aggregateMaxCount(ArrayList<Double> maxCountList){
		double maxCount = 0.0;
		for(double count: maxCountList){
			maxCount += count;
		}
		return maxCount;
	}
	
	public static EditDistanceResult getEditDistanceForStringComparison(String firstString, String secondString){
		EditDistance ed = new EditDistance(0, 0, 0, 0);
		return ed.editDistance(firstString, secondString);
	}
	
	public static double getMaxCount (String firstString, String secondString){
		LocalSequenceAlignment lsa = new LocalSequenceAlignment(firstString, secondString);
		return lsa.getMaxScore();
	}
	
	public static Evaluation getEvaluation(ArrayList<EditDistanceResult> editList, ArrayList<Double> maxCountList){
		double tempCount = 0.0;
		Evaluation evaluation = new Evaluation(new EditDistanceResult(Integer.MAX_VALUE,0,0,0), 0.0, "");
		int c = 0;
		Map<Integer, Double> editListMap = new HashMap<Integer, Double>();
		Map<Integer, Double> maxCountMap = new HashMap<Integer, Double>();
		
		for(EditDistanceResult ed: editList){
			editListMap.put(c, ed.getMinimumDistance());
			c++;
		}
		
		c = 0;
		for (double count : maxCountList){
			maxCountMap.put(c, count);
			c++;
		}
		
		LinkedHashMap<Integer, Double> editLinkMap = getMapSorted(editListMap, "Sort");
		LinkedHashMap<Integer, Double> maxCountLinkMap = getMapSorted(maxCountMap, "Reverse");
		
		int count = Integer.MAX_VALUE;
		int i = 0;
		int j = 0;
		
		for(int key : maxCountLinkMap.keySet()){
			j = 0;
			for (int keyV: editLinkMap.keySet()){
				if (key == keyV){
					break;
				}
				j++;
			}
			if (i + j < count){
				evaluation.setAllElements(editList.get(key), maxCountList.get(key), "class" + String.valueOf(key));
				count = i + j;
			}
			i++;
		}
		return evaluation;
	}
	
	public static void insertDataIntoMongoCollection(String classname, DBCollection collection, int fraction, String process,
		EditDistanceResult editdistanceresult, double maxCount, String filename){
		BasicDBObject doc = new BasicDBObject("class", classname).
            append("fraction", fraction).
            append("process", process).
            append("insertion", editdistanceresult.getInsertion()).
            append("deletion", editdistanceresult.getDeletion()).
            append("replacement", editdistanceresult.getReplacement()).
            append("editdistance", editdistanceresult.getMinimumDistance()).
            append("maxCount", maxCount).
            append("filename", filename);
				
         collection.insert(doc);
	}
	
	public static LinkedHashMap<Integer, Double> getMapSorted(Map<Integer, Double> anyHash, String operation){
		ArrayList<Integer> mapKeys = new ArrayList<>(anyHash.keySet());
	    ArrayList<Double> mapValues = new ArrayList<>(anyHash.values());
	    if (operation.equals("Reverse")){
	    	Collections.reverse(mapValues);
	    } else {
	    	Collections.sort(mapValues);
	    }
	    Collections.reverse(mapValues);
	    Collections.sort(mapKeys);

	    LinkedHashMap<Integer, Double> sortedMap =
	        new LinkedHashMap<>();

	    Iterator<Double> valueIt = mapValues.iterator();
	    while (valueIt.hasNext()) {
	        double val = valueIt.next();
	        Iterator<Integer> keyIt = mapKeys.iterator();

	        while (keyIt.hasNext()) {
	            int key = keyIt.next();
	            double comp1 = anyHash.get(key);
	            double comp2 = val;

	            if (comp1 == comp2) {
	                keyIt.remove();
	                sortedMap.put(key, val);
	                break;
	            }
	        }
	    }
	    return sortedMap;
	}

}
