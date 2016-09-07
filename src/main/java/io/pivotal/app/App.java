package io.pivotal.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.snapshot.SnapshotIterator;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;
import com.gemstone.gemfire.cache.snapshot.SnapshotReader;
import com.gemstone.gemfire.pdx.PdxInstance;

public class App {

	public static <K, V> void main(String args[]) throws Exception {
		//clearRegion();
		//generateData(10);
		//queryData();
		//purge();
		//getVINs();
		
		//File mySnapshot = new File("/Users/lwang/Desktop/server1.gfd");
		
		//SnapshotIterator<K, V> iter = SnapshotReader.read(mySnapshot);
		
		//System.out.println(iter.hasNext());
//		ClientCache cache = new ClientCacheFactory()
//		.set("cache-xml-file", "config/clientCache.xml")
//		.set("log-level", "error").create();
//		
//		Region<Integer, PdxInstance> unitTelemetryRegion = cache
//				.getRegion("UnitTelemetry");
//		
//		unitTelemetryRegion.registerInterestRegex(".*");
//		
//		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
//	    bufferedReader.readLine();
		
	}
	
	@SuppressWarnings("unchecked")
	private static <K, V> void getVINs() {
		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<K, V> UTRegion = cache.getRegion("UnitTelemetry");

		Execution execution = FunctionService.onRegion(UTRegion);

        ResultCollector<?, ?> collector = execution.execute("UT-VINs-Function");
		
        Set<String> vinSet = new HashSet<String>();
        
		List<Set<String>> responseList = (List<Set<String>>) collector.getResult();
		
		for (Set<String> e : responseList) {
			vinSet.addAll(e);
		}
		
		System.out.println(vinSet);
	}
	
	private static <K, V> void purge() {
		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<String, Set<K>> UTPurgeHelperRegion = cache
				.getRegion("UTPurgeHelper");
		Region<K, V> UTRegion = cache
				.getRegion("UnitTelemetry");

		
		Map<String, TreeSet<Date>> vinMap = new HashMap<String, TreeSet<Date>>();
		
		// Build VIN-Date HashMap
		for (String key : UTPurgeHelperRegion.keySetOnServer()) {
			//Parse key into VIN and date
			String newVin = key.split("\\|")[0];
			String newDateString = key.split("\\|")[1];
			
			try {
				Date newDate = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(newDateString);
				
				
				TreeSet<Date> dateSet = vinMap.containsKey(newVin) ? vinMap.get(newVin) : new TreeSet<Date>();
				dateSet.add(newDate);
				vinMap.put(newVin, dateSet);
	
			} catch (ParseException e) {
				e.printStackTrace();
			}		
		}
		
		// Purge date for each VIN
		for (String vin : vinMap.keySet()) {
			TreeSet<Date> dateSet = vinMap.get(vin);
			while (dateSet.size() > 3) {
				// Get oldest date from TreeSet
				Date oldDate = dateSet.pollFirst();
				
				// Convert Date Object back to String
				String oldDateString = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).format(oldDate);
				
				// Rebuild UTPurgeHelper Key
				String purgeKey = vin+'|'+oldDateString;
				
				// drop keys and remove entry in UTPurgeHelper
				UTRegion.removeAll(UTPurgeHelperRegion.get(purgeKey));
				UTPurgeHelperRegion.remove(purgeKey);
			}
		}
	}

	private static <K, V> void clearRegion() {
		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion = cache
				.getRegion("UTPurgeHelper");
		Region<Integer, PdxInstance> unitTelemetryRegion = cache
				.getRegion("UnitTelemetry");

		unitTelemetryHelperRegion.removeAll(unitTelemetryHelperRegion
				.keySetOnServer());
		unitTelemetryRegion.removeAll(unitTelemetryRegion.keySetOnServer());
	}

	private static <K, V> void generateData(int num) {
		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<Integer, PdxInstance> unitTelemetryRegion = cache
				.getRegion("UnitTelemetry");

		Random random = new Random();

		System.out.println("Putting Data...");
		long tStart = System.currentTimeMillis();

		for (int i = 0; i < num; i++) {
			String time = "2015-11-1" + random.nextInt(10) + "T"
					+ random.nextInt(24) + ":" + random.nextInt(60) + ":"
					+ random.nextInt(60) + ":" + random.nextInt(1000) + "Z";
			PdxInstance newItem = cache
					.createPdxInstanceFactory("io.pivotal.Json")
					.writeString("vin", "1FVACWDT39HAJ377" + random.nextInt(10))
					.writeString("capture_datetime", time).create();

			unitTelemetryRegion.put(newItem.hashCode(), newItem);
		}

		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStart;
		double elapsedSeconds = tDelta / 1000.0;
		System.out.println("Finished Putting Data. Time elapsed: "
				+ elapsedSeconds);
	}

	private static <K, V> void purgeData() {

		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion = cache
				.getRegion("UTPurgeHelper");
		Region<K, V> unitTelemetryRegion = cache.getRegion("UnitTelemetry");

		// For each VIN, purge the old data
		for (String vin : unitTelemetryHelperRegion.keySetOnServer()) {

			// Get the Date Map for this VIN
			Map<Date, Set<K>> dateMap = unitTelemetryHelperRegion.get(vin);

			// Convert to TreeSet
			TreeSet<Date> dates = new TreeSet<Date>(dateMap.keySet());

			// If map has more than 3 dates, need to remove old keys
			while (dateMap.entrySet().size() > 3) {

				// Remove the earliest date, and get all the keys in it
				Set<K> keys = dateMap.remove(dates.pollFirst());

				// Remove above keys in UnitTelemetry
				unitTelemetryRegion.removeAll(keys);
			}

			// Update dateMap for this VIN in UnitTelemetryHelper
			unitTelemetryHelperRegion.put(vin, dateMap);
		}
	}

	@SuppressWarnings({ "unused", "unchecked" })
	private static <K, V> void queryData() throws Exception {

		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion = cache
				.getRegion("UTPurgeHelper");
		Region<K, V> unitTelemetryRegion = cache.getRegion("UnitTelemetry");

		QueryService queryService = cache.getQueryService();

		Query query = queryService
				.newQuery("SELECT * FROM /UTPurgeHelper.values()");

		Object result = query.execute();

		Collection<?> collection = ((SelectResults<?>) result).asList();

		Iterator<?> iter = collection.iterator();

		int sumAll = 0;
		while (iter.hasNext()) {
			Set<K> map = (Set<K>) iter.next();

			sumAll += map.size();

			//System.out.println("Total keys in UTPurgeHelper : " + sum + "\n");
		}

		System.out.println("Total keys in UTPurgeHelper : " + sumAll + "\n");
		
		Query query2 = queryService
				.newQuery("SELECT count(*) FROM /UnitTelemetry");

		Object result2 = query2.execute();

		Collection<?> collection2 = ((SelectResults<?>) result2).asList();

		Iterator<?> iter2 = collection2.iterator();

		while (iter2.hasNext()) {
			System.out.println("Total records in UnitTelemetry: "
					+ iter2.next());
		}

	}
}
