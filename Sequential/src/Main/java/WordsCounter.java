import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.stream.Collectors;

public class WordsCounter {

	public static void main(String[] args) {

		HashMap<String, Integer> map = new HashMap<>();

		FileReader fr = null;
		try {
			fr = new FileReader("../../resources/internet.wet");
			BufferedReader br = new BufferedReader(fr) ;
			Scanner        sc = new Scanner(br) ;

			System.out.println("Counting word...");
			long startTime = System.currentTimeMillis();
			String word = null;
			while(sc.hasNext()) {
				word = sc.next().toLowerCase();

				if (map.containsKey(word))  
				{
					int value = map.get(word);
					map.put(word, value + 1);
				}
				else {
					map.put(word, 1);
				}
			}
			long endTime   = System.currentTimeMillis();
			System.out.println("Counting time: " + (endTime - startTime) + " ms \n");	

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Sorting...");
			long startTime = System.currentTimeMillis();

			//Sort by keys
			HashMap<String, Integer> sortedByKeyMap = map.entrySet().stream()
					.sorted(Entry.comparingByKey())
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue,
							(e1, e2) -> e1, LinkedHashMap::new));

			// Sort by values
			HashMap<String, Integer> sortedMap = sortedByKeyMap.entrySet().stream()
					.sorted(Entry.comparingByValue(Comparator.reverseOrder()))
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue,
							(e1, e2) -> e1, LinkedHashMap::new));
			long endTime   = System.currentTimeMillis();
			System.out.println("Sorting time: " + (endTime - startTime) + " ms \n");

			Iterator it = sortedMap.entrySet().iterator();

			int wordsPrinted = 0;
			while (it.hasNext() & wordsPrinted < 100) {
				Map.Entry mapElement = (Map.Entry)it.next();
				String key = (String)mapElement.getKey();
				int value = (int)mapElement.getValue(); 
				System.out.println(key + " " + value);
				wordsPrinted ++;
			}

			try {fr.close();} catch(Exception e) {e.printStackTrace();}
		}
	}
}
