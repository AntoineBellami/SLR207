import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

public class Main {

	public static void main(String[] args) {
		
		int mode = Integer.parseInt(args[0]);
		
		/*
		 * Mode 0 corresponds to the map step
		 */
		if (mode == 0) {
			String splitNumber = args[1];
			String pathToSplit = "/tmp/abellami/splits/S" + splitNumber + ".txt";
			
			HashMap<String, Integer> map = new HashMap<>();

			FileReader fr = null;
			try {
				fr = new FileReader(pathToSplit);
				BufferedReader br = new BufferedReader(fr) ;
				@SuppressWarnings("resource")
				Scanner        sc = new Scanner(br) ;
				
				String word = null;
				while(sc.hasNext()) {
					word = sc.next();

					if (map.containsKey(word))  
					{
						int value = map.get(word);
						map.put(word, value + 1);
					}
					else {
						map.put(word, 1);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} finally {
				try {
					
					System.out.println("Creating map file...");
					
					// Create maps directory
					Process p = null;
					try {
						p = new ProcessBuilder("mkdir", "-p", "/tmp/abellami/maps/").start();
						p.waitFor();
					} catch (IOException e1) {
						e1.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					// Create UM file
					FileWriter fstream;
				    BufferedWriter out;

				    // Create filewriter and bufferedreader
				    fstream = new FileWriter("/tmp/abellami/maps/UM" + splitNumber + ".txt");
				    out = new BufferedWriter(fstream);

				    // create Iterator for the map
				    Iterator<Entry<String, Integer>> it = map.entrySet().iterator();

				    // Use the iterator to loop through the map, stopping when we reach the
				    while (it.hasNext()) {

				        // the key/value pair is stored here in pairs
				        Map.Entry<String, Integer> pairs = it.next();

				        // since you only want the value, we only care about pairs.getValue(), which is written to out
				        out.write(pairs.getKey() + " " + pairs.getValue() + "\n");
				    }
				    // lastly, close the file and end
				    out.close();
				    fstream.close();
				    
				    System.out.println("Map file created");
					
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}

	}

}
