import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.List;

public class Main {
	public static void main(String[] args) {
		
		boolean localExecution = false;
		try {
			localExecution = System.getenv().get("HOSTNAME").equals("localhost.localdomain");
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*
		* Clean the splits/ directory
		*/
		Process pr = null;
        try {
			pr = new ProcessBuilder("rm", "-rf", "splits/").start();
            pr.waitFor();
            pr = new ProcessBuilder("mkdir", "-p", "splits/").start();
            pr.waitFor();
        } catch (Exception e) {}

		/*
		* Cut an input file into smaller splits.
		*/
		final int max_lines = 10;
		FileReader fr = null;
		BufferedWriter out = null;
		try {
			// Local execution
			if (localExecution) {
				fr = new FileReader("resources/input.txt");
			}
			// Distant execution
			else {
				fr = new FileReader("/cal/homes/abellami/tmp/abellami/resources/forestier_mayotte.txt");
			}
			BufferedReader br = new BufferedReader(fr) ;
			Scanner        sc = new Scanner(br) ;
			/*
			* Fix a max file size (in terms of lines number) for each split.
			* Increment the split index
			*/
			int splitIndex = 0;
			int lines_written = 0;
			while (sc.hasNextLine()) {
				FileWriter fstream = null;
				try {
					fstream = new FileWriter("splits/S" + Integer.toString(splitIndex) + ".txt");
					splitIndex ++;
					out = new BufferedWriter(fstream);
					lines_written = 0;
					while (sc.hasNextLine() && lines_written < max_lines) {
						out.write(sc.nextLine() + "\n");
						lines_written ++;
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally { out.close(); }
			}


		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try { fr.close(); } catch (Exception e) {}
		}
		
		/*
		 * This boolean determines whether the split files are deployed after
		 * the search for reachable machines 
		 */
		final boolean deploySplits = true;
		
		/*
		 * Read deployed machines names by calling deploy.jar and store them
		 * in an ArrayList
		 */
		ArrayList<String> machinesDeployed = new ArrayList<String>();
		
		/*
		 * Dictionary mapping the names of the UMx files with the names of
		 * the machines containing them
		 */
		HashMap<String, String> UMx_machines_dict = new HashMap<String, String>();
		HashMap<String, List<String>> keys_UMx_dict = new HashMap<String, List<String>>();
		
		System.out.println("Seek for deployed machines:" + "\n");
		
		try {
			Process p = new ProcessBuilder("java", "-jar", "deploy.jar").start();
			
			/*
			 * This thread intercepts the standard input of the process builder
			 * and stores it the machinesDeployed array
			 */
		    StandardStreamThread it = new StandardStreamThread(false, p, machinesDeployed);
		    
		    /*
			 * This thread intercepts the error input of the process builder
			 * and displays it in the error output stream
			 */
			ErrorStreamThread et = new ErrorStreamThread(false, p);
		    
		    it.start();
			et.start();
			
			/*
			 * Wait for the end of the execution of deploy.jar
			 */
			p.waitFor();
			System.out.println("\n" + "Deployed");
			
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		if (deploySplits) {
			System.out.println("Begin of the map phase\n");
			long startMapTime = System.currentTimeMillis();

			SplitsDeployer splitsDeployer = new SplitsDeployer(machinesDeployed, "splits/");
			splitsDeployer.deploy();

			long endMapTime   = System.currentTimeMillis();
			System.out.println("Mapping time: " + (endMapTime - startMapTime) + " ms\n");
			
			UMx_machines_dict = new HashMap<String, String>(splitsDeployer.getUMx_machines_dict());
			keys_UMx_dict = new HashMap<String, List<String>>(splitsDeployer.getKeys_UMx_dict());
			System.out.println("UMx-machines: " + UMx_machines_dict);
			System.out.println("Keys-UMx: " + keys_UMx_dict);

			System.out.println("Begin of the shuffle phase\n");
			long startShuffleTime = System.currentTimeMillis();

			Reducer reducer = new Reducer(machinesDeployed, UMx_machines_dict, keys_UMx_dict);
			reducer.reduce();

			long endReduceTime   = System.currentTimeMillis();
			System.out.println("Shuffle-reduce time: " + (endReduceTime - startShuffleTime) + " ms\n");

			reducer.displayResult();
		}
	}
}