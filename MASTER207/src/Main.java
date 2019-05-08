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

		/*
		 * Read the configuration data (cf. README)
		 */
		FileReader fr = null;
		String username = null;
		String input = null;
		int max_lines = 0;
		try {
			fr = new FileReader("hadoop.conf");
			
			BufferedReader br = new BufferedReader(fr);
			Scanner        sc = new Scanner(br);

			sc.next();
			username = sc.next();
			sc.next();
			input = sc.next();
			sc.next();
			max_lines = sc.nextInt();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {fr.close();} catch(Exception e) {e.printStackTrace();}
		}

		/*
		* Clean the ./splits/ directory of the machine executing MASTER
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
		 * The max_lines parameter determines the maximum size of a single plit file:
		 * new split files are created and filled with a maximum of max_lines lines
		 * until the whole input file is scanned.
		 */
		BufferedWriter out = null;
		try {
			fr = new FileReader("/cal/homes/"+username+"/tmp/"+username+"/resources/"+input);
			BufferedReader br = new BufferedReader(fr) ;
			Scanner        sc = new Scanner(br) ;

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
						out.write(sc.nextLine().replace(",", "").replace(".", "").toLowerCase() + "\n");
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
		 * Read deployed machines names by calling deploy.jar and store them
		 * in an ArrayList
		 */
		ArrayList<String> machinesDeployed = new ArrayList<String>();
		
		/*
		 * Dictionary mapping the names of the UMx files with the names of
		 * the machines on which they are stored
		 */
		HashMap<String, String> UMx_machines_dict = new HashMap<String, String>();
		/*
		 * Dictionary mapping the keys (words contained in input file) with the unsorted
		 * maps containing them
		 */
		HashMap<String, List<String>> keys_UMx_dict = new HashMap<String, List<String>>();
		
		System.out.println("Seek for deployed machines:" + "\n");
		
		try {
			/*
			 * deploy.jar WHAT DOES IT DO
			 */
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
			System.out.println("\n" + "Deployment completed");
			
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("\nBegin of the map phase");
		long startMapTime = System.currentTimeMillis();

		/*
		 * Distribute the splits and launch the mapping operation
		 */
		SplitsDeployer splitsDeployer = new SplitsDeployer(username, machinesDeployed, "splits/");
		splitsDeployer.deploy();

		long endMapTime   = System.currentTimeMillis();
		System.out.println("\nMapping time: " + (endMapTime - startMapTime) + " ms\n");
		
		UMx_machines_dict = new HashMap<String, String>(splitsDeployer.getUMx_machines_dict());
		keys_UMx_dict = new HashMap<String, List<String>>(splitsDeployer.getKeys_UMx_dict());
		System.out.println("\nUMx-machines: " + UMx_machines_dict);
		System.out.println("\nKeys-UMx: " + keys_UMx_dict);

		System.out.println("\nBegin of the shuffle phase\n");
		long startShuffleTime = System.currentTimeMillis();

		/*
		 * Manage the shuffle and reduce operations
		 */
		Reducer reducer = new Reducer(username, machinesDeployed, UMx_machines_dict, keys_UMx_dict);
		reducer.reduce();

		long endReduceTime   = System.currentTimeMillis();
		System.out.println("\nShuffle-reduce time: " + (endReduceTime - startShuffleTime) + " ms\n");

		/*
		 * Write the number of occurences of every word in the input file (not ordered)
		 * either in a file (./result.txt) if writeFile=true or in the standard output stream
		 */
		reducer.displayResult(true);
	}
}