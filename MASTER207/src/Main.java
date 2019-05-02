import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {
	public static void main(String[] args) {
		
		/*
		 * This boolean determines whether the slave.jar file is deployed after
		 * the search for reachable machines
		 */
		final boolean deploySlave = true;
		
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
		HashMap<String, List<String>> keys_machines_dict = new HashMap<String, List<String>>();
		
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
		
		if (deploySlave) {
			
			/*
			 * Simultaneously deploy for every machine the SLAVE
			 */			
			machinesDeployed.parallelStream().forEach(machine -> {
		
				Process p = null;
				try {
					/*
					 * Create /tmp/abellami/ directory and deploy slave in it
					 */
					p = new ProcessBuilder("ssh", "abellami@" + machine, "mkdir", "-p",
							"/tmp/abellami/splits/").start();
					p.waitFor();
					
					p = new ProcessBuilder("scp", "slave.jar", "abellami@" + machine
							+ ":/tmp/abellami").start();
					
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			
			});
			
		}
		
		if (deploySplits) {
			SplitsDeployer splitsDeployer = new SplitsDeployer(machinesDeployed, "splits/");
			splitsDeployer.deploy();
			
			UMx_machines_dict = new HashMap<String, String>(splitsDeployer.getUMx_machines_dict());
			System.out.println(UMx_machines_dict);
			keys_machines_dict = new HashMap<String, List<String>>(splitsDeployer.getKeys_machines_dict());
			System.out.println(keys_machines_dict);
			
			System.out.println("End of the map phase");

			// Reducer reducer = new Reducer(machinesDeployed, UMx_machines_dict, keys_machines_dict);
			// reducer.prepare();
		}
	}
}