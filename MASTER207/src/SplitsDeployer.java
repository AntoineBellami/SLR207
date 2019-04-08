import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SplitsDeployer {
	
	final int timeout = 5; // timeout in seconds
	
	private ArrayList<String> machinesDeployed = new ArrayList<String>();
	private String splitsPath = null;
	private ConcurrentHashMap<String, String> UMx_machines_dict = new ConcurrentHashMap<String, String>();
	private ConcurrentHashMap<String, List<String>> keys_machines_dict = new ConcurrentHashMap<String, List<String>>();
	
	public ConcurrentHashMap<String, String> getUMx_machines_dict() {
		return UMx_machines_dict;
	}

	public ConcurrentHashMap<String, List<String>> getKeys_machines_dict() {
		return keys_machines_dict;
	}

	public SplitsDeployer(ArrayList<String> machinesDeployed, String splitsPath) {
		this.machinesDeployed = machinesDeployed;
		this.splitsPath = splitsPath;
	}
	
	/*
	 * Be careful success or failure
	 */
	public void deploy() {
		/*
		 * Count the number n of splits in splitsPath
		 */
		int splitsNumber = new File(splitsPath).list().length;
		/*
		 * Select the first n machines and deploy
		 */
		
		if (machinesDeployed.size() < splitsNumber) {
			System.err.println("Splits number lower than the number of machines deployed");
		}
		
		List<String> subMachines = machinesDeployed.subList(0, splitsNumber);
		
		subMachines.parallelStream().forEach(machine -> {
			
			/*
    		 * Get index of split to deal with
    		 */
			String splitIndex = Integer.toString(subMachines.indexOf(machine));
			
			/*
			 * Create splits directory and send file
			 */					
			Process p = null;
			try {
				/*
				 * Create /tmp/abellami/splits/ directory and deploy the convenient
				 * split file in it
				 */
				p = new ProcessBuilder("ssh", "abellami@" + machine, "mkdir", "-p",
						"/tmp/abellami/splits/").start();
				p.waitFor();
				
				p = new ProcessBuilder("scp", "splits/S" + splitIndex +".txt",
						"abellami@" + machine + ":/tmp/abellami/splits").start();
				p.waitFor();
				
				System.out.println("Machine " + machine + " Received split " + splitIndex);
				
				/*
				 * Execute slave.jar on the reachable machines thanks to a process builder,
				 * with a timeout (defined above) enabled by the use of a LinkedBlockedQueue.
				 * The operation succeeds if nothing is read in the error stream
				 */
				p = new ProcessBuilder("ssh", "abellami@" + machine, "java", "-jar",
						"/tmp/abellami/slave.jar", "0", splitIndex).start();
				
				LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
				
				/*
				 * This thread intercepts the standard input of the process builder
				 * and stores it the LinkedBlockedQueue queue 
				 */
				StandardStreamThread it = new StandardStreamThread(true, p, queue);
			    
			    /*
				 * This thread intercepts the error input of the process builder
				 * and stores it the LinkedBlockedQueue queue 
				 */
				ErrorStreamThread et = new ErrorStreamThread(true, p);
			    
			    it.start();
				et.start();
				
				/*
				 * Print any output of the execution of slave.jar until the timeout
				 * is reached (timeout gets reinitialized after every new output)
				 */
				String nextLine = (String) queue.poll(timeout, TimeUnit.SECONDS);
				while(nextLine != null) {
					
					/*
					 * Add the new word to the keys-Umx lists dictionary
					 */
					if (keys_machines_dict.containsKey(nextLine)) {
						/*
						 * Copy the previous machines list and update it with the new machine which
						 * contains the key
						 */
						List<String> new_machines_list = new ArrayList<>(keys_machines_dict.get(nextLine));
						new_machines_list.add(machine);
						keys_machines_dict.replace(nextLine, new_machines_list);
					}
					else {
						keys_machines_dict.put(nextLine, Arrays.asList(machine));
					}
					
					nextLine = (String) queue.poll(timeout, TimeUnit.SECONDS);
				}

				it.interrupt();
				et.interrupt();
				p.destroy();
				
				UMx_machines_dict.put("UM"+splitIndex.toString(), machine);
				System.out.println("Slave execution TIMEOUT");
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		
	}

}