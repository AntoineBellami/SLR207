import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SplitsDeployer {
	
	final int timeout = 2; // timeout in seconds
	
	private String username;
	/*
	 * List of machines on which deploy.jar is deployed
	 */
	private ArrayList<String> machinesDeployed = new ArrayList<String>();
	private String splitsPath = null;
	/*
	 * Dictionary mapping the names of the UMx files with the names of
	 * the machines on which they are stored
	 */
	private ConcurrentHashMap<String, String> UMx_machines_dict = new ConcurrentHashMap<String, String>();
	/*
	 * Dictionary mapping the keys (words contained in input file) with the unsorted
	 * maps containing them. Concurrency is necessary because UMx_machines_dict is
	 * modified in parallel streams
	 */
	private ConcurrentHashMap<String, List<String>> keys_UMx_dict = new ConcurrentHashMap<String, List<String>>();
	
	public ConcurrentHashMap<String, String> getUMx_machines_dict() {
		return UMx_machines_dict;
	}

	public ConcurrentHashMap<String, List<String>> getKeys_UMx_dict() {
		return keys_UMx_dict;
	}

	public SplitsDeployer(String username, ArrayList<String> machinesDeployed, String splitsPath) {
		this.username = username;
		this.machinesDeployed = machinesDeployed;
		this.splitsPath = splitsPath;
	}
	
	public void deploy() {
		/*
		 * Dictionary storing for each machines the set of splits hosted
		 */
		HashMap<String, Set<String>> machines_splits_dict = new HashMap<String, Set<String>>();
		for (String machine: machinesDeployed) {
            machines_splits_dict.put(machine, new TreeSet<String>());
        }

		/*
		* Iterator on machines
		*/
		Iterator machinesIterator = machinesDeployed.iterator();
		String[] splits = new File(splitsPath).list();
        
		/*
		 * Assign each split to a machine using an iterator on the list of deployed machines
		 * If the number of splits is higher than the number of working machines, attribute
		 * a split to each machines until all machines posess the same number of splits
		 */
        for (String split: splits) {
			if (machinesIterator.hasNext()) {
				machines_splits_dict.get(machinesIterator.next()).add(split);
			}
			else {
				machinesIterator = machinesDeployed.iterator();
				machines_splits_dict.get(machinesIterator.next()).add(split);
			}			
		}
		
		machinesDeployed.parallelStream().forEach(machine -> {

			/*
			 * Create /tmp/username/splits/ directory on machine
			 */
			Process p2 = null;
			try {
				p2 = new ProcessBuilder("ssh", username + "@" + machine, "mkdir", "-p",
						"/tmp/" + username + "/splits/").start();
				p2.waitFor();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			machines_splits_dict.get(machine).parallelStream().forEach(split -> {
				
				/*
				* deploy the convenient split file on the convenient machine
				*/
				Process p = null;
				try {
					p = new ProcessBuilder("scp", "splits/" + split,
										   username + "@" + machine + ":/tmp/"
										   + username + "/splits").start();
					p.waitFor();
					
					String stringIndex = split.substring(1, 2);
					int splitIndex = Integer.parseInt(stringIndex);
					System.out.println("Machine " + machine + " Received split " + splitIndex);
					
					/*
					 * Execute slave.jar on the reachable machines thanks to a process builder,
					 * with a timeout (defined above) enabled by the use of a LinkedBlockedQueue.
					 * The operation succeeds if nothing is read in the error stream
					 */
					p = new ProcessBuilder("ssh", username + "@" + machine, "java", "-jar",
										   "/tmp/" + username + "/slave.jar", username,
										   "0", Integer.toString(splitIndex)).start();
					
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
						 * Add the new word to the [keys]-[Umx lists] dictionary
						 */
						if (keys_UMx_dict.containsKey(nextLine)) {
							/*
							 * If the key is already contained in another UM, copy the previous machines
							 * list and update it with the new UMx file which contains the key
							 */
							List<String> new_UMx_list = new ArrayList<>(keys_UMx_dict.get(nextLine));
							new_UMx_list.add("UM" + stringIndex);
							keys_UMx_dict.replace(nextLine, new_UMx_list);
						}
						else {
							keys_UMx_dict.put(nextLine, Arrays.asList("UM" + stringIndex));
						}
						
						nextLine = (String) queue.poll(timeout, TimeUnit.SECONDS);
					}

					it.interrupt();
					et.interrupt();
					p.destroy();
					
					UMx_machines_dict.put("UM"+ stringIndex, machine);
					
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			});

		});
		
	}

}