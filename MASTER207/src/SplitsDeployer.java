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
		// Dictionary storing for each machines a set of hosted splits
		HashMap<String, Set<String>> machines_splits_dict = new HashMap<String, Set<String>>();
		for (String machine: machinesDeployed) {
            machines_splits_dict.put(machine, new TreeSet<String>());
        }

		// Iterator on machines
		Iterator machinesIterator = machinesDeployed.iterator();
		String[] splits = new File(splitsPath).list();
        
        // Assign each key to a machine
        for (String split: splits) {
			machines_splits_dict.get(machinesIterator.next()).add(split);
		}
		
		machinesDeployed.parallelStream().forEach(machine -> {

			/*
			* Create splits directory
			*/
			Process p2 = null;
			try {
				/*
				* Create /tmp/abellami/splits/ directory
				*/
				p2 = new ProcessBuilder("ssh", "abellami@" + machine, "mkdir", "-p",
						"/tmp/abellami/splits/").start();
				p2.waitFor();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			machines_splits_dict.get(machine).parallelStream().forEach(split -> {
				
				Process p = null;
				/*
				* deploy the convenient split file on the convenient machine
				*/					
				try {
					p = new ProcessBuilder("scp", "splits/" + split,
										   "abellami@" + machine + ":/tmp/abellami/splits").start();
					p.waitFor();
					
					int splitIndex = Integer.parseInt(split.substring(1, 2));
					System.out.println("Machine " + machine + " Received split " + splitIndex);
					
					/*
					* Execute slave.jar on the reachable machines thanks to a process builder,
					* with a timeout (defined above) enabled by the use of a LinkedBlockedQueue.
					* The operation succeeds if nothing is read in the error stream
					*/
					p = new ProcessBuilder("ssh", "abellami@" + machine, "java", "-jar", "/tmp/abellami/slave.jar",
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
					
					UMx_machines_dict.put("UM"+ Integer.toString(splitIndex), machine);
					System.out.println("Slave execution TIMEOUT");
					
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			});

		});
		
	}

}