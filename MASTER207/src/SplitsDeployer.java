import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SplitsDeployer {
	
	final int timeout = 5; // timeout in seconds
	
	ArrayList<String> machinesDeployed = new ArrayList<String>();
	private String splitsPath = null;

	public SplitsDeployer(ArrayList<String> machinesDeployed, String splitsPath) {
		this.machinesDeployed = machinesDeployed;
		this.splitsPath = splitsPath;
	}
	
	/*
	 * Be careful success or failure
	 */
	public boolean deploy() {
		/*
		 * Count the number n of splits in splitsPath
		 */
		int splitsNumber = new File(splitsPath).list().length;
		/*
		 * Select the first n machines and deploy
		 */
		
		if (machinesDeployed.size() < splitsNumber) {
			return false;
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
				 * with a timeout (defined above) enabled by the use of a LinkedBlockedQueue
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
				ErrorStreamThread et = new ErrorStreamThread(true, p, queue);
			    
			    it.start();
				et.start();
				
				/*
				 * Print any output of the execution of slave.jar until the timeout
				 * is reached (timeout gets reinitialized after every new output)
				 */
				String nextLine = (String) queue.poll(timeout, TimeUnit.SECONDS);
				while(nextLine != null) {
					System.out.println(nextLine);
					nextLine = (String) queue.poll(timeout, TimeUnit.SECONDS);
				}

				it.interrupt();
				et.interrupt();
				p.destroy();
				
				System.out.println("TIMEOUT");
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		
		return true;
		
	}

}