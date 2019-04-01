import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {

	public static void main(String[] args) {
		
		/*
		 * This boolean determines whether the slave.jar file is launched after
		 * the search for reachable machines 
		 */
		final boolean launchSlave = true;
		
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
		
		if (launchSlave) {
			
			/*
			 * Simultaneously launch for every machine the code previously deployed
			 */
			final int timeout = 5;
			
			machinesDeployed.parallelStream().forEach(machine -> {
		
				try {
					/*
					 * Execute slave.jar on the reachable machines thanks to a process builder,
					 * with a timeout (defined above) enabled by the use of a LinkedBlockedQueue
					 */
					Process p2 = new ProcessBuilder("ssh", "abellami@" + machine, "java", "-jar",
							"/cal/homes/abellami/tmp/abellami/slave.jar").start();
					
					LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
				    
					/*
					 * This thread intercepts the standard input of the process builder
					 * and stores it the LinkedBlockedQueue queue 
					 */
					StandardStreamThread it = new StandardStreamThread(true, p2, queue);
				    
				    /*
					 * This thread intercepts the error input of the process builder
					 * and stores it the LinkedBlockedQueue queue 
					 */
					ErrorStreamThread et = new ErrorStreamThread(true, p2, queue);
				    
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
					p2.destroy();
					
					System.out.println("TIMEOUT");
					
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
		}
	}
}