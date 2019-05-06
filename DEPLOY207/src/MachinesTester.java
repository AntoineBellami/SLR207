import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MachinesTester {
	
	final int timeout = 2; // timeout in seconds
	
	private ArrayList<String> machines = new ArrayList<String>();
	private ArrayList<String> machinesDeployed = new ArrayList<String>();

	public MachinesTester() {
		super();
	}

	public ArrayList<String> getMachinesDeployed() {
		return machinesDeployed;
	}

	public void checkMachines() {
		/*
		 * Read machines names from 'machines.txt' and store them
		 * in an ArrayList
		 */
		FileReader fr = null;
		
		try {
			fr = new FileReader("machines.txt");
			BufferedReader br = new BufferedReader(fr) ;
			Scanner        sc = new Scanner(br) ;

			String machine = null;
			while(sc.hasNext()) {
				machine = sc.nextLine();
				machines.add(machine);				
			}
			sc.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {fr.close();} catch(Exception e) {e.printStackTrace();}
		}
		
		/*
		 * Simultaneously launch for every machine a thread which checks the accessibility
		 * via SSH of the machine by executing the command 'hostname', with a timeout
		 * delay defined at the beginning of this file
		 */		
		machines.parallelStream().forEach(machine -> {
			
			/*
    		 * Execute the command 'hostname' on the studied machine
    		 */
    		Process p = null;
			try {
				p = new ProcessBuilder("ssh", "abellami@" + machine, "hostname").start();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
			String result = null;
			LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
    		
			/*
			 * Intercept the standard input stream of the process builder
			 */
    		BufferedReader inputBr = new BufferedReader(
            		new InputStreamReader(
            		p.getInputStream()));
    		
    		Thread t = new Thread() {
    		    public void run() {
    		    	try {
    		    		String inLine = null;
						while((inLine = inputBr.readLine()) != null) {
							queue.put(inLine);
						    }
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
    		    }
    		};
    		t.setDaemon(true);
    		t.start();
		    
		    /*
		     * Wait for at most 'timeout' seconds the output of the command ;
		     * if the machine is not reachable within this time,
		     * the connection is considered to have failed
		     */
		    try {
				result = (String) queue.poll(timeout, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if (machine.equals(result)) {
				result = machine;
				System.out.println(result);
				this.machinesDeployed.add(machine);
				
				try {
					/*
					* Clean the /tmp/abellami/ directory for a fresh start
					*/
					p = new ProcessBuilder("ssh", "abellami@" + machine, "rm", "-rf",
							"/tmp/abellami/").start();
					p.waitFor();
					p = new ProcessBuilder("ssh", "abellami@" + machine, "mkdir", "-p",
							"/tmp/abellami/").start();
					p.waitFor();
				} catch (IOException e1) {
					e1.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else {
				result = "Connection failed for machine " + machine;
				System.err.println(result);
			}
		    t.interrupt();
		});
	}

	public void deploySlave() {
		/*
		* Simultaneously deploy on every reached machine the slave.jar program
		*/
		machinesDeployed.parallelStream().forEach(machine -> {
	
			Process p = null;
			try {
				/*
				* Create /tmp/abellami/ directory and deploy slave.jar in it
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
	
}