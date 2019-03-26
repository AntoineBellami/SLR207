import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/*
 * This class is a thread which deals with the standard input stream of a process
 */
public class StandardStreamThread extends Thread {
	
	/*
	 * This boolean determines which treatment to apply to the content read
	 * in the stream
	 */
	private boolean timeoutThing = false;
	
	private Process p;
	private ArrayList<String> machinesDeployed = new ArrayList<String>();
	
	private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
	
	public StandardStreamThread(boolean timeoutThing, Process p, LinkedBlockingQueue<String> queue) {
		super();
		this.timeoutThing = timeoutThing;
		this.p = p;
		this.queue = queue;
	}

	public StandardStreamThread(boolean timeoutThing, Process p, ArrayList<String> machinesDeployed) {
		super();
		this.timeoutThing = timeoutThing;
		this.p = p;
		this.machinesDeployed = machinesDeployed;
	}

	public void run() {
		BufferedReader inputBr = new BufferedReader(
    			new InputStreamReader(
    			p.getInputStream()));
		
		String inLine;
	    try {
			while((inLine = inputBr.readLine()) != null) {
				/*
				 * Use a linked blocked queue to set a timeout for the
				 * distant execution of slave.jar
				 */
				if (timeoutThing) queue.put(inLine);
				/*
				 * Set a list of machines on which the code is
				 * efficiently deployed
				 */
				else printDeployed(inLine);
			    }
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void printDeployed(String inLine) {
		machinesDeployed.add(inLine);
		System.out.println("Connection succeeded for machine " + inLine);
	}
	
}