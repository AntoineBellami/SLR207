import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {

	public static void main(String[] args) {
		
		/*
		 * Read machines names from 'machines.txt' and store them
		 * in an ArrayList
		 */
		ArrayList<String> machines = new ArrayList<String>(); 
		FileReader fr = null;
		
		try {
			fr = new FileReader("machines.txt");
			BufferedReader br = new BufferedReader(fr) ;
			Scanner        sc = new Scanner(br) ;

			System.out.println("Reading machines names...");
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
		 * delay defined below
		 */
		final int timeout = 10;
		
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
		     * Wait for at most 10 seconds the output of the command ;
		     * if the machine is not reachable during those 10 seconds,
		     * the connection is considered to have failed
		     */
		    try {
				result = (String) queue.poll(timeout, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if (machine.equals(result)) {
				result = "Connection succeeded for machine " + machine;
			}
			else {
				result = "Connection failed for machine " + machine;
			}
		    System.out.println(result);
		    t.interrupt();
		});
		
	}
}
