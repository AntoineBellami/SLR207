import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {

	public static void main(String[] args) {
		
		final int timeout = 15; // Timeout in seconds
	
		try {
			Process p = new ProcessBuilder("java", "-jar", "/tmp/abellami/slave.jar").start();
			// Process p = new ProcessBuilder("ls", "-al", "/tmp").start();

			LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
		    
		    Thread inputThread = new Thread() {
		    	public void run() {
		    		BufferedReader inputBr = new BufferedReader(
	            			new InputStreamReader(
	            			p.getInputStream()));
		    		
		    		String inLine;
				    try {
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
		    
		    
			Thread errorThread = new Thread() {
		    	public void run() {
		    		BufferedReader errorBr= new BufferedReader(
		        			new InputStreamReader(
		        			p.getErrorStream()));
		    		
		    		String errLine;
					try {
						while((errLine = errorBr.readLine()) != null) {
							queue.put(errLine);
						    }
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
		    	}
		    };
		    
		    inputThread.start();
			errorThread.start();
			
			String nextLine = (String) queue.poll(timeout, TimeUnit.SECONDS);

			while(nextLine != null) {
				System.out.println(nextLine);
				nextLine = (String) queue.poll(timeout, TimeUnit.SECONDS);
			}

			inputThread.interrupt();
			errorThread.interrupt();
			p.destroy();
			
			System.out.println("TIMEOUT");
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
