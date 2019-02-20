import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {

	public static void main(String[] args) {
	
		try {
			// Process p = new ProcessBuilder("java", "-jar", "/tmp/abellami/slave.jar").start();
			Process p = new ProcessBuilder("ls", "-al", "/tmp").start();
		    
		    Thread inputThread = new Thread() {
		    	public void run() {
		    		BufferedReader inputBr = new BufferedReader(
	            			new InputStreamReader(
	            			p.getInputStream()));
		    		
		    		String inLine;
				    try {
						while((inLine = inputBr.readLine()) != null) {
							System.out.println(inLine);
						    }
					} catch (IOException e) {
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
							System.out.println(errLine);
						    }
					} catch (IOException e) {
						e.printStackTrace();
					}
		    	}
		    };
		    
		    inputThread.start();
		    errorThread.start();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
