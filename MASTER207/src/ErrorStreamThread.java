import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.LinkedBlockingQueue;

/*
 * This class is a thread which deals with the error input stream of a process
 */
public class ErrorStreamThread extends Thread {
	
	/*
	 * This boolean determines which treatment to apply to the content read in the stream:
	 * - if timeoutThing = true, the error stream should be managed with a LinkedBlockingQueue
	 * (for sensitive processes)
	 * - either way, the error input stream is just output in the error output stream
	 */
	private boolean timeoutThing = false;
	
	private Process p;
	
	private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
	
	public ErrorStreamThread(boolean timeoutThing, Process p, LinkedBlockingQueue<String> queue) {
		super();
		this.timeoutThing = timeoutThing;
		this.p = p;
		this.queue = queue;
	}

	public ErrorStreamThread(boolean timeoutThing, Process p) {
		super();
		this.timeoutThing = timeoutThing;
		this.p = p;
	}

	public void run() {
		BufferedReader errorBr= new BufferedReader(
    			new InputStreamReader(
    			p.getErrorStream()));
		
		String errLine;
		try {
			while((errLine = errorBr.readLine()) != null) {
				/*
				 * Use a linked blocked queue to set a timeout for the
				 * distant execution of slave.jar
				 */
				if (timeoutThing) queue.put(errLine);
				else System.err.println(errLine);
			    }
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}