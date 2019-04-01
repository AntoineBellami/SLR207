import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SplitsDeployer {
	
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
			int splitIndex = subMachines.indexOf(machine);
			/*
			 * Create splits directory and send file
			 */
			Process p = null;
			try {
				p = new ProcessBuilder("ssh", "abellami@" + machine, "mkdir", "-p",
						"/cal/homes/abellami/tmp/abellami/splits/").start();
				p.waitFor();
				p = new ProcessBuilder("scp", "/cal/homes/abellami/tmp/abellami/splits/S"
						+ Integer.toString(splitIndex) +".txt",
						"abellami@" + machine + ":/cal/homes/abellami/tmp/abellami/splits").start();
				p.waitFor();
				
				System.out.println("Machine " + machine + " Received split " + Integer.toString(splitIndex));
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		
		return true;
		
	}

}