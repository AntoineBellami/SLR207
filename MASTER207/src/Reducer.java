import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Iterator;

public class Reducer {
    private ArrayList<String> machinesDeployed = new ArrayList<String>();
	private HashMap<String, String> UMx_machines_dict = new HashMap<String, String>();
    private HashMap<String, List<String>> keys_machines_dict = new HashMap<String, List<String>>();
    private HashMap<String, Set<String>> shufflers_keys_dict = new HashMap<String, Set<String>>();
    
    public HashMap<String, String> getUMx_machines_dict() {
		return UMx_machines_dict;
	}
	public HashMap<String, List<String>> getKeys_machines_dict() {
		return keys_machines_dict;
	}

    public Reducer(ArrayList<String> machinesDeployed, HashMap<String, String> UMx_machines_dict, HashMap<String, List<String>> keys_machines_dict) {
		this.machinesDeployed = machinesDeployed;
        this.UMx_machines_dict = UMx_machines_dict;
        this.keys_machines_dict = keys_machines_dict;
        for (String machine: machinesDeployed) {
            shufflers_keys_dict.put(machine, new TreeSet<String>());
        }
    }
    
    public void prepare() {
        // Iterator on machines
        Iterator machinesIterator = machinesDeployed.iterator();
        Set<String> keys = keys_machines_dict.keySet();
        
        // Assign each key to a machine
        for (String key: keys) {
            shufflers_keys_dict.get(machinesIterator.next()).add(key);
        }
        System.out.println(shufflers_keys_dict);
    }
}