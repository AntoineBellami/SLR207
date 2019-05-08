import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;

import java.lang.ProcessBuilder.Redirect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.TreeSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.IntStream;
import java.util.stream.Collectors;
import java.util.List;

public class Reducer {
    private String username;
    /*
     * List of machines on which deploy.jar is deployed
     */
    private ArrayList<String> machinesDeployed = new ArrayList<String>();
    /*
     * Dictionary mapping the names of the UMx files with the names of
     * the machines on which they are stored
     */
    private HashMap<String, String> UMx_machines_dict = new HashMap<String, String>();
    /*
     * Dictionary mapping the keys (words contained in input file) with the unsorted
     * maps containing them
     */
    private HashMap<String, List<String>> keys_UMx_dict = new HashMap<String, List<String>>();
    /*
     * Dictionary mapping the machines to the keys they shuffle
     */
    private HashMap<String, Set<String>> shufflers_keys_dict = new HashMap<String, Set<String>>();
    /*
     * Dictionary mapping the names of the RMx files with the names of
     * the machines on which they are stored
     */
    private HashMap<String, String> RMx_machines_dict = new HashMap<String, String>();
    /*
     * This concurrent skip list set will be initialized with a range from 0 to the number
     * of keys minus one (which equals to the number of RMx files) in order to poll the
     * [number of keys] distinct 'x' numbers in 'RMx' file names.
     * Concurrency is necessary because files are named in parallel streams
     */
    private ConcurrentSkipListSet<Integer> ids = new ConcurrentSkipListSet();
    
    public HashMap<String, String> getUMx_machines_dict() {
		return UMx_machines_dict;
	}
	public HashMap<String, List<String>> getKeys_UMx_dict() {
		return keys_UMx_dict;
	}

    public Reducer(String username, ArrayList<String> machinesDeployed, HashMap<String,
                   String> UMx_machines_dict, HashMap<String, List<String>> keys_UMx_dict) {
        this.username = username;
		this.machinesDeployed = machinesDeployed;
        this.UMx_machines_dict = UMx_machines_dict;
        this.keys_UMx_dict = keys_UMx_dict;
        for (String machine: machinesDeployed) {
            shufflers_keys_dict.put(machine, new TreeSet<String>());
        }
        /*
        * Associate each key with a number from 0 to numberOfKeys-1
        */
        int nb_keys = keys_UMx_dict.keySet().size();
        List<Integer> range = IntStream.rangeClosed(0, nb_keys-1).boxed().collect(Collectors.toList());
        ids = new ConcurrentSkipListSet(range);
    }
    
    public void reduce() {
        /*
		 * Iterator on machines
		 */
        Iterator machinesIterator = machinesDeployed.iterator();
        Set<String> keys = keys_UMx_dict.keySet();
        
        /*
		 * Assign each split to a machine using an iterator on the list of deployed machines
		 * If the number of splits is higher than the number of working machines, attribute
		 * a split to each machines until all machines posess the same number of splits
		 */
        for (String key: keys) {
            if (machinesIterator.hasNext()) {
                shufflers_keys_dict.get(machinesIterator.next()).add(key);
            }
            else {
                machinesIterator = machinesDeployed.iterator();
                shufflers_keys_dict.get(machinesIterator.next()).add(key);
            }
        }

        System.out.println("Shufflers-keys: " + shufflers_keys_dict);

        shufflers_keys_dict.keySet().parallelStream().forEach(shuffle_machine -> {

            Process proc = null;
            try {
                /*
                * Create the maps/ and reduces/ directories on the shuffle machine
                */
                proc = new ProcessBuilder("ssh", username + "@" + shuffle_machine, "mkdir", "-p",
                    "/tmp/" + username + "/maps/").start();
                proc.waitFor();
                proc = new ProcessBuilder("ssh", username + "@" + shuffle_machine, "mkdir", "-p",
                    "/tmp/" + username + "/reduces/").start();
                proc.waitFor();
            } catch (Exception e) { e.printStackTrace(); }

            shufflers_keys_dict.get(shuffle_machine).parallelStream().forEach(key -> {

                keys_UMx_dict.get(key).parallelStream().forEach(UM -> {

                    String machine_sender = UMx_machines_dict.get(UM);

                    Process p = null;
                    try {
                        p = new ProcessBuilder("scp", username + "@" + machine_sender
                                               + ":/tmp/" + username + "/maps/" + UM + ".txt",
                                               username + "@" + shuffle_machine
                                               + ":/tmp/" + username + "/maps").start();
                        p.waitFor();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                
                /*
                 * Poll a number from 0 to [number of keys - 1]
                 */
                int id = ids.pollFirst();
                String SM = "SM" + Integer.toString(id);
                String RM = "RM" + Integer.toString(id);
                RMx_machines_dict.put(RM, shuffle_machine);

                /*
                * Shuffle maps
                */
                Process q = null;
                try {
                    List<String> command = new ArrayList<String>();
                    command.add("ssh");
                    command.add(username + "@" + shuffle_machine);
                    command.add("java");
                    command.add("-jar");
                    command.add("/tmp/" + username + "/slave.jar");
                    command.add(username);
                    command.add("1");
                    command.add("\"" + key.replace("\"", "\\\"").replace("\'", "\\\'") + "\"");
                    command.add("/tmp/" + username + "/maps/" + SM + ".txt");
                    for (String UM: keys_UMx_dict.get(key)) {
                        command.add("/tmp/" + username + "/maps/" + UM + ".txt");
                    }

                    q = new ProcessBuilder(command).start();
                    q.waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                /*
                * Reduce sorted maps
                */
                try {
                    List<String> command = new ArrayList<String>();
                    command.add("ssh");
                    command.add(username + "@" + shuffle_machine);
                    command.add("java");
                    command.add("-jar");
                    command.add("/tmp/" + username + "/slave.jar");
                    command.add(username);
                    command.add("2");
                    command.add("\"" + key.replace("\"", "\\\"").replace("\'", "\\\'") + "\"");
                    command.add("/tmp/" + username + "/maps/" + SM + ".txt");
                    command.add("/tmp/" + username + "/reduces/" + RM + ".txt");

                    q = new ProcessBuilder(command).start();
                    q.waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            });
        });

        System.out.println("RMx-machines: " + RMx_machines_dict);
    }

    public void displayResult(boolean writeFile) {
        Process p = null;
        try {
            p = new ProcessBuilder("mkdir", "-p", "/tmp/" + username + "/results/").start();
            p.waitFor();
        } catch (Exception e) {}

        FileWriter fstream = null;
        BufferedWriter out = null;
		try {
            fstream = new FileWriter("result.txt");
            out = new BufferedWriter(fstream);
            for (String RM: RMx_machines_dict.keySet()) {
                FileReader fr = null;
                try {
                    p = new ProcessBuilder("scp", RMx_machines_dict.get(RM) +
                                           ":/tmp/" + username + "/reduces/" + RM + ".txt",
                                           "/tmp/" + username + "/results").start();
                    p.waitFor();
                    fr = new FileReader("/tmp/" + username + "/results/" + RM + ".txt");
                    BufferedReader br = new BufferedReader(fr) ;
                    Scanner        sc = new Scanner(br) ;
                    
                    while (sc.hasNextLine()) {
                        if (writeFile) {
                            out.write(sc.nextLine() + "\n");        // Write the result in result.txt
                        } else {
                            System.out.print(sc.nextLine() + "\n"); // Output the result in the terminal
                        }
                    }
                    
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {e.printStackTrace();}
        }
    }
}