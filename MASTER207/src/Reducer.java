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
    private ArrayList<String> machinesDeployed = new ArrayList<String>();
	private HashMap<String, String> UMx_machines_dict = new HashMap<String, String>();
    private HashMap<String, List<String>> keys_UMx_dict = new HashMap<String, List<String>>();
    private HashMap<String, Set<String>> shufflers_keys_dict = new HashMap<String, Set<String>>();
    private ConcurrentSkipListSet<Integer> ids = new ConcurrentSkipListSet();
    private HashMap<String, String> RMx_machines_dict = new HashMap<String, String>();
    
    public HashMap<String, String> getUMx_machines_dict() {
		return UMx_machines_dict;
	}
	public HashMap<String, List<String>> getKeys_UMx_dict() {
		return keys_UMx_dict;
	}

    public Reducer(ArrayList<String> machinesDeployed, HashMap<String,
                   String> UMx_machines_dict, HashMap<String, List<String>> keys_UMx_dict) {
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
        // Iterator on machines
        Iterator machinesIterator = machinesDeployed.iterator();
        Set<String> keys = keys_UMx_dict.keySet();
        
        // Assign each key to a machine
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

            shufflers_keys_dict.get(shuffle_machine).parallelStream().forEach(key -> {

                keys_UMx_dict.get(key).parallelStream().forEach(UM -> {

                    String machine_sender = UMx_machines_dict.get(UM);

                    Process p = null;	
                    try {
                        p = new ProcessBuilder("scp", "abellami@" + machine_sender
                                               + ":/tmp/abellami/maps/" + UM + ".txt",
                                               "abellami@" + shuffle_machine
                                               + ":/tmp/abellami/maps").start();
                        p.waitFor();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
                
                // for each key, name it from 0 to keyNb-1
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
                    command.add("abellami@" + shuffle_machine);
                    command.add("java");
                    command.add("-jar");
                    command.add("/tmp/abellami/slave.jar");
                    command.add("1");
                    command.add(key);
                    command.add("/tmp/abellami/maps/" + SM + ".txt");
                    for (String UM: keys_UMx_dict.get(key)) {
                        command.add("/tmp/abellami/maps/" + UM + ".txt");
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
                    command.add("abellami@" + shuffle_machine);
                    command.add("java");
                    command.add("-jar");
                    command.add("/tmp/abellami/slave.jar");
                    command.add("2");
                    command.add(key);
                    command.add("/tmp/abellami/maps/" + SM + ".txt");
                    command.add("/tmp/abellami/reduces/" + RM + ".txt");

                    q = new ProcessBuilder(command).start();
                    q.waitFor();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            });
        });
    }

    public void displayResult() {
        Process p = null;
        try {
            p = new ProcessBuilder("mkdir", "-p", "/tmp/abellami/results/").start();
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
                                           ":/tmp/abellami/reduces/" + RM + ".txt",
                                           "cat", "/tmp/abellami/results").start();
                    p.waitFor();
                    fr = new FileReader("/tmp/abellami/results/" + RM + ".txt");
                    BufferedReader br = new BufferedReader(fr) ;
                    Scanner        sc = new Scanner(br) ;
                    
                    while (sc.hasNextLine()) {
                        out.write(sc.nextLine() + "\n");
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