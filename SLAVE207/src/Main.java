import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

public class Main {

	public static void main(String[] args) {
		
		String username = args[0];
		int mode = Integer.parseInt(args[1]);
		
		/*
		 * Mode 0 corresponds to the mapping operation
		 */
		if (mode == 0) {
			String splitNumber = args[2];
			String pathToSplit = "/tmp/" + username + "/splits/S" + splitNumber + ".txt";

			HashSet<String> set = new HashSet<String>();
			
			Process p = null;
			FileReader fr = null;
			FileWriter fstream = null;
			BufferedWriter out = null;
			try {
				p = new ProcessBuilder("mkdir", "-p", "/tmp/" + username + "/maps/").start();
				p.waitFor();

				fr = new FileReader(pathToSplit);
				BufferedReader br = new BufferedReader(fr) ;
				Scanner        sc = new Scanner(br) ;
				
				String word = null;
				fstream = new FileWriter("/tmp/" + username + "/maps/UM" + splitNumber + ".txt");
				out = new BufferedWriter(fstream);

				while (sc.hasNext()) {
					word = sc.next().replace("\\\'", "\'").replace("\\\"", "\"");
					if (!set.contains(word)) {
						/*
						 * Write the key in the standard output stream
						 */
						System.out.println(word);
						set.add(word);
					}

					/*
					* Write the key and the value in a new line of the buffered writer,
					* separated by a space
					*/
					out.write(word + " 1\n");
				}
				out.close();
				fstream.close();
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/*
		 * Mode 1 corresponds to the shuffle operation
		 */
		if (mode==1) {
			String key = args[2];
			String SM = args[3];
			int inputUMNb = args.length - 4;

			FileReader fr = null;
			
			FileWriter fstream = null;
			BufferedWriter out = null;
				
			try {
				// Create filewriter and bufferedreader
				fstream = new FileWriter(SM);
				out = new BufferedWriter(fstream);

				for (int k=0; k<inputUMNb; k++) {

					fr = new FileReader(args[k+4]);
					BufferedReader br = new BufferedReader(fr);
					Scanner sc = new Scanner(br);
					
					String word = null;
					int count = 0;
					while(sc.hasNext()) {
						word = sc.next();
						count = sc.nextInt();
						if (key.equals(word)) {
							out.write(key + " " + count + "\n");
						}
					}
				}

				// lastly, close the file and end
				out.close();
				fstream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/*
		 * Mode 2 corresponds to the reduce operation
		 */
		if (mode == 2) {

			String key = args[2];
			String SM = args[3];
			String RM = args[4];

			FileReader fr = null;
			FileWriter fstream = null;
			BufferedWriter out = null;
			try {
				fstream = new FileWriter(RM);
				out = new BufferedWriter(fstream);

				fr = new FileReader(SM);
				LineNumberReader lnr = new LineNumberReader(fr);
				int linesCount = 0;
				while (lnr.readLine() != null) {
				  linesCount++;
				}
				lnr.close();

				out.write(key + " " + Integer.toString(linesCount));
				out.close();
				fstream.close();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}