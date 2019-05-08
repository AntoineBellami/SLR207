# SLR207

Implementing Hadoop MapReduce "from scratch" in Java.

## Sequential implementation of words counting

This program can be configured (userneme - like "abellami" - and input file) through the project_root/Sequential/sequential.conf file.
The input file should be stored in a project_root/resources folder.

This program can be compiled and ran with the command 'ant' executed in the Sequential/ folder.

## Hadoop MapReduce (parallel implementation)

This program can be configured (userneme - like "abellami" - and input file) through the project_root/hadoop.conf file and through project_root/machines.txt (list of the machines that the program will try to reach).
The input file should be stored in a project_root/resources folder.

The MASTER, DEPLOY and SLAVE programs are compiled and runnable jar files are generated with the command 'ant compile && ant create_run_jar' executed in the project_root/ folder.

When running master.jar, the output of the program (words count) is by default written in a ./result.txt file.

## Performance

The linear implementation is incomparably more efficient than my Hadoop-Mapreduce implementation, essentially because the latter is slowed down by many files transfers and files readings/writings. As things stand now, this Hadoop-MapReduce implementation works but is not optimized (magnitude dozen of seconds for a 1MB input file).
