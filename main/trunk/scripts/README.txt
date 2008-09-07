This directory contains scripts to manage a panFMP repository.
It is recommended to have separate panFMP installations for
each repository. The repository is located at "repository/" with
its config file. In "scripts/" are files to mange it. Windows
scripts end with ".cmd", Unix/Linux scripts with ".sh".

The scripts currently have no parameter parsing, they pass the config file
and all other parameters to the Java VM. If you miss parameters, e.g.
just call "./harvest.sh", you will get a message from Java about missing
parameters. The first parameter "config.xml" is always automatically passed
by the scripts. Normally you only need to add an index ID or "*"
for all indexes.

To configure Java options, edit config.cmd/config.sh. These files
contain variables with path specifications, used by the other scripts
to startup the Java VM and use the correct parameters.

If you use other metadata formats than the default DIF, you may raise
the memory limits or enable the 64bit Java VM (add "-d64" on some operating
systems). On 64bit systems it is also recommended for the reading part
of your application (SearchService), e.g. in the webserver config, to
add to Java options:
"-Dorg.apache.lucene.FSDirectory.class=org.apache.lucene.store.MMapDirectory"

To change logging of script output to a file called "harvest.log" in the
repository directory, change the name of the logging properties file
to "./harvest.log.properties", which is bundled. This is optimal for
cron jobs, but not for testing.

For Unix/Linux there is also a locking mechanism:
Just call "./harvest.sh" and others this way:
	./withlock.sh ./harvest.sh "*"
This script returns with exit code 1, if the lock is still hold,
else calls "./harvest.sh" and returns 0. This helps to prevent
concurrent index modification in overlapping cron jobs. Use
withlock.sh in your cron job definitions!
