This directory contains scripts to manage a panFMP repository.
It is recommended to have separate panFMP installations for
each repository. The repository is located at "repository/" with
its config file. In "scripts/" are files to mange it. Windows
scripts end with ".cmd", Unix/Linux scripts with ".sh".

The scripts currently have no parameter parsing, they pass the config file
and all other parameters to the Java VM. If you miss parameters, e.g.
just call "./harvest.sh|.cmd", you may get a message from Java about missing
parameters. The first parameter "config.xml" is always automatically passed
by the scripts.

To configure Java options, edit config.sh|config.cmd. These files
contain variables with path specifications, used by the other scripts
to startup the Java VM and use the correct parameters.

If you use other metadata formats than the default DIF, you may raise
the memory limits or enable the 64bit Java VM (add "-d64" on some operating
systems). On 64bit systems it is also recommended for the reading part
of your application (SearchService), e.g. in the webserver config, to
add to Java options:
"-Dorg.apache.lucene.FSDirectory.class=org.apache.lucene.store.MMapDirectory"

To configure logging of script output to a file called "harvest.log" in the
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

panFMP also contains a bundled "Jetty Web Container", which can be
started serving the Axis API on http://127.0.0.1:8801 for web services.
You can configure this also in config.sh|.cmd. To detach the
web container after starting, change the options to use
"./webserver.log.properties" as log config and enable detaching
from console.