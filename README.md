Load
====

Welcome
-------

This is the Cascading.Load (Load) application.

Load provides a simple command line interface for building high load
cluster jobs, based on Cascading.

Cascading is an application framework for Java developers to quickly
and easily develop robust Data Analytics and Data Management applications
on Apache Hadoop. It can be found at http://www.cascading.org/

Installing
----------

Installation is not necessary if you want to run Load directly from
the distribution folder, or if Load was pre-installed with your Hadoop
distribution.

To see if Load has already been added to your PATH, type:

    $ which load

To install for all users into `/usr/local/bin`:

    $ sudo ./bin/load install

or for the current user only into `~/.load`:

    $ ./bin/load install

For detailed instructions:

    $ ./bin/load help install

Choose the method that best suites your environment.

If you are running Load on AWS Elastic MapReduce, you need to follow the
[Elastic MapReduce instructions](https://aws.amazon.com/elasticmapreduce/#details)
on the AWS site, which typically expect the
`load-<release-date>.jar` to be uploaded to AWS S3.

Using
-----

The environment variable `HADOOP_HOME` should always be set to use
Cascading.Load.

To run from the command line with the jar, Hadoop should be in the path:

    $ hadoop jar load-<release-date>.jar <args>

or if Load has been installed from above:

    $ load <args>

If no args are given, a comprehensive list of commands will be
printed. That list is also available as `COMMANDS.md` in this
directory.

Building
--------

This release uses Cascading 2.0 and will pull all dependencies from
the relevant maven repos, including http://conjars.org

To build a jar,

    $ gradle jar

To test,

    $ gradle test

License
-------

See `apl.txt` in this directory.
