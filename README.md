Load
====

Welcome
-------

This is the Cascading.Load (Load) application.

Load provides a simple command line interface for building high load
cluster jobs, based on Cascading.

Cascading is a feature rich API for defining and executing complex,
scale-free, and fault tolerant data processing workflows on a Hadoop
cluster. It can be found at http://www.cascading.org/

Installing
----------

Installation is not necessary if you want to run Load directly from
the distribution folder, or if Load was pre-installed with your Hadoop
distribution.

To see if Load has already been added to your PATH, type:

    $ which cascading.load

To install for all users into `/usr/local/bin`:

    $ sudo ./bin/cascading.load install

or for the current user only into `~/.cascading.load`:

    $ ./bin/cascading.load install

For detailed instructions:

    $ ./bin/cascading.load help install

Choose the method that best suites your environment.

If you are running Load on AWS Elastic MapReduce, you need to follow the 
[Elastic MapReduce instructions](https://aws.amazon.com/elasticmapreduce/#details)
on the AWS site, which typically expect the 
`cascading.load-<release-date>.jar` to be uploaded to AWS S3.

Using
-----

The environment variable `HADOOP_HOME` should always be set to use
Cascading.Load.

To run from the command line with the jar, Hadoop should be in the path:

    $ hadoop jar cascading.load-<release-date>.jar <args>

If no args are given, a comprehensive list of commands will be printed.

Or if Load has been installed from above:

    $ cascading.load <args>

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

See `LICENSE.txt` in this directory.
