Load - Command Line Reference
=============================
    load [param] [param] ...

At least one flow must be selected, to run Load

<table>
<tr><td><code>-h|--help</code></td><td>print this help text</td><td></td></tr>
<tr><td><code>--markdown</code></td><td>generate help text as GitHub Flavored Markdown</td><td></td></tr>
<tr><td><code>-pf|--platform</code></td><td>set platform</td><td>requires argument</td></tr>
<tr><td><code>-ALL</code></td><td>run all available loads (not intended to produce errors)</td><td></td></tr>
<tr><td><code>-dt|--destructive-testing</code></td><td>run loads that are intended to produce errors</td><td></td></tr>
<tr><td><code>-ct|--comparison-testing</code></td><td>run loads that are intended for comparison across platforms</td><td></td></tr>
<tr><td><code>-SLS</code></td><td>single-line stats</td><td></td></tr>
<tr><td><code>-X</code></td><td>debug logging</td><td></td></tr>
<tr><td><code>-BS</code></td><td>default block size</td><td>requires argument</td></tr>
<tr><td><code>-NM</code></td><td>default num mappers</td><td>requires argument</td></tr>
<tr><td><code>-NR</code></td><td>default num reducers</td><td>requires argument</td></tr>
<tr><td><code>-PM</code></td><td>percent of max mappers</td><td>requires argument</td></tr>
<tr><td><code>-PR</code></td><td>percent of max reducers</td><td>requires argument</td></tr>
<tr><td><code>-EM</code></td><td>enable map side speculative execution</td><td></td></tr>
<tr><td><code>-ER</code></td><td>enable reduce side speculative execution</td><td></td></tr>
<tr><td><code>-TS</code></td><td>tuple spill threshold, default 100,000</td><td>requires argument</td></tr>
<tr><td><code>-DH</code></td><td>optional Hadoop config job properties (can be used multiple times)</td><td>requires argument</td></tr>
<tr><td><code>-I</code></td><td>load input data path (generated data arrives here)</td><td>requires argument</td></tr>
<tr><td><code>-O</code></td><td>output path for load results</td><td>requires argument</td></tr>
<tr><td><code>-W</code></td><td>input/output path for working files</td><td>requires argument</td></tr>
<tr><td><code>-S</code></td><td>output path for job stats</td><td>requires argument</td></tr>
<tr><td><code>-CWF</code></td><td>clean all files from prior run</td><td></td></tr>
<tr><td><code>-CVMO</code></td><td>child JVM options</td><td>requires argument</td></tr>
<tr><td><code>-MXCF</code></td><td>maximum concurrent flows</td><td>requires argument</td></tr>
<tr><td><code>-MXCS</code></td><td>maximum concurrent steps</td><td>requires argument</td></tr>
<tr><td><code>-g|--generate</code></td><td>generate test data</td><td></td></tr>
<tr><td><code>-gf|--generate-num-files</code></td><td>num files to create</td><td>requires argument</td></tr>
<tr><td><code>-gs|--generate-file-size</code></td><td>size in MB of each file</td><td>requires argument</td></tr>
<tr><td><code>-gmax|--generate-max-words</code></td><td>max words per line, inclusive</td><td>requires argument</td></tr>
<tr><td><code>-gmin|--generate-min-words</code></td><td>min words per line, inclusive</td><td>requires argument</td></tr>
<tr><td><code>-gd|--generate-word-delimiter</code></td><td>delimiter for words</td><td>requires argument</td></tr>
<tr><td><code>-gbf|--generate-blocks-per-file</code></td><td>fill num blocks per file</td><td>requires argument</td></tr>
<tr><td><code>-gfm|--generate-files-per-mapper</code></td><td>fill num files per available mapper</td><td>requires argument</td></tr>
<tr><td><code>-gwm|--generate-words-mean</code></td><td>mean modifier [-1,1] of a normal distribution from dictionary</td><td>requires argument</td></tr>
<tr><td><code>-gws|--generate-words-stddev</code></td><td>standard-deviation modifier (0,1) of a normal distribution from dictionary</td><td>requires argument</td></tr>
<tr><td><code>-cd|--consume</code></td><td>consume test data</td><td></td></tr>
<tr><td><code>-s|--certify-tests</code></td><td>run certification tests</td><td></td></tr>
<tr><td><code>-c|--count-sort</code></td><td>run count sort load</td><td></td></tr>
<tr><td><code>-ss|--staggered-sort</code></td><td>run staggered compare sort load</td><td></td></tr>
<tr><td><code>-fg|--full-group</code></td><td>run full tuple grouping load</td><td></td></tr>
<tr><td><code>-m|--multi-join</code></td><td>run multi join load</td><td></td></tr>
<tr><td><code>-ij|--inner-join</code></td><td>run inner join load</td><td></td></tr>
<tr><td><code>-pij|--pathological-inner-join</code></td><td>run pathological inner join load</td><td></td></tr>
<tr><td><code>-oj|--outer-join</code></td><td>run outer join load</td><td></td></tr>
<tr><td><code>-lj|--left-join</code></td><td>run left join load</td><td></td></tr>
<tr><td><code>-rj|--right-join</code></td><td>run right join load</td><td></td></tr>
<tr><td><code>-p|--pipeline</code></td><td>run pipeline load</td><td></td></tr>
<tr><td><code>-pm|--pipeline-hash-modulo</code></td><td>hash modulo for managing key distribution</td><td>requires argument</td></tr>
<tr><td><code>-ca|--chained-aggregate</code></td><td>run chained aggregate load</td><td></td></tr>
<tr><td><code>-cf|--chained-function</code></td><td>run chained function load</td><td></td></tr>
<tr><td><code>-wd|--write-dot</code></td><td>write DOT file</td><td></td></tr>
<tr><td><code>-wt|--write-trace</code></td><td>write planner trace files</td><td></td></tr>
<tr><td><code>-an|--app-name</code></td><td>set the application name</td><td>requires argument</td></tr>
<tr><td><code>-tn|--tags</code></td><td>set the application tags, comma separated</td><td>requires argument</td></tr>
</table>


This release is licensed under the Apache Software License 2.0.

