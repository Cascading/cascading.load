/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.load.util.Util;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import org.apache.log4j.Logger;

import static joptsimple.util.DateConverter.*;

//import org.kohsuke.args4j.Option;


/**
 *
 */
public class Options
  {
  private static final Logger LOG = Logger.getLogger( Options.class );

  public static final float MIN_DATA_STDDEV = Float.MIN_VALUE;
  public static final float DEF_DATA_STDDEV = 0.2f;
  public static final float MAX_DATA_STDDEV = 0.9999f;

  //////////////////////////////////////////////////////////////////////
  // inner class to parse options

  OptionParser parser = new OptionParser();
  ArrayList option_list = new ArrayList();

  private static Collection<String> trimDashes( Collection<String> params )
    {
    ArrayList<String> stripped = new ArrayList<String>();

    for( String param : params )
      {
      int i = 0;

      do
        {
        if( param.charAt( i ) != '-' )
          break;
        else
          i += 1;
        }
      while( i < param.length() );

      stripped.add( param.substring( i ) );
      }

    return stripped;
    }


  private class OptionGlyph
    {
    Collection<String> opt_param;
    Collection<String> opt_names;
    String method_name;
    Class arg_class;
    boolean is_required;
    boolean multi_value;
    String description;

    OptionGlyph( Collection<String> opt_param, String method_name, Class arg_class, boolean is_required, boolean multi_value, String description )
      {
      option_list.add( this );

      this.opt_param = opt_param;
      this.opt_names = trimDashes( opt_param );
      this.method_name = method_name;
      this.arg_class = arg_class;
      this.is_required = is_required;
      this.multi_value = multi_value;
      this.description = description;

      OptionSpecBuilder osb = Options.this.parser.acceptsAll( this.opt_names, this.description );

      if( this.arg_class != null )
        osb.withRequiredArg().ofType( this.arg_class );

      if( this.is_required )
        osb.isRequired();
      }


    String generateMarkdown()
      {
      StringBuilder line = new StringBuilder( "<tr><td><code>" );
      boolean is_first = true;

      for( String p : this.opt_param )
        {
        if( is_first )
          {
          line.append( p );
          is_first = false;
          }
        else
          line.append( "|" ).append( p );
        }

      line.append( "</code></td><td>" );
      line.append( this.description );

      if( this.arg_class != null )
        {
        line.append( "</td><td>requires argument" );
        }

      line.append( "</td></tr>" );

      return line.toString();
      }


    boolean attempt( OptionSet opts )
      {
      for( String opt_name : this.opt_names )
        {
        if( opts.has( opt_name ) )
          {
          try
            {
            Method method;
            Class clazz = Options.this.getClass();

            if( arg_class != null )
              {
              method = clazz.getMethod( this.method_name, new Class[]{this.arg_class} );

              if( this.multi_value )
                {
                for( Object val : opts.valuesOf( opt_name ) )
                  {
                  Object result = method.invoke( Options.this, new Object[]{val} );
                  }
                }
              else
                {
                Object result = method.invoke( Options.this, new Object[]{opts.valueOf( opt_name )} );
                }
              }
            else
              {
              method = clazz.getMethod( this.method_name, new Class[]{boolean.class} );
              Object result = method.invoke( Options.this, new Object[]{true} );
              }
            }
          catch( NoSuchMethodException e )
            {
            e.printStackTrace();
            }
          catch( IllegalAccessException e )
            {
            e.printStackTrace();
            }
          catch( InvocationTargetException e )
            {
            e.printStackTrace();
            }

          return true;
          }
        }

      return false;
      }
    }


  //////////////////////////////////////////////////////////////////////
  // option variables

  boolean singlelineStats = false;
  boolean debugLogging = false;
  int blockSizeMB = 64;
  int numDefaultMappers = -1;
  int numDefaultReducers = -1;
  float percentMaxMappers = 0;
  float percentMaxReducers = 0;
  boolean mapSpecExec = false;
  boolean reduceSpecExec = false;
  int tupleSpillThreshold = 100000;
  List<String> hadoopProperties = new ArrayList<String>();
  int numMappersPerBlock = 1; // multiplier for num mappers, needs 1.2 wip for this
  int numReducersPerMapper = -1;
  String childVMOptions = "-Xmx1000m -XX:+UseParallelOldGC";

  int maxConcurrentFlows = -1;
  int maxConcurrentSteps = -1;

  String inputRoot;
  String outputRoot;
  String workingRoot = "working_" + System.currentTimeMillis() + "_" + (int) ( Math.random() * 1000 );
  String statsRoot;

  boolean cleanWorkFiles = false;

  private boolean localMode = false;

  boolean runAllLoads = false;

  boolean dataGenerate;
  int dataNumFiles = 100;
  float dataFileSizeMB = 100;
  int dataMaxWords = 10;
  int dataMinWords = 10;
  String dataWordDelimiter = " "; // space
  int fillBlocksPerFile = -1;
  int fillFilesPerAvailMapper = -1;
  float dataMeanWords = -1;
  float dataStddevWords = -1;
  boolean dataConsume = false;

  boolean countSort;
  boolean staggeredSort;
  boolean fullTupleGroup;

  boolean multiJoin;
  boolean innerJoin;
  boolean outerJoin;
  boolean leftJoin;
  boolean rightJoin;

  boolean pipeline;
  boolean chainedAggregate;
  boolean chainedFunction;
  int hashModulo = -1;
  boolean writeDotFile = false;

  OptionGlyph help_option;
  OptionGlyph mark_option;


  public Options()
    {
    OptionGlyph glyph;
    OptionSpecBuilder osb;

    glyph = new OptionGlyph( Arrays.asList( "-h", "--help" ), "printUsageWrapper", null, false, false, "print this help text" );
    this.help_option = glyph;

    glyph = new OptionGlyph( Arrays.asList( "--markdown" ), "generateMarkdown", null, false, false, "generate help text as GitHub Flavored Markdown" );
    this.mark_option = glyph;

    glyph = new OptionGlyph( Arrays.asList( "-SLS" ), "setSinglelineStats", null, false, false, "single-line stats" );
    glyph = new OptionGlyph( Arrays.asList( "-X" ), "setDebugLogging", null, false, false, "debug logging" );
    glyph = new OptionGlyph( Arrays.asList( "-BS" ), "setBlockSizeMB", int.class, false, false, "default block size" );
    glyph = new OptionGlyph( Arrays.asList( "-NM" ), "setNumDefaultMappers", int.class, false, false, "default num mappers" );
    glyph = new OptionGlyph( Arrays.asList( "-NR" ), "setNumDefaultReducers", int.class, false, false, "default num reducers" );
    glyph = new OptionGlyph( Arrays.asList( "-PM" ), "setPercentMaxMappers", float.class, false, false, "percent of max mappers" );
    glyph = new OptionGlyph( Arrays.asList( "-PR" ), "setPercentMaxReducers", float.class, false, false, "percent of max reducers" );
    glyph = new OptionGlyph( Arrays.asList( "-EM" ), "setMapSpecExec", null, false, false, "enable map side speculative execution" );
    glyph = new OptionGlyph( Arrays.asList( "-ER" ), "setReduceSpecExec", null, false, false, "enable reduce side speculative execution" );
    glyph = new OptionGlyph( Arrays.asList( "-TS" ), "setTupleSpillThreshold", int.class, false, false, "tuple spill threshold, default 100,000" );
    glyph = new OptionGlyph( Arrays.asList( "-DH" ), "setHadoopProperties", String.class, false, true, "optional Hadoop config job properties (can be used multiple times)" );
    glyph = new OptionGlyph( Arrays.asList( "-MB" ), "setNumMappersPerBlock", int.class, false, false, "mappers per block (unused)" );
    glyph = new OptionGlyph( Arrays.asList( "-RM" ), "setNumReducersPerMapper", int.class, false, false, "reducers per mapper (unused)" );
    glyph = new OptionGlyph( Arrays.asList( "-I" ), "setInputRoot", String.class, true, false, "load input data path (generated data arrives here)" );
    glyph = new OptionGlyph( Arrays.asList( "-O" ), "setOutputRoot", String.class, true, false, "output path for load results" );
    glyph = new OptionGlyph( Arrays.asList( "-W" ), "setWorkingRoot", String.class, false, false, "input/output path for working files" );
    glyph = new OptionGlyph( Arrays.asList( "-S" ), "setStatsRoot", String.class, false, false, "output path for job stats" );
    glyph = new OptionGlyph( Arrays.asList( "-CWF" ), "setCleanWorkFiles", null, false, false, "clean work files" );
    glyph = new OptionGlyph( Arrays.asList( "-CVMO" ), "setChildVMOptions", String.class, false, false, "child JVM options" );
    glyph = new OptionGlyph( Arrays.asList( "-MXCF" ), "setMaxConcurrentFlows", int.class, false, false, "maximum concurrent flows" );
    glyph = new OptionGlyph( Arrays.asList( "-MXCS" ), "setMaxConcurrentSteps", int.class, false, false, "maximum concurrent steps" );
    glyph = new OptionGlyph( Arrays.asList( "-ALL" ), "setRunAllLoads", null, false, false, "run all available (non-discrete) loads" );
    glyph = new OptionGlyph( Arrays.asList( "-LM" ), "setLocalMode", null, false, false, "use the local platform" );
    glyph = new OptionGlyph( Arrays.asList( "-g", "--generate" ), "setDataGenerate", null, false, false, "generate test data" );
    glyph = new OptionGlyph( Arrays.asList( "-gf", "--generate-num-files" ), "setDataNumFiles", int.class, false, false, "num files to create" );
    glyph = new OptionGlyph( Arrays.asList( "-gs", "--generate-file-size" ), "setDataFileSizeMB", float.class, false, false, "size in MB of each file" );
    glyph = new OptionGlyph( Arrays.asList( "-gmax", "--generate-max-words" ), "setDataMaxWords", int.class, false, false, "max words per line, inclusive" );
    glyph = new OptionGlyph( Arrays.asList( "-gmin", "--generate-min-words" ), "setDataMinWords", int.class, false, false, "min words per line, inclusive" );
    glyph = new OptionGlyph( Arrays.asList( "-gd", "--generate-word-delimiter" ), "setDataWordDelimiter", String.class, false, false, "delimiter for words" );
    glyph = new OptionGlyph( Arrays.asList( "-gbf", "--generate-blocks-per-file" ), "setFillBlocksPerFile", int.class, false, false, "fill num blocks per file" );
    glyph = new OptionGlyph( Arrays.asList( "-gfm", "--generate-files-per-mapper" ), "setFillFilesPerAvailMapper", int.class, false, false, "fill num files per available mapper" );
    glyph = new OptionGlyph( Arrays.asList( "-gwm", "--generate-words-mean" ), "setDataMeanWords", float.class, false, false, "mean modifier [-1,1] of a normal distribution from dictionary" );
    glyph = new OptionGlyph( Arrays.asList( "-gws", "--generate-words-stddev" ), "setDataStddevWords", float.class, false, false, "standard-deviation modifier (0,1) of a normal distribution from dictionary" );
    glyph = new OptionGlyph( Arrays.asList( "-cd", "--consume" ), "setDataConsume", null, false, false, "consume test data" );
    glyph = new OptionGlyph( Arrays.asList( "-c", "--count-sort" ), "setCountSort", null, false, false, "run count sort load" );
    glyph = new OptionGlyph( Arrays.asList( "-ss", "--staggered-sort" ), "setStaggeredSort", null, false, false, "run staggered compare sort load" );
    glyph = new OptionGlyph( Arrays.asList( "-fg", "--full-group" ), "setFullTupleGroup", null, false, false, "run full tuple grouping load" );
    glyph = new OptionGlyph( Arrays.asList( "-m", "--multi-join" ), "setMultiJoin", null, false, false, "run multi join load" );
    glyph = new OptionGlyph( Arrays.asList( "-ij", "--inner-join" ), "setInnerJoin", null, false, false, "run inner join load" );
    glyph = new OptionGlyph( Arrays.asList( "-oj", "--outer-join" ), "setOuterJoin", null, false, false, "run outer join load" );
    glyph = new OptionGlyph( Arrays.asList( "-lj", "--left-join" ), "setLeftJoin", null, false, false, "run left join load" );
    glyph = new OptionGlyph( Arrays.asList( "-rj", "--right-join" ), "setRightJoin", null, false, false, "run right join load" );
    glyph = new OptionGlyph( Arrays.asList( "-p", "--pipeline" ), "setPipeline", null, false, false, "run pipeline load" );
    glyph = new OptionGlyph( Arrays.asList( "-pm", "--pipeline-hash-modulo" ), "setHashModulo", int.class, false, false, "hash modulo for managing key distribution" );
    glyph = new OptionGlyph( Arrays.asList( "-ca", "--chained-aggregate" ), "setChainedAggregate", null, false, false, "run chained aggregate load" );
    glyph = new OptionGlyph( Arrays.asList( "-cf", "--chained-function" ), "setChainedFunction", null, false, false, "run chained function load" );
    glyph = new OptionGlyph( Arrays.asList( "-wd", "--write-dot" ), "setWriteDotFile", null, false, false, "write DOT file" );
    }


  public void parseArgs( String[] args )
    {
    OptionSet opts = this.parser.parse( args );

    if( !opts.hasOptions() )
      {
      printUsage( false );
      System.exit( 1 );
      }
    else if( this.help_option.attempt( opts ) )
      {
      printUsage( false );
      System.exit( 0 );
      }
    else if( this.mark_option.attempt( opts ) )
      {
      printUsage( true );
      System.exit( 0 );
      }
    else
      {
      for( Object obj : option_list )
        {
        ( (OptionGlyph) obj ).attempt( opts );
        }
      }
    }


  //////////////////////////////////////////////////////////////////////
  // print markdown, usage, version, license

  public void printUsageWrapper( boolean ignore )
    {
    // placeholder: print help text
    }

  public void generateMarkdown( boolean ignore )
    {
    // placeholder: generate help text as GitHub Flavored Markdown
    }


  private static void printCascadingVersion()
    {
    try
      {
      Properties versionProperties = new Properties();

      InputStream stream = Cascade.class.getClassLoader().getResourceAsStream( "cascading/version.properties" );
      versionProperties.load( stream );

      stream = Cascade.class.getClassLoader().getResourceAsStream( "cascading/build.number.properties" );
      if( stream != null )
        versionProperties.load( stream );

      String releaseMajor = versionProperties.getProperty( "cascading.release.major" );
      String releaseMinor = versionProperties.getProperty( "cascading.release.minor", null );
      String releaseBuild = versionProperties.getProperty( "cascading.build.number", null );
      String releaseFull = null;

      if( releaseMinor == null )
        releaseFull = releaseMajor;
      else if( releaseBuild == null )
        releaseFull = String.format( "%s.%s", releaseMajor, releaseMinor );
      else
        releaseFull = String.format( "%s.%s%s", releaseMajor, releaseMinor, releaseBuild );

      System.out.println( String.format( "Using Cascading %s", releaseFull ) );
      }
    catch( IOException exception )
      {
      System.out.println( "Unknown Cascading Version" );
      }
    }

  private static void printLicense()
    {
    System.out.println( "This release is licensed under the Apache Software License 2.0.\n" );

    /*
    try
      {
      InputStream stream = Main.class.getResourceAsStream( "/LOAD-LICENSE.txt" );
      BufferedReader reader = new BufferedReader( new InputStreamReader( stream ) );

      System.out.print( "This release is licensed under the " );

      String line = reader.readLine();

      while( line != null )
        {
        if( line.matches( "^Binary License:.*$" ) )
          {
          System.out.println( line.substring( 15 ).trim() );
          break;
          }

        line = reader.readLine();
        }

      reader.close();
      }
    catch( Exception exception )
      {
      System.out.println( "Unspecified License" );
      }
    */
    }

  public void printUsage( boolean gen_markdown )
    {
    if( gen_markdown )
      {
      System.out.println( "Load - Command Line Reference" );
      System.out.println( "=============================" );

      System.out.println( "    cascading.load [param] [param] ..." );
      System.out.println( "" );
      System.out.println( "<table>" );
      }
    else
      {
      System.out.println( "cascading.load [options...]" );

      System.out.println( "" );
      System.out.println( "Usage:" );
      System.out.println( "" );
      }

    try
      {
      if( gen_markdown )
        {
        for( Object obj : option_list )
          {
          System.out.println( ( (OptionGlyph) obj ).generateMarkdown() );
          }
        }
      else
        {
        this.parser.printHelpOn( System.out );
        }

      }
    catch( IOException e )
      {
      e.printStackTrace();
      }

    if( gen_markdown )
      {
      System.out.println( "</table>" );
      }

    System.out.println( "" );
    printCascadingVersion();
    printLicense();
    }

  //////////////////////////////////////////////////////////////////////
  // option handlers

  public boolean isSinglelineStats()
    {
    return singlelineStats;
    }

  //@Option(name = "-SLS", usage = "single-line stats", required = false)
  public void setSinglelineStats( boolean singlelineStats )
    {
    this.singlelineStats = singlelineStats;
    }

  public boolean isDebugLogging()
    {
    return debugLogging;
    }

  //@Option(name = "-X", usage = "debug logging", required = false)
  public void setDebugLogging( boolean debugLogging )
    {
    this.debugLogging = debugLogging;
    }

  public int getBlockSizeMB()
    {
    return blockSizeMB;
    }

  //@Option(name = "-BS", usage = "default block size", required = false)
  public void setBlockSizeMB( int blockSizeMB )
    {
    this.blockSizeMB = blockSizeMB;
    }

  public int getNumDefaultMappers()
    {
    return numDefaultMappers;
    }

  //@Option(name = "-NM", usage = "default num mappers", required = false)
  public void setNumDefaultMappers( int numDefaultMappers )
    {
    this.numDefaultMappers = numDefaultMappers;
    }

  public int getNumDefaultReducers()
    {
    return numDefaultReducers;
    }

  //@Option(name = "-NR", usage = "default num reducers", required = false)
  public void setNumDefaultReducers( int numDefaultReducers )
    {
    this.numDefaultReducers = numDefaultReducers;
    }

  public float getPercentMaxMappers()
    {
    return percentMaxMappers;
    }

  //@Option(name = "-PM", usage = "percent of max mappers", required = false)
  public void setPercentMaxMappers( float percentMaxMappers )
    {
    this.percentMaxMappers = percentMaxMappers;
    }

  public float getPercentMaxReducers()
    {
    return percentMaxReducers;
    }

  //@Option(name = "-PR", usage = "percent of max reducers", required = false)
  public void setPercentMaxReducers( float percentMaxReducers )
    {
    this.percentMaxReducers = percentMaxReducers;
    }

  public boolean isMapSpecExec()
    {
    return mapSpecExec;
    }

  //@Option(name = "-EM", usage = "enable map side speculative execution", required = false)
  public void setMapSpecExec( boolean mapSpecExec )
    {
    this.mapSpecExec = mapSpecExec;
    }

  public boolean isReduceSpecExec()
    {
    return reduceSpecExec;
    }

  //@Option(name = "-ER", usage = "enable reduce side speculative execution", required = false)
  public void setReduceSpecExec( boolean reduceSpecExec )
    {
    this.reduceSpecExec = reduceSpecExec;
    }

  public int getTupleSpillThreshold()
    {
    return tupleSpillThreshold;
    }

  //@Option(name = "-TS", usage = "tuple spill threshold, default 100,000", required = false)
  public void setTupleSpillThreshold( int tupleSpillThreshold )
    {
    this.tupleSpillThreshold = tupleSpillThreshold;
    }

  public List<String> getHadoopProperties()
    {
    return hadoopProperties;
    }

  //@Option(name = "-DH", usage = "optional Hadoop config job properties", required = false, multiValued = true)
  public void setHadoopProperties( String hadoopProperty )
    {
    this.hadoopProperties.add( hadoopProperty );
    }

  public int getNumMappersPerBlock()
    {
    return numMappersPerBlock;
    }

  //@Option(name = "-MB", usage = "mappers per block (unused)", required = false)
  public void setNumMappersPerBlock( int numMappersPerBlock )
    {
    this.numMappersPerBlock = numMappersPerBlock;
    }

  public int getNumReducersPerMapper()
    {
    return numReducersPerMapper;
    }

  //@Option(name = "-RM", usage = "reducers per mapper (unused)", required = false)
  public void setNumReducersPerMapper( int numReducersPerMapper )
    {
    this.numReducersPerMapper = numReducersPerMapper;
    }

  //////////////////////////////////

  public String getInputRoot()
    {
    return makePathDir( inputRoot );
    }

  //@Option(name = "-I", usage = "load input data path (generated data arrives here)", required = true)
  public void setInputRoot( String inputRoot )
    {
    this.inputRoot = inputRoot;
    }

  public String getOutputRoot()
    {
    return makePathDir( outputRoot );
    }

  //@Option(name = "-O", usage = "output path for load results", required = true)
  public void setOutputRoot( String outputRoot )
    {
    this.outputRoot = outputRoot;
    }

  public String getWorkingRoot()
    {
    return makePathDir( workingRoot );
    }

  //@Option(name = "-W", usage = "input/output path for working files", required = false)
  public void setWorkingRoot( String workingRoot )
    {
    this.workingRoot = workingRoot;
    }

  public boolean hasStatsRoot()
    {
    return statsRoot != null;
    }

  public String getStatsRoot()
    {
    return makePathDir( statsRoot );
    }

  //@Option(name = "-S", usage = "output path for job stats", required = false)
  public void setStatsRoot( String statsRoot )
    {
    this.statsRoot = statsRoot;
    }

  public boolean isCleanWorkFiles()
    {
    return cleanWorkFiles;
    }

  //@Option(name = "-CWF", usage = "clean work files", required = false)
  public void setCleanWorkFiles( boolean cleanWorkFiles )
    {
    this.cleanWorkFiles = cleanWorkFiles;
    }

  public String getChildVMOptions()
    {
    return childVMOptions;
    }

  //@Option(name = "-CVMO", usage = "child JVM options", required = false)
  public void setChildVMOptions( String childVMOptions )
    {
    this.childVMOptions = childVMOptions;
    }

  public int getMaxConcurrentFlows()
    {
    return maxConcurrentFlows;
    }

  //@Option(name = "-MXCF", usage = "maximum concurrent flows", required = false)
  public void setMaxConcurrentFlows( int maxConcurrentFlows )
    {
    // Treat as "default" setting
    if( maxConcurrentFlows < 0 )
      maxConcurrentFlows = -1;
    this.maxConcurrentFlows = maxConcurrentFlows;
    }

  public int getMaxConcurrentSteps()
    {
    return maxConcurrentSteps;
    }

  //@Option(name = "-MXCS", usage = "maximum concurrent steps", required = false)
  public void setMaxConcurrentSteps( int maxConcurrentSteps )
    {
    // Treat as "default" setting
    if( maxConcurrentSteps < 0 )
      maxConcurrentSteps = -1;
    this.maxConcurrentSteps = maxConcurrentSteps;
    }


  private String makePathDir( String path )
    {
    if( path == null || path.isEmpty() )
      return "./";

    if( !path.endsWith( "/" ) )
      path += "/";

    return path;
    }

  public boolean isRunAllLoads()
    {
    return runAllLoads;
    }

  //@Option(name = "-ALL", usage = "run all available (non-discrete) loads", required = false)
  public void setRunAllLoads( boolean runAllLoads )
    {
    this.runAllLoads = runAllLoads;
    }

  public boolean isLocalMode()
    {
    return localMode;
    }

  //@Option(name = "-LM", usage = "use the local platform", required = false)
  public void setLocalMode( boolean localMode )
    {
    this.localMode = localMode;
    }

//////////////////////////////////

  public boolean isDataGenerate()
    {
    return dataGenerate;
    }

  //@Option(name = "-g", aliases = {"--generate"}, usage = "generate test data", required = false)
  public void setDataGenerate( boolean dataGenerate )
    {
    this.dataGenerate = dataGenerate;
    }

  public int getDataNumFiles()
    {
    return dataNumFiles;
    }

  //@Option(name = "-gf", aliases = {"--generate-num-files"}, usage = "num files to create", required = false)
  public void setDataNumFiles( int dataNumFiles )
    {
    this.dataNumFiles = dataNumFiles;
    }

  public float getDataFileSizeMB()
    {
    return dataFileSizeMB;
    }

  //@Option(name = "-gs", aliases = {"--generate-file-size"}, usage = "size in MB of each file", required = false)
  public void setDataFileSizeMB( float dataFileSizeMB )
    {
    this.dataFileSizeMB = dataFileSizeMB;
    }

  public int getDataMaxWords()
    {
    return dataMaxWords;
    }

  //@Option(name = "-gmax", aliases = {"--generate-max-words"}, usage = "max words per line, inclusive", required = false)
  public void setDataMaxWords( int dataMaxWords )
    {
    this.dataMaxWords = dataMaxWords;
    }

  public int getDataMinWords()
    {
    return dataMinWords;
    }

  //@Option(name = "-gmin", aliases = {"--generate-min-words"}, usage = "min words per line, inclusive", required = false)
  public void setDataMinWords( int dataMinWords )
    {
    this.dataMinWords = dataMinWords;
    }

  public String getDataWordDelimiter()
    {
    return dataWordDelimiter;
    }

  //@Option(name = "-gd", aliases = {"--generate-word-delimiter"}, usage = "delimiter for words", required = false)
  public void setDataWordDelimiter( String dataWordDelimiter )
    {
    this.dataWordDelimiter = dataWordDelimiter;
    }

  public int getFillBlocksPerFile()
    {
    return fillBlocksPerFile;
    }

  //@Option(name = "-gbf", aliases = {"--generate-blocks-per-file"}, usage = "fill num blocks per file", required = false)
  public void setFillBlocksPerFile( int fillBlocksPerFile )
    {
    this.fillBlocksPerFile = fillBlocksPerFile;
    }

  public int getFillFilesPerAvailMapper()
    {
    return fillFilesPerAvailMapper;
    }

  //@Option(name = "-gfm", aliases = {"--generate-files-per-mapper"}, usage = "fill num files per available mapper", required = false)
  public void setFillFilesPerAvailMapper( int fillFilesPerAvailMapper )
    {
    this.fillFilesPerAvailMapper = fillFilesPerAvailMapper;
    }

  //TODO --generate-words-normal [<mean>][,<stddev>]
  //Note that ',' is a potential decimal-point

  public float getDataMeanWords()
    {
    return dataMeanWords;
    }

  //@Option(name = "-gwm", aliases = {"--generate-words-mean"}, usage = "mean modifier [-1,1] of a normal distribution from dictionary", required = false)
  public void setDataMeanWords( float dataMeanWords )
    {
    if( dataMeanWords < -1 )
      dataMeanWords = -1;
    else if( dataMeanWords > 1 )
      dataMeanWords = 1;
    this.dataMeanWords = dataMeanWords;
    }

  public float getDataStddevWords()
    {
    return dataStddevWords;
    }

  //@Option(name = "-gws", aliases = {"--generate-words-stddev"}, usage = "standard-deviation modifier (0,1) of a normal distribution from dictionary", required = false)
  public void setDataStddevWords( float dataStddevWords )
    {
    if( dataStddevWords < MIN_DATA_STDDEV )
      dataStddevWords = MIN_DATA_STDDEV;
    else if( dataStddevWords > MAX_DATA_STDDEV )
      dataStddevWords = MAX_DATA_STDDEV;
    this.dataStddevWords = dataStddevWords;
    }

  public boolean useNormalDistribution()
    {
    return dataMeanWords != -1 || dataStddevWords != -1;
    }

  private String getDataNormalDesc()
    {
    return "normal(" + getDataMeanWords() + "," + getDataStddevWords() + ")";
    }

  public boolean isDataConsume()
    {
    return dataConsume;
    }

  //@Option(name = "-cd", aliases = {"--consume"}, usage = "consume test data", required = false)
  public void setDataConsume( boolean dataConsume )
    {
    this.dataConsume = dataConsume;
    }

  ////////////////////////////////////////

  public boolean isCountSort()
    {
    return countSort;
    }

  //@Option(name = "-c", aliases = {"--count-sort"}, usage = "run count sort load", required = false)
  public void setCountSort( boolean countSort )
    {
    this.countSort = countSort;
    }

  public boolean isStaggeredSort()
    {
    return staggeredSort;
    }

  //@Option(name = "-ss", aliases = {"--staggered-sort"}, usage = "run staggered compare sort load", required = false)
  public void setStaggeredSort( boolean staggeredSort )
    {
    this.staggeredSort = staggeredSort;
    }

  public boolean isFullTupleGroup()
    {
    return fullTupleGroup;
    }

  //@Option(name = "-fg", aliases = {"--full-group"}, usage = "run full tuple grouping load", required = false)
  public void setFullTupleGroup( boolean fullTupleGroup )
    {
    this.fullTupleGroup = fullTupleGroup;
    }

  ////////////////////////////////////////

  public boolean isMultiJoin()
    {
    return multiJoin;
    }

  //@Option(name = "-m", aliases = {"--multi-join"}, usage = "run multi join load", required = false)
  public void setMultiJoin( boolean multiJoin )
    {
    this.multiJoin = multiJoin;
    }

  public boolean isInnerJoin()
    {
    return innerJoin;
    }

  //@Option(name = "-ij", aliases = {"--inner-join"}, usage = "run inner join load", required = false)
  public void setInnerJoin( boolean innerJoin )
    {
    this.innerJoin = innerJoin;
    }

  public boolean isOuterJoin()
    {
    return outerJoin;
    }

  //@Option(name = "-oj", aliases = {"--outer-join"}, usage = "run outer join load", required = false)
  public void setOuterJoin( boolean outerJoin )
    {
    this.outerJoin = outerJoin;
    }

  public boolean isLeftJoin()
    {
    return leftJoin;
    }

  //@Option(name = "-lj", aliases = {"--left-join"}, usage = "run left join load", required = false)
  public void setLeftJoin( boolean leftJoin )
    {
    this.leftJoin = leftJoin;
    }

  public boolean isRightJoin()
    {
    return rightJoin;
    }

  //@Option(name = "-rj", aliases = {"--right-join"}, usage = "run right join load", required = false)
  public void setRightJoin( boolean rightJoin )
    {
    this.rightJoin = rightJoin;
    }

  ////////////////////////////////////////

  public boolean isPipeline()
    {
    return pipeline;
    }

  //@Option(name = "-p", aliases = {"--pipeline"}, usage = "run pipeline load", required = false)
  public void setPipeline( boolean pipeline )
    {
    this.pipeline = pipeline;
    }

  public int getHashModulo()
    {
    return hashModulo;
    }

  //@Option(name = "-pm", aliases = {"--pipeline-hash-modulo"}, usage = "hash modulo for managing key distribution", required = false)
  public void setHashModulo( int hashModulo )
    {
    this.hashModulo = hashModulo;
    }

  public boolean isChainedAggregate()
    {
    return chainedAggregate;
    }

  //@Option(name = "-ca", aliases = {"--chained-aggregate"}, usage = "run chained aggregate load", required = false)
  public void setChainedAggregate( boolean chainedAggregate )
    {
    this.chainedAggregate = chainedAggregate;
    }

  public boolean isChainedFunction()
    {
    return chainedFunction;
    }

  //@Option(name = "-cf", aliases = {"--chained-function"}, usage = "run chained function load", required = false)
  public void setChainedFunction( boolean chainedFunction )
    {
    this.chainedFunction = chainedFunction;
    }

  public boolean isWriteDotFile()
    {
    return writeDotFile;
    }

  //@Option(name = "-wd", aliases = {"--write-dot"}, usage = "write DOT file", required = false)
  public void setWriteDotFile( boolean writeDotFile )
    {
    this.writeDotFile = writeDotFile;
    }


  ////////////////////////////////////////

  public void prepare()
    {
    if( isRunAllLoads() )
      {
      setDataGenerate( true );
      setCountSort( true );
      setMultiJoin( true );
      setPipeline( true );
      }

    if( numDefaultMappers == -1 && percentMaxMappers != 0 )
      numDefaultMappers = (int) ( Util.getMaxConcurrentMappers() * percentMaxMappers );

    if( numDefaultReducers == -1 && percentMaxReducers != 0 )
      numDefaultReducers = (int) ( Util.getMaxConcurrentReducers() * percentMaxReducers );

    if( numDefaultMappers != -1 )
      LOG.info( "using default mappers: " + numDefaultMappers );

    if( numDefaultReducers != -1 )
      LOG.info( "using default reducers: " + numDefaultReducers );

    if( fillBlocksPerFile != -1 )
      {
      dataFileSizeMB = blockSizeMB * fillBlocksPerFile;
      LOG.info( "using file size (MB): " + dataFileSizeMB );
      }

    if( fillFilesPerAvailMapper != -1 )
      {
      dataNumFiles = Util.getMaxConcurrentMappers() * fillFilesPerAvailMapper;
      LOG.info( "using num files: " + dataNumFiles );
      }

    if( dataMaxWords < dataMinWords )
      {
      dataMaxWords = dataMinWords;
      LOG.info( "using max words: " + dataMaxWords );
      }
    }

  private String getLoadsDesc()
    {
    final StringBuilder loads = new StringBuilder( "(" );

    if( dataGenerate )
      loads.append( "dataGenerate," );
    if( dataConsume )
      loads.append( "dataConsume," );
    if( pipeline )
      loads.append( "pipeline," );
    if( countSort )
      loads.append( "countSort," );
    if( multiJoin )
      loads.append( "multiJoin," );
    if( fullTupleGroup )
      loads.append( "fullTupleGroup," );
    if( staggeredSort )
      loads.append( "staggeredSort," );
    if( chainedFunction )
      loads.append( "chainedFunction," );
    if( chainedAggregate )
      loads.append( "chainedAggregate," );
    if( innerJoin )
      loads.append( "innerJoin," );
    if( outerJoin )
      loads.append( "outerJoin," );
    if( leftJoin )
      loads.append( "leftJoin," );
    if( rightJoin )
      loads.append( "rightJoin," );

    if( loads.length() > 1 )
      loads.setCharAt( loads.length() - 1, ')' );
    else
      loads.append( ')' );

    return loads.toString();
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "Options" );
    sb.append( "{singlelineStats=" ).append( singlelineStats );
    sb.append( ", debugLogging=" ).append( debugLogging );
    sb.append( ", blockSizeMB=" ).append( blockSizeMB );
    sb.append( ", numDefaultMappers=" ).append( numDefaultMappers );
    sb.append( ", numDefaultReducers=" ).append( numDefaultReducers );
    sb.append( ", percentMaxMappers=" ).append( percentMaxMappers );
    sb.append( ", percentMaxReducers=" ).append( percentMaxReducers );
    sb.append( ", mapSpecExec=" ).append( mapSpecExec );
    sb.append( ", reduceSpecExec=" ).append( reduceSpecExec );
    sb.append( ", tupleSpillThreshold=" ).append( tupleSpillThreshold );
    sb.append( ", hadoopProperties=" ).append( hadoopProperties );
    sb.append( ", numMappersPerBlock=" ).append( numMappersPerBlock );
    sb.append( ", numReducersPerMapper=" ).append( numReducersPerMapper );
    sb.append( ", childVMOptions='" ).append( childVMOptions ).append( '\'' );
    sb.append( ", maxConcurrentFlows=" ).append( maxConcurrentFlows );
    sb.append( ", maxConcurrentSteps=" ).append( maxConcurrentSteps );
    sb.append( ", inputRoot='" ).append( inputRoot ).append( '\'' );
    sb.append( ", outputRoot='" ).append( outputRoot ).append( '\'' );
    sb.append( ", workingRoot='" ).append( workingRoot ).append( '\'' );
    sb.append( ", statsRoot='" ).append( statsRoot ).append( '\'' );
    sb.append( ", cleanWorkFiles=" ).append( cleanWorkFiles );
    sb.append( ", localMode=" ).append( localMode );
    sb.append( ", runAllLoads=" ).append( runAllLoads );
    sb.append( ", dataGenerate=" ).append( dataGenerate );
    sb.append( ", dataNumFiles=" ).append( dataNumFiles );
    sb.append( ", dataFileSizeMB=" ).append( dataFileSizeMB );
    sb.append( ", dataMaxWords=" ).append( dataMaxWords );
    sb.append( ", dataMinWords=" ).append( dataMinWords );
    sb.append( ", dataWordDelimiter='" ).append( dataWordDelimiter ).append( '\'' );
    sb.append( ", fillBlocksPerFile=" ).append( fillBlocksPerFile );
    sb.append( ", fillFilesPerAvailMapper=" ).append( fillFilesPerAvailMapper );
    sb.append( ", dataMeanWords=" ).append( dataMeanWords );
    sb.append( ", dataStddevWords=" ).append( dataStddevWords );
    sb.append( ", dataConsume=" ).append( dataConsume );
    sb.append( ", countSort=" ).append( countSort );
    sb.append( ", staggeredSort=" ).append( staggeredSort );
    sb.append( ", fullTupleGroup=" ).append( fullTupleGroup );
    sb.append( ", multiJoin=" ).append( multiJoin );
    sb.append( ", innerJoin=" ).append( innerJoin );
    sb.append( ", outerJoin=" ).append( outerJoin );
    sb.append( ", leftJoin=" ).append( leftJoin );
    sb.append( ", rightJoin=" ).append( rightJoin );
    sb.append( ", pipeline=" ).append( pipeline );
    sb.append( ", chainedAggregate=" ).append( chainedAggregate );
    sb.append( ", chainedFunction=" ).append( chainedFunction );
    sb.append( ", hashModulo=" ).append( hashModulo );
    sb.append( ", writeDotFile=" ).append( writeDotFile );
    sb.append( '}' );
    return sb.toString();
    }
  }
