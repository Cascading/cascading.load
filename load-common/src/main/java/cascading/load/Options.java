/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cascading.load.platform.CascadeLoadPlatform;
import cascading.load.platform.PlatformLoader;
import cascading.load.util.Util;
import cascading.util.Version;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import org.apache.log4j.Logger;

import static java.util.Arrays.asList;

/**
 * Class for handling the commandline options.
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
  ArrayList optionList = new ArrayList();

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
    List<String> optParam;
    Collection<String> optNames;
    String methodName;
    Class argClass;
    boolean isRequired;
    boolean multiValue;
    String description;

    OptionGlyph( List<String> optParam, String methodName, Class argClass, boolean isRequired, boolean multiValue, String description )
      {
      optionList.add( this );

      this.optParam = optParam;
      this.optNames = trimDashes( optParam );
      this.methodName = methodName;
      this.argClass = argClass;
      this.isRequired = isRequired;
      this.multiValue = multiValue;
      this.description = description;

      OptionSpecBuilder osb = Options.this.parser.acceptsAll( this.optNames, this.description );

      if( this.argClass != null )
        osb.withRequiredArg().ofType( this.argClass );
      }

    String generateMarkdown()
      {
      StringBuilder line = new StringBuilder( "<tr><td><code>" );
      boolean isFirst = true;

      for( String p : this.optParam )
        {
        if( isFirst )
          {
          line.append( p );
          isFirst = false;
          }
        else
          line.append( "|" ).append( p );
        }

      line.append( "</code></td><td>" );
      line.append( this.description );

      if( this.argClass != null )
        line.append( "</td><td>requires argument" );
      else
        line.append( "</td><td>" );

      line.append( "</td></tr>" );

      return line.toString();
      }

    boolean attempt( OptionSet opts ) throws Exception
      {
      for( String optName : this.optNames )
        {
        if( opts.has( optName ) )
          {
          try
            {
            Method method;
            Class clazz = Options.this.getClass();

            if( argClass != null )
              {
              method = clazz.getMethod( this.methodName, new Class[]{this.argClass} );

              if( this.multiValue )
                {
                for( Object val : opts.valuesOf( optName ) )
                  method.invoke( Options.this, new Object[]{val} );
                }
              else
                {
                method.invoke( Options.this, new Object[]{opts.valueOf( optName )} );
                }
              }
            else
              {
              method = clazz.getMethod( this.methodName, new Class[]{boolean.class} );
              method.invoke( Options.this, new Object[]{true} );
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

      if( this.isRequired )
        throw new Exception( "The " + this.optParam.get( 0 ) + " option is required" );
      else
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

  boolean certifyTests;
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
  private String platformName;


  OptionGlyph helpOption;
  OptionGlyph markOption;



  public Options()
    {
    OptionGlyph glyph;

    glyph = new OptionGlyph( asList( "-h", "--help" ), "printUsageWrapper", null, false, false, "print this help text" );
    this.helpOption = glyph;

    glyph = new OptionGlyph( asList( "--markdown" ), "generateMarkdown", null, false, false, "generate help text as GitHub Flavored Markdown" );
    this.markOption = glyph;

    glyph = new OptionGlyph( asList( "-SLS" ), "setSinglelineStats", null, false, false, "single-line stats" );
    glyph = new OptionGlyph( asList( "-X" ), "setDebugLogging", null, false, false, "debug logging" );
    glyph = new OptionGlyph( asList( "-BS" ), "setBlockSizeMB", int.class, false, false, "default block size" );
    glyph = new OptionGlyph( asList( "-NM" ), "setNumDefaultMappers", int.class, false, false, "default num mappers" );
    glyph = new OptionGlyph( asList( "-NR" ), "setNumDefaultReducers", int.class, false, false, "default num reducers" );
    glyph = new OptionGlyph( asList( "-PM" ), "setPercentMaxMappers", float.class, false, false, "percent of max mappers" );
    glyph = new OptionGlyph( asList( "-PR" ), "setPercentMaxReducers", float.class, false, false, "percent of max reducers" );
    glyph = new OptionGlyph( asList( "-EM" ), "setMapSpecExec", null, false, false, "enable map side speculative execution" );
    glyph = new OptionGlyph( asList( "-ER" ), "setReduceSpecExec", null, false, false, "enable reduce side speculative execution" );
    glyph = new OptionGlyph( asList( "-TS" ), "setTupleSpillThreshold", int.class, false, false, "tuple spill threshold, default 100,000" );
    glyph = new OptionGlyph( asList( "-DH" ), "setHadoopProperties", String.class, false, true, "optional Hadoop config job properties (can be used multiple times)" );
    glyph = new OptionGlyph( asList( "-MB" ), "setNumMappersPerBlock", int.class, false, false, "mappers per block (unused)" );
    glyph = new OptionGlyph( asList( "-RM" ), "setNumReducersPerMapper", int.class, false, false, "reducers per mapper (unused)" );
    glyph = new OptionGlyph( asList( "-I" ), "setInputRoot", String.class, true, false, "load input data path (generated data arrives here)" );
    glyph = new OptionGlyph( asList( "-O" ), "setOutputRoot", String.class, true, false, "output path for load results" );
    glyph = new OptionGlyph( asList( "-W" ), "setWorkingRoot", String.class, false, false, "input/output path for working files" );
    glyph = new OptionGlyph( asList( "-S" ), "setStatsRoot", String.class, false, false, "output path for job stats" );
    glyph = new OptionGlyph( asList( "-CWF" ), "setCleanWorkFiles", null, false, false, "clean work files" );
    glyph = new OptionGlyph( asList( "-CVMO" ), "setChildVMOptions", String.class, false, false, "child JVM options" );
    glyph = new OptionGlyph( asList( "-MXCF" ), "setMaxConcurrentFlows", int.class, false, false, "maximum concurrent flows" );
    glyph = new OptionGlyph( asList( "-MXCS" ), "setMaxConcurrentSteps", int.class, false, false, "maximum concurrent steps" );
    glyph = new OptionGlyph( asList( "-ALL" ), "setRunAllLoads", null, false, false, "run all available (non-discrete) loads" );
    glyph = new OptionGlyph( asList( "-LM" ), "setLocalMode", null, false, false, "use the local platform" );
    glyph = new OptionGlyph( asList( "-g", "--generate" ), "setDataGenerate", null, false, false, "generate test data" );
    glyph = new OptionGlyph( asList( "-gf", "--generate-num-files" ), "setDataNumFiles", int.class, false, false, "num files to create" );
    glyph = new OptionGlyph( asList( "-gs", "--generate-file-size" ), "setDataFileSizeMB", float.class, false, false, "size in MB of each file" );
    glyph = new OptionGlyph( asList( "-gmax", "--generate-max-words" ), "setDataMaxWords", int.class, false, false, "max words per line, inclusive" );
    glyph = new OptionGlyph( asList( "-gmin", "--generate-min-words" ), "setDataMinWords", int.class, false, false, "min words per line, inclusive" );
    glyph = new OptionGlyph( asList( "-gd", "--generate-word-delimiter" ), "setDataWordDelimiter", String.class, false, false, "delimiter for words" );
    glyph = new OptionGlyph( asList( "-gbf", "--generate-blocks-per-file" ), "setFillBlocksPerFile", int.class, false, false, "fill num blocks per file" );
    glyph = new OptionGlyph( asList( "-gfm", "--generate-files-per-mapper" ), "setFillFilesPerAvailMapper", int.class, false, false, "fill num files per available mapper" );
    glyph = new OptionGlyph( asList( "-gwm", "--generate-words-mean" ), "setDataMeanWords", float.class, false, false, "mean modifier [-1,1] of a normal distribution from dictionary" );
    glyph = new OptionGlyph( asList( "-gws", "--generate-words-stddev" ), "setDataStddevWords", float.class, false, false, "standard-deviation modifier (0,1) of a normal distribution from dictionary" );
    glyph = new OptionGlyph( asList( "-cd", "--consume" ), "setDataConsume", null, false, false, "consume test data" );
    glyph = new OptionGlyph( asList( "-s", "--certify-tests" ), "setCertifyTests", null, false, false, "run certification tests" );
    glyph = new OptionGlyph( asList( "-c", "--count-sort" ), "setCountSort", null, false, false, "run count sort load" );
    glyph = new OptionGlyph( asList( "-ss", "--staggered-sort" ), "setStaggeredSort", null, false, false, "run staggered compare sort load" );
    glyph = new OptionGlyph( asList( "-fg", "--full-group" ), "setFullTupleGroup", null, false, false, "run full tuple grouping load" );
    glyph = new OptionGlyph( asList( "-m", "--multi-join" ), "setMultiJoin", null, false, false, "run multi join load" );
    glyph = new OptionGlyph( asList( "-ij", "--inner-join" ), "setInnerJoin", null, false, false, "run inner join load" );
    glyph = new OptionGlyph( asList( "-oj", "--outer-join" ), "setOuterJoin", null, false, false, "run outer join load" );
    glyph = new OptionGlyph( asList( "-lj", "--left-join" ), "setLeftJoin", null, false, false, "run left join load" );
    glyph = new OptionGlyph( asList( "-rj", "--right-join" ), "setRightJoin", null, false, false, "run right join load" );
    glyph = new OptionGlyph( asList( "-p", "--pipeline" ), "setPipeline", null, false, false, "run pipeline load" );
    glyph = new OptionGlyph( asList( "-pm", "--pipeline-hash-modulo" ), "setHashModulo", int.class, false, false, "hash modulo for managing key distribution" );
    glyph = new OptionGlyph( asList( "-ca", "--chained-aggregate" ), "setChainedAggregate", null, false, false, "run chained aggregate load" );
    glyph = new OptionGlyph( asList( "-cf", "--chained-function" ), "setChainedFunction", null, false, false, "run chained function load" );
    glyph = new OptionGlyph( asList( "-wd", "--write-dot" ), "setWriteDotFile", null, false, false, "write DOT file" );
    glyph = new OptionGlyph( asList( "-pf", "--platform" ), "setPlatformName", String.class, false, false, "set platform" );
    }

  public void parseArgs( String[] args ) throws Exception
    {
    OptionSet opts = this.parser.parse( args );

    if( !opts.hasOptions() )
      {
      printUsage( false );
      System.exit( 1 );
      }
    else if( this.helpOption.attempt( opts ) )
      {
      printUsage( false );
      System.exit( 0 );
      }
    else if( this.markOption.attempt( opts ) )
      {
      printUsage( true );
      System.exit( 0 );
      }
    else
      {
      for( Object obj : optionList )
        {
        ( (OptionGlyph) obj ).attempt( opts );
        }

      if( !( this.runAllLoads || this.dataGenerate || this.dataConsume || this.countSort || this.certifyTests ||
        this.staggeredSort || this.fullTupleGroup || this.multiJoin || this.innerJoin || this.outerJoin ||
        this.leftJoin || this.rightJoin || this.pipeline || this.chainedAggregate || this.chainedFunction ) )
        {
        throw new Exception( "At least one flow must be selected, to run Load" );
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
    Version.printBanner();
    }

  private static void printLicense()
    {
    System.out.println( "This release is licensed under the Apache Software License 2.0.\n" );
    }

  public void printUsage( boolean genMarkdown )
    {
    if( genMarkdown )
      {
      System.out.println( "Load - Command Line Reference" );
      System.out.println( "=============================" );

      System.out.println( "    load [param] [param] ..." );
      System.out.println( "" );
      System.out.println( "At least one flow must be selected, to run Load" );
      System.out.println( "" );
      System.out.println( "<table>" );
      }
    else
      {
      System.out.println( "Usage:" );
      System.out.println( "" );
      System.out.println( "load [param] [param] ..." );
      System.out.println( "" );
      System.out.println( "At least one flow must be selected, to run Load" );
      System.out.println( "" );
      }

    try
      {
      if( genMarkdown )
        {
        for( Object obj : optionList )
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

    if( genMarkdown )
      {
      System.out.println( "</table>" );
      }

    System.out.println( "" );
    printCascadingVersion();
    System.out.println( "" );
    printLicense();
    }

  //////////////////////////////////////////////////////////////////////
  // option handlers

  public boolean isSinglelineStats()
    {
    return singlelineStats;
    }

  public void setSinglelineStats( boolean singlelineStats )
    {
    this.singlelineStats = singlelineStats;
    }

  public boolean isDebugLogging()
    {
    return debugLogging;
    }

  public void setDebugLogging( boolean debugLogging )
    {
    this.debugLogging = debugLogging;
    }

  public int getBlockSizeMB()
    {
    return blockSizeMB;
    }

  public void setBlockSizeMB( int blockSizeMB )
    {
    this.blockSizeMB = blockSizeMB;
    }

  public int getNumDefaultMappers()
    {
    return numDefaultMappers;
    }

  public void setNumDefaultMappers( int numDefaultMappers )
    {
    this.numDefaultMappers = numDefaultMappers;
    }

  public int getNumDefaultReducers()
    {
    return numDefaultReducers;
    }

  public void setNumDefaultReducers( int numDefaultReducers )
    {
    this.numDefaultReducers = numDefaultReducers;
    }

  public float getPercentMaxMappers()
    {
    return percentMaxMappers;
    }

  public void setPercentMaxMappers( float percentMaxMappers )
    {
    this.percentMaxMappers = percentMaxMappers;
    }

  public float getPercentMaxReducers()
    {
    return percentMaxReducers;
    }

  public void setPercentMaxReducers( float percentMaxReducers )
    {
    this.percentMaxReducers = percentMaxReducers;
    }

  public boolean isMapSpecExec()
    {
    return mapSpecExec;
    }

  public void setMapSpecExec( boolean mapSpecExec )
    {
    this.mapSpecExec = mapSpecExec;
    }

  public boolean isReduceSpecExec()
    {
    return reduceSpecExec;
    }

  public void setReduceSpecExec( boolean reduceSpecExec )
    {
    this.reduceSpecExec = reduceSpecExec;
    }

  public int getTupleSpillThreshold()
    {
    return tupleSpillThreshold;
    }

  public void setTupleSpillThreshold( int tupleSpillThreshold )
    {
    this.tupleSpillThreshold = tupleSpillThreshold;
    }

  public List<String> getHadoopProperties()
    {
    return hadoopProperties;
    }

  public void setHadoopProperties( String hadoopProperty )
    {
    this.hadoopProperties.add( hadoopProperty );
    }

  public int getNumMappersPerBlock()
    {
    return numMappersPerBlock;
    }

  public void setNumMappersPerBlock( int numMappersPerBlock )
    {
    this.numMappersPerBlock = numMappersPerBlock;
    }

  public int getNumReducersPerMapper()
    {
    return numReducersPerMapper;
    }

  public void setNumReducersPerMapper( int numReducersPerMapper )
    {
    this.numReducersPerMapper = numReducersPerMapper;
    }

  //////////////////////////////////

  public String getInputRoot()
    {
    return makePathDir( inputRoot );
    }

  public void setInputRoot( String inputRoot )
    {
    this.inputRoot = inputRoot;
    }

  public String getOutputRoot()
    {
    return makePathDir( outputRoot );
    }

  public void setOutputRoot( String outputRoot )
    {
    this.outputRoot = outputRoot;
    }

  public String getWorkingRoot()
    {
    return makePathDir( workingRoot );
    }

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

  public void setStatsRoot( String statsRoot )
    {
    this.statsRoot = statsRoot;
    }

  public boolean isCleanWorkFiles()
    {
    return cleanWorkFiles;
    }

  public void setCleanWorkFiles( boolean cleanWorkFiles )
    {
    this.cleanWorkFiles = cleanWorkFiles;
    }

  public String getChildVMOptions()
    {
    return childVMOptions;
    }

  public void setChildVMOptions( String childVMOptions )
    {
    this.childVMOptions = childVMOptions;
    }

  public int getMaxConcurrentFlows()
    {
    return maxConcurrentFlows;
    }

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

  public void setRunAllLoads( boolean runAllLoads )
    {
    this.runAllLoads = runAllLoads;
    }

//////////////////////////////////

  public boolean isDataGenerate()
    {
    return dataGenerate;
    }

  public void setDataGenerate( boolean dataGenerate )
    {
    this.dataGenerate = dataGenerate;
    }

  public int getDataNumFiles()
    {
    return dataNumFiles;
    }

  public void setDataNumFiles( int dataNumFiles )
    {
    this.dataNumFiles = dataNumFiles;
    }

  public float getDataFileSizeMB()
    {
    return dataFileSizeMB;
    }

  public void setDataFileSizeMB( float dataFileSizeMB )
    {
    this.dataFileSizeMB = dataFileSizeMB;
    }

  public int getDataMaxWords()
    {
    return dataMaxWords;
    }

  public void setDataMaxWords( int dataMaxWords )
    {
    this.dataMaxWords = dataMaxWords;
    }

  public int getDataMinWords()
    {
    return dataMinWords;
    }

  public void setDataMinWords( int dataMinWords )
    {
    this.dataMinWords = dataMinWords;
    }

  public String getDataWordDelimiter()
    {
    return dataWordDelimiter;
    }

  public void setDataWordDelimiter( String dataWordDelimiter )
    {
    this.dataWordDelimiter = dataWordDelimiter;
    }

  public int getFillBlocksPerFile()
    {
    return fillBlocksPerFile;
    }

  public void setFillBlocksPerFile( int fillBlocksPerFile )
    {
    this.fillBlocksPerFile = fillBlocksPerFile;
    }

  public int getFillFilesPerAvailMapper()
    {
    return fillFilesPerAvailMapper;
    }

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

  public void setDataConsume( boolean dataConsume )
    {
    this.dataConsume = dataConsume;
    }

  ////////////////////////////////////////
  public boolean isCertifyTests()
    {
    return certifyTests;
    }

  public void setCertifyTests( boolean certifyTests )
    {
    this.certifyTests = certifyTests;
    }

  public boolean isCountSort()
    {
    return countSort;
    }

  public void setCountSort( boolean countSort )
    {
    this.countSort = countSort;
    }

  public boolean isStaggeredSort()
    {
    return staggeredSort;
    }

  public void setStaggeredSort( boolean staggeredSort )
    {
    this.staggeredSort = staggeredSort;
    }

  public boolean isFullTupleGroup()
    {
    return fullTupleGroup;
    }

  public void setFullTupleGroup( boolean fullTupleGroup )
    {
    this.fullTupleGroup = fullTupleGroup;
    }

  ////////////////////////////////////////

  public boolean isMultiJoin()
    {
    return multiJoin;
    }

  public void setMultiJoin( boolean multiJoin )
    {
    this.multiJoin = multiJoin;
    }

  public boolean isInnerJoin()
    {
    return innerJoin;
    }

  public void setInnerJoin( boolean innerJoin )
    {
    this.innerJoin = innerJoin;
    }

  public boolean isOuterJoin()
    {
    return outerJoin;
    }

  public void setOuterJoin( boolean outerJoin )
    {
    this.outerJoin = outerJoin;
    }

  public boolean isLeftJoin()
    {
    return leftJoin;
    }

  public void setLeftJoin( boolean leftJoin )
    {
    this.leftJoin = leftJoin;
    }

  public boolean isRightJoin()
    {
    return rightJoin;
    }

  public void setRightJoin( boolean rightJoin )
    {
    this.rightJoin = rightJoin;
    }

  ////////////////////////////////////////

  public boolean isPipeline()
    {
    return pipeline;
    }

  public void setPipeline( boolean pipeline )
    {
    this.pipeline = pipeline;
    }

  public int getHashModulo()
    {
    return hashModulo;
    }

  public void setHashModulo( int hashModulo )
    {
    this.hashModulo = hashModulo;
    }

  public boolean isChainedAggregate()
    {
    return chainedAggregate;
    }

  public void setChainedAggregate( boolean chainedAggregate )
    {
    this.chainedAggregate = chainedAggregate;
    }

  public boolean isChainedFunction()
    {
    return chainedFunction;
    }

  public void setChainedFunction( boolean chainedFunction )
    {
    this.chainedFunction = chainedFunction;
    }

  public boolean isWriteDotFile()
    {
    return writeDotFile;
    }

  public void setWriteDotFile( boolean writeDotFile )
    {
    this.writeDotFile = writeDotFile;
    }

  public void setPlatformName( String platformName )
    {
    this.platformName = platformName;
    }

  public String getPlatformName()
    {
    return platformName;
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

    if( isCertifyTests() )
      {
      setDataGenerate( true );
      setCountSort( true );
      setMultiJoin( true );
      setPipeline( true );
      }
    CascadeLoadPlatform platform = new PlatformLoader().loadPlatform( platformName );
    if( numDefaultMappers == -1 && percentMaxMappers != 0 )
      numDefaultMappers = (int) ( platform.getMaxConcurrentMappers() * percentMaxMappers );

    if( numDefaultReducers == -1 && percentMaxReducers != 0 )
      numDefaultReducers = (int) ( platform.getMaxConcurrentReducers() * percentMaxReducers );

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
      dataNumFiles = platform.getMaxConcurrentMappers() * fillFilesPerAvailMapper;
      LOG.info( "using num files: " + dataNumFiles );
      }

    if( dataMaxWords < dataMinWords )
      {
      dataMaxWords = dataMinWords;
      LOG.info( "using max words: " + dataMaxWords );
      }
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "Options" );
    sb.append( "{parser=" ).append( parser );
    sb.append( ", option_list=" ).append( optionList );
    sb.append( ", singlelineStats=" ).append( singlelineStats );
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
    sb.append( ", certifyTests=" ).append( certifyTests );
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
    sb.append( ", help_option=" ).append( helpOption );
    sb.append( ", mark_option=" ).append( markOption );
    sb.append( ", platform=" ).append( platformName );
    sb.append( '}' );
    return sb.toString();
    }
  }
