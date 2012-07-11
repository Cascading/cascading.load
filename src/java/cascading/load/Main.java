/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.CascadeProps;
import cascading.flow.Flow;
import cascading.flow.FlowConnectorProps;
import cascading.flow.FlowProps;
import cascading.load.common.CascadeLoadPlatform;
import cascading.load.consume.ConsumeData;
import cascading.load.countsort.CountSort;
import cascading.load.countsort.FullTupleGroup;
import cascading.load.countsort.StaggeredSort;
import cascading.load.generate.GenerateData;
import cascading.load.join.MultiJoin;
import cascading.load.join.OnlyInnerJoin;
import cascading.load.join.OnlyLeftJoin;
import cascading.load.join.OnlyOuterJoin;
import cascading.load.join.OnlyRightJoin;
import cascading.load.pipeline.ChainedAggregate;
import cascading.load.pipeline.ChainedFunction;
import cascading.load.pipeline.Pipeline;
import cascading.load.util.StatsPrinter;
import cascading.load.util.Util;
import cascading.operation.DebugLevel;
import cascading.property.AppProps;
import cascading.stats.CascadeStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.collect.SpillableTupleList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 */
public class Main
  {
  private static final Logger LOG = Logger.getLogger( Options.class );

  private Options options;
  private CascadeLoadPlatform platform;

  public static void main( String[] args ) throws Exception
    {
    new Main( args ).execute();
    }

  public Main( String[] args ) throws IOException
    {
    options = new Options();

    initOptions( args, options );

    platform = CascadeLoadPlatform.getPlatform( options );
    }

  public boolean execute() throws Exception
    {
    List<Flow> flows = new ArrayList<Flow>();

    // This is unweildy
    // todo: use reflection (?) w/ table of load classes instead

    if( options.isDataGenerate() )
      flows.add( new GenerateData( options, getDefaultProperties() ).createFlow() );

    if( options.isDataConsume() )
      flows.add( new ConsumeData( options, getDefaultProperties() ).createFlow() );

    if( options.isCountSort() )
      flows.add( new CountSort( options, getDefaultProperties() ).createFlow() );

    if( options.isMultiJoin() )
      flows.add( new MultiJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isPipeline() )
      flows.add( new Pipeline( options, getDefaultProperties() ).createFlow() );

    if( options.isStaggeredSort() )
      flows.add( new StaggeredSort( options, getDefaultProperties() ).createFlow() );

    if( options.isFullTupleGroup() )
      flows.add( new FullTupleGroup( options, getDefaultProperties() ).createFlow() );

    if( options.isChainedAggregate() )
      flows.add( new ChainedAggregate( options, getDefaultProperties() ).createFlow() );

    if( options.isChainedFunction() )
      flows.add( new ChainedFunction( options, getDefaultProperties() ).createFlow() );

    if( options.isLeftJoin() )
      flows.add( new OnlyLeftJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isRightJoin() )
      flows.add( new OnlyRightJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isInnerJoin() )
      flows.add( new OnlyInnerJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isOuterJoin() )
      flows.add( new OnlyOuterJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isWriteDotFile() )
      for( Flow flow : flows )
        {
        String file_name = flow.getName() + ".dot";
        LOG.info( "DOT file: " + file_name );
        flow.writeDOT( file_name );
        }

    Cascade cascade = new CascadeConnector( getDefaultProperties() ).connect( flows.toArray( new Flow[ 0 ] ) );

    CascadeStats stats = cascade.getCascadeStats();

    try
      {
      cascade.complete();
      }
    catch( Exception exception )
      {
      LOG.error( "failed running cascade ", exception );

      return false;
      }

    printSummary( stats );

    if( options.isCleanWorkFiles() )
      cleanWorkFiles();

    return true;
    }

  private void cleanWorkFiles()
    {
    // - Ask each phase to delete work files:
    //   - Save every work sink Tap & then tap.deletePath( new JobConf() )
    //   - Save list of all Load instances.
    //   - Add method that stores sink Taps to Load baseclass - noteSinkTap say.
    //   noteSinkTap will save the ref when a saving flag is set. value of
    //   this flag set via options.isCleanWorkFiles() when Load instance
    //   created: also have set/get? have noteSinkTap w/ bool param?
    //   - Each Load instance calls noteSinkTap accordingly
    //   - Add method deleteSinkTaps to Load.

    try
      {
      LOG.info( "cleaning work files" );
      if( options.isLocalMode() )
        {
        Util.deleteRecursive( new File( options.getInputRoot() ) );
        Util.deleteRecursive( new File( options.getWorkingRoot() ) );
        Util.deleteRecursive( new File( options.getOutputRoot() ) );
        }
      else
        {
        FileSystem fs = FileSystem.get( new JobConf() );
        fs.delete( new Path( options.getInputRoot() ), true );
        fs.delete( new Path( options.getWorkingRoot() ), true );
        fs.delete( new Path( options.getOutputRoot() ), true );
        }
      }
    catch( Exception exception )
      {
      LOG.error( "failed cleaning work files ", exception );
      }
    }

  private void printSummary( CascadeStats stats ) throws IOException
    {
    stats.captureDetail();

    OutputStream outputStream = options.hasStatsRoot() ? new ByteArrayOutputStream() : System.out;
    PrintWriter writer = new PrintWriter( outputStream );

    writer.println( options );

    StatsPrinter.printCascadeStats( writer, stats, options.isSinglelineStats() );

    if( options.hasStatsRoot() )
      {
      String[] lines = outputStream.toString().split( "\n" );

      Tap statsTap = platform.newTap( platform.newTextLine(), options.getStatsRoot(), SinkMode.REPLACE );

      TupleEntryCollector tapWriter = platform.newTupleEntryCollector( statsTap );

      for( String line : lines )
        tapWriter.add( new Tuple( line ) );

      tapWriter.close();
      }
    }

  protected Properties getDefaultProperties() throws IOException
    {
    Properties properties = new Properties();

    if( options.isDebugLogging() )
      {
      Logger.getLogger( "cascading" ).setLevel( Level.DEBUG );
      Logger.getLogger( "load" ).setLevel( Level.DEBUG );
      properties.put( "log4j.logger", "cascading=DEBUG,load=DEBUG" );
      }
    else
      {
      Logger.getLogger( "cascading" ).setLevel( Level.INFO );
      Logger.getLogger( "load" ).setLevel( Level.INFO );
      properties.put( "log4j.logger", "cascading=INFO,load=INFO" );
      }

    if( options.isDebugLogging() )
      FlowConnectorProps.setDebugLevel( properties, DebugLevel.VERBOSE );
    else
      FlowConnectorProps.setDebugLevel( properties, DebugLevel.NONE );

    if( !options.isLocalMode() )
      {
      properties.setProperty( SpillableTupleList.SPILL_THRESHOLD, Integer.toString( options.getTupleSpillThreshold() ) );

//    properties.setProperty( "mapred.output.compress", "true" );
      properties.setProperty( "mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );
      properties.setProperty( "mapred.output.compression.type", "BLOCK" );
      properties.setProperty( "mapred.compress.map.output", "true" );

      // -XX:+UseParallelOldGC -XX:ParallelGCThreads=1
      properties.setProperty( "mapred.child.java.opts", "-server " + options.getChildVMOptions() );

      if( options.getNumDefaultMappers() != -1 )
        properties.setProperty( "mapred.map.tasks", Integer.toString( options.getNumDefaultMappers() ) );

      if( options.getNumDefaultReducers() != -1 )
        properties.setProperty( "mapred.reduce.tasks", Integer.toString( options.getNumDefaultReducers() ) );

      properties.setProperty( "mapred.map.tasks.speculative.execution", options.isMapSpecExec() ? "true" : "false" );
      properties.setProperty( "mapred.reduce.tasks.speculative.execution", options.isReduceSpecExec() ? "true" : "false" );

      properties.setProperty( "dfs.block.size", Long.toString( options.getBlockSizeMB() * 1024 * 1024 ) );

      // need to try and detect if native codecs are loaded, if so, use gzip
      if( Util.hasNativeZlib() )
        {
        properties.setProperty( "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );
        LOG.info( "using native codec for gzip" );
        }
      else
        {
        properties.setProperty( "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec" );
        LOG.info( "native codec not found" );
        }

      for( String property : options.getHadoopProperties() )
        {
        String[] split = property.split( "=" );
        properties.setProperty( split[ 0 ], split[ 1 ] );
        }

      if( options.getMaxConcurrentSteps() != -1 )
        FlowProps.setMaxConcurrentSteps( properties, options.getMaxConcurrentSteps() );
      }

    if( options.getMaxConcurrentFlows() != -1 )
      CascadeProps.setMaxConcurrentFlows( properties, options.getMaxConcurrentFlows() );

    AppProps.setApplicationJarClass( properties, Main.class );

    return properties;
    }

  protected static Options initOptions( String[] args, Options options ) throws IOException
    {
    try
      {
      options.parseArgs( args );
      }
    catch( Exception e )
      {
      System.out.print( "options error: " );
      System.out.println( e.getMessage() );
      System.out.println( "" );

      options.printUsage( false );
      System.exit( 1 );
      }

    options.prepare();
    LOG.info( options );

    return options;
    }
  }
