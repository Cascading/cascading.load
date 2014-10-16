/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.io.ByteArrayOutputStream;
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
import cascading.load.pathological.BreakingLoad;
import cascading.load.pathological.PathologicalOnlyInnerJoin;
import cascading.load.pipeline.ChainedAggregate;
import cascading.load.pipeline.ChainedFunction;
import cascading.load.pipeline.Pipeline;
import cascading.load.platform.CascadingLoadPlatform;
import cascading.load.platform.PlatformLoader;
import cascading.load.util.StatsPrinter;
import cascading.operation.DebugLevel;
import cascading.property.AppProps;
import cascading.stats.CascadeStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 */
public class Main
  {
  private static final Logger LOG = Logger.getLogger( Options.class );

  private Options options;
  private CascadingLoadPlatform platform;
  private String statsRoot;

  public static void main( String[] args ) throws Exception
    {
    new Main( args ).execute();
    }

  public Main( String[] args ) throws IOException
    {
    options = new Options();

    initOptions( args, options );

    platform = new PlatformLoader().loadPlatform( options.getPlatformName() );
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

    if( options.isPathologicalInnerJoin() )
      flows.add( new PathologicalOnlyInnerJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isBreakingLoads() )
      for( BreakingLoad breakingLoad : BreakingLoad.breakingLoads( options, getDefaultProperties() ) )
        flows.add( breakingLoad.createFlow() );

    if( options.isWriteDotFile() )
      {
      for( Flow flow : flows )
        {
        String file_name = flow.getName() + ".dot";
        LOG.info( "DOT file: " + file_name );
        flow.writeDOT( file_name );
        }
      }

    Cascade cascade = new CascadeConnector( getDefaultProperties() ).connect( "load", flows );

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
      platform.cleanDirectories( options.getInputRoot(), options.getWorkingRoot(), options.getOutputRoot() );
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

    new StatsPrinter( platform ).printStats( writer, stats, options.isSinglelineStats() );

    if( options.hasStatsRoot() )
      {
      String[] lines = outputStream.toString().split( "\n" );

      String statsRoot = getFullStatsRoot();

      Tap statsTap = platform.newTap( platform.newTextLine(), statsRoot, SinkMode.REPLACE );

      TupleEntryCollector tapWriter = platform.newTupleEntryCollector( statsTap );

      for( String line : lines )
        tapWriter.add( new Tuple( line ) );

      tapWriter.close();
      }
    }

  public String getFullStatsRoot()
    {
    if( statsRoot != null )
      return statsRoot;

    statsRoot = options.getStatsRoot();

    statsRoot += String.format( "%s-%d", platform.getClass().getSimpleName(), System.currentTimeMillis() );

    return statsRoot;
    }

  protected Properties getDefaultProperties() throws IOException
    {
    Properties properties = platform.buildPlatformProperties( options );

    if( options.getAppName() != null )
      AppProps.setApplicationName( properties, options.getAppName() );

    if( options.getTags() != null )
      AppProps.addApplicationTag( properties, options.getTags() );

    AppProps.addApplicationFramework( properties, "Load" );

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
