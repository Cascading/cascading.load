/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.load.platform;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingException;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProps;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.flow.tez.Hadoop2TezFlowProcess;
import cascading.load.Options;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.collect.SpillableProps;
import cascading.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

/**
 * Implementation of CascadeLoadPlatform for hadoop2-tez.
 */
public class Hadoop2TezCascadingPlatform implements CascadingLoadPlatform
  {
  private static final Logger LOG = Logger.getLogger( Hadoop2TezCascadingPlatform.class );

  @Override
  public String getName()
    {
    return "hadoop2-tez";
    }

  @Override
  public FlowConnector newFlowConnector()
    {
    return new Hadoop2TezFlowConnector();
    }

  @Override
  public FlowConnector newFlowConnector( Map<Object, Object> properties )
    {
    return new Hadoop2TezFlowConnector( properties );
    }

  @Override
  public Tap newTap( Scheme scheme, String stringPath )
    {
    return new Hfs( scheme, stringPath );
    }

  @Override
  public Tap newTap( Scheme scheme, String stringPath, SinkMode sinkMode )
    {
    return new Hfs( scheme, stringPath, sinkMode );
    }

  @Override
  public String[] getChildrenOf( String path )
    {
    try
      {
      return new Hfs( new TextLine(), path ).getChildIdentifiers( new JobConf() );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    }

  @Override
  public TupleEntryCollector newTupleEntryCollector( Tap tap ) throws IOException
    {
    return tap.openForWrite( new Hadoop2TezFlowProcess() );
    }

  @Override
  public Scheme newTextLine()
    {
    return new cascading.scheme.hadoop.TextLine();
    }

  @Override
  public Scheme newTextLine( Fields sourceFields )
    {
    return new cascading.scheme.hadoop.TextLine( sourceFields );
    }

  @Override
  public Scheme newTextLine( Fields sourceFields, Fields sinkFields )
    {
    return new cascading.scheme.hadoop.TextLine( sourceFields, sinkFields );
    }

  @Override
  public void writeDictionaryData( Tuple data, String path, int numberOfFiles ) throws IOException
    {
    Tap tap = newTap( newTextLine(), path );
    TezConfiguration configuration = new TezConfiguration();

    for( int i = 0; i < numberOfFiles; i++ )
      {
      configuration.setInt( "mapred.task.partition", i );
      TupleEntryCollector writer = tap.openForWrite( new Hadoop2TezFlowProcess( configuration ) );
      writer.add( data );
      writer.close();
      }
    }

  @Override
  public Properties buildPlatformProperties( Options options )
    {
    Properties properties = new Properties();
    properties.setProperty( SpillableProps.LIST_THRESHOLD, Integer.toString( options.getTupleSpillThreshold() ) );

//    properties.setProperty( "mapred.output.compress", "true" );
    properties.setProperty( "mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );
    properties.setProperty( "mapred.output.compression.type", "BLOCK" );
    properties.setProperty( "mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec" );
    properties.setProperty( "mapreduce.output.fileoutputformat.compress.type", "BLOCK" );

//    properties.setProperty( YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, "-1" );
//    properties.setProperty( TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS, "true" );

//    properties.setProperty( TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, "false" ); // disabled to bypass deadlock

    // unsupported currently
//    properties.setProperty( TezConfiguration.TEZ_AM_SESSION_MODE, "true" ); // enable sessions

    properties.setProperty( TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, "true" );
    properties.setProperty( TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, "org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService" );
    properties.setProperty( YarnConfiguration.TIMELINE_SERVICE_ENABLED, "true" );

    if( HadoopUtil.hasNativeZlib() )
      properties.setProperty( TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, "org.apache.hadoop.io.compress.GzipCodec" );

    // -XX:+UseParallelOldGC -XX:ParallelGCThreads=1
    if( !Util.isEmpty( options.getChildVMOptions() ) )
      properties.setProperty( TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, options.getChildVMOptions() );

    if( options.getNumDefaultMappers() != -1 )
      LOG.warn( "getNumDefaultMappers not honored" );

    if( options.getNumDefaultReducers() != -1 )
      properties.setProperty( FlowRuntimeProps.GATHER_PARTITIONS, Integer.toString( options.getNumDefaultReducers() ) );

//    properties.setProperty( "mapred.map.tasks.speculative.execution", options.isMapSpecExec() ? "true" : "false" );
//    properties.setProperty( "mapred.reduce.tasks.speculative.execution", options.isReduceSpecExec() ? "true" : "false" );

    properties.setProperty( "dfs.blocksize", Long.toString( options.getBlockSizeMB() * 1024 * 1024 ) );

    // need to try and detect if native codecs are loaded, if so, use gzip
    if( HadoopUtil.hasNativeZlib() )
      {
      properties.setProperty( "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );
      properties.setProperty( "mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec" );
      LOG.info( "using native codec for gzip" );
      }
    else
      {
      properties.setProperty( "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec" );
      properties.setProperty( "mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec" );
      LOG.info( "native codec not found" );
      }

    for( String property : options.getHadoopProperties() )
      {
      String[] split = property.split( "=" );

      if( split.length == 2 )
        properties.setProperty( split[ 0 ], split[ 1 ] );
      else
        properties.setProperty( split[ 0 ], null );
      }

    if( options.getMaxConcurrentSteps() != -1 )
      FlowProps.setMaxConcurrentSteps( properties, options.getMaxConcurrentSteps() );

    return properties;
    }

  @Override
  public void cleanDirectories( String... paths ) throws IOException
    {
    FileSystem fs = FileSystem.get( new TezConfiguration() );
    for( String path : paths )
      fs.delete( new Path( path ), true );
    }

  @Override
  public int getMaxConcurrentMappers()
    {
    return HadoopUtil.getMaxConcurrentMappers();
    }

  @Override
  public int getMaxConcurrentReducers()
    {
    return HadoopUtil.getMaxConcurrentReducers();
    }

  @Override
  public long getCPUMillis( CascadingStats cascadingStats )
    {
    return cascadingStats.getCounterValue( TaskCounter.CPU_MILLISECONDS );
    }
  }