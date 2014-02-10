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
import java.util.Properties;

import cascading.flow.FlowProps;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.load.Options;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.collect.SpillableProps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Base class for all hadoop platform implementations.
 */
public abstract class BaseHadoopCascadePlatform  implements CascadeLoadPlatform
  {

  private static final Logger LOG = Logger.getLogger( BaseHadoopCascadePlatform.class );

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
  public TupleEntryCollector newTupleEntryCollector( Tap tap ) throws IOException
    {
    return tap.openForWrite( new HadoopFlowProcess() );
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
    JobConf jobConf = new JobConf();
    for( int i = 0; i < numberOfFiles; i++ )
      {
      jobConf.setInt( "mapred.task.partition", i );
      TupleEntryCollector writer = tap.openForWrite( new HadoopFlowProcess( jobConf ) );
      writer.add( data );
      writer.close();
      }
    }

  @Override
  public Properties buildPlatformProperties( Options options )
    {
    Properties properties = new Properties(  );
    properties.setProperty( SpillableProps.LIST_THRESHOLD, Integer.toString( options.getTupleSpillThreshold() ) );

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
    if( HadoopUtil.hasNativeZlib() )
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

    return properties;
    }

  @Override
  public void cleanDirectories( String... paths ) throws IOException
    {
    FileSystem fs = FileSystem.get( new JobConf() );
    for ( String path : paths)
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
  }
