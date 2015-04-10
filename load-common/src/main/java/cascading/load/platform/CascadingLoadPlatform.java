/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.platform;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.load.Options;
import cascading.scheme.Scheme;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public interface CascadingLoadPlatform
  {
  public String getName();

  String[] getChildrenOf( String path );

  public Tap newTap( Scheme scheme, String stringPath );

  public Tap newTap( Scheme scheme, String stringPath, SinkMode sinkMode );

  public TupleEntryCollector newTupleEntryCollector( Tap tap ) throws IOException;

  public Scheme newTextLine();

  public Scheme newTextLine( Fields sourceFields );

  public Scheme newTextLine( Fields sourceFields, Fields sinkFields );

  public FlowConnector newFlowConnector();

  public FlowConnector newFlowConnector( Map<Object, Object> properties );

  public void writeDictionaryData( Tuple data, String path, int numberOfFiles ) throws IOException;

  public Properties buildPlatformProperties( Options options );

  public void cleanDirectories( String... paths ) throws IOException;

  public int getMaxConcurrentMappers();

  public int getMaxConcurrentReducers();

  long getCPUMillis( CascadingStats cascadingStats );
  }
