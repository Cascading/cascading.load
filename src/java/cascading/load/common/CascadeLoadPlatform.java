/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.load.common;

import java.io.IOException;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.load.Options;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

/**
 * Provides access to Cascading platform (local or hadoop) specific implementation objects.
 * <p/>
 * Only those aspects used by cascading.load are supported.
 * <p/>
 * Not thread safe.
 */
public abstract class CascadeLoadPlatform
  {
  private static CascadeLoadPlatform hadoopCascadePlatform = null;
  private static CascadeLoadPlatform localCascadePlatform = null;

  private static final class HadoopCascadePlatform extends CascadeLoadPlatform
    {
    protected HadoopCascadePlatform()
      {
      // has no state
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
    public FlowConnector newFlowConnector()
      {
      return new HadoopFlowConnector();
      }

    @Override
    public FlowConnector newFlowConnector( Map<Object, Object> properties )
      {
      return new HadoopFlowConnector( properties );
      }
    }

  private static final class LocalCascadePlatform extends CascadeLoadPlatform
    {
    protected LocalCascadePlatform()
      {
      // has no state
      }

    @Override
    public Tap newTap( Scheme scheme, String stringPath )
      {
      return new FileTap( scheme, stringPath );
      }

    @Override
    public Tap newTap( Scheme scheme, String stringPath, SinkMode sinkMode )
      {
      return new FileTap( scheme, stringPath, sinkMode );
      }

    @Override
    public TupleEntryCollector newTupleEntryCollector( Tap tap ) throws IOException
      {
      return tap.openForWrite( new LocalFlowProcess() );
      }

    @Override
    public Scheme newTextLine()
      {
      return new cascading.scheme.local.TextLine();
      }

    @Override
    public Scheme newTextLine( Fields sourceFields )
      {
      return new cascading.scheme.local.TextLine( sourceFields );
      }

    @Override
    public Scheme newTextLine( Fields sourceFields, Fields sinkFields )
      {
      return new cascading.scheme.local.TextLine( sourceFields, sinkFields );
      }

    @Override
    public FlowConnector newFlowConnector()
      {
      return new LocalFlowConnector();
      }

    @Override
    public FlowConnector newFlowConnector( Map<Object, Object> properties )
      {
      return new LocalFlowConnector( properties );
      }
    }

  public static CascadeLoadPlatform getPlatform( Options options )
    {
    if( options.isLocalMode() )
      {
      if( localCascadePlatform == null )
        localCascadePlatform = new LocalCascadePlatform();
      return localCascadePlatform;
      }
    else
      {
      if( hadoopCascadePlatform == null )
        hadoopCascadePlatform = new HadoopCascadePlatform();
      return hadoopCascadePlatform;
      }
    }

  public abstract Tap newTap( Scheme scheme, String stringPath );

  public abstract Tap newTap( Scheme scheme, String stringPath, SinkMode sinkMode );

  public abstract TupleEntryCollector newTupleEntryCollector( Tap tap ) throws IOException;

  public abstract Scheme newTextLine();

  public abstract Scheme newTextLine( Fields sourceFields );

  public abstract Scheme newTextLine( Fields sourceFields, Fields sinkFields );

  public abstract FlowConnector newFlowConnector();

  public abstract FlowConnector newFlowConnector( Map<Object, Object> properties );
  }
