/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.load.pathological;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.log4j.Logger;

/**
 *
 */
public abstract class BreakingLoad extends Load
  {

  protected final Logger LOG = Logger.getLogger( getClass() );

  public BreakingLoad( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    String loadName = getLoadName();
    LOG.error( "creating flow for " +  loadName);
    Tap source = getSource();
    Tap sink = getSink();
    Pipe pipe = getPipe();
    return platform.newFlowConnector( properties ).connect( getLoadName(), source, sink, pipe);
    }

  protected abstract Pipe getPipe();

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + getLoadName()};
    }

  protected Tap getSource()
    {
    return platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    }

  protected Tap getSink()
    {
    return platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );
    }

  protected String getLoadName()
    {
    return getClass().getSimpleName().replace( "CrashOn", "" ) + "-RunToDestruction";
    }

  public static BreakingLoad[] breakingLoads( Options options, Properties properties )
    {
    return new BreakingLoad[]{
      new CrashOnAggregation( options, properties ),
      new CrashOnBuffer( options, properties ),
      new CrashOnFilter( options, properties ),
      new CrashOnFunction( options, properties ),
      new CrashOnMemory( options, properties ),
      new CrashOnStackOverflow( options, properties )};
    }

  }
