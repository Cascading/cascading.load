/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.common;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.planner.FlowPlanner;
import cascading.load.Options;
import cascading.load.platform.CascadingLoadPlatform;
import cascading.load.platform.PlatformLoader;
import cascading.util.Util;

/**
 *
 */
public abstract class Load
  {
  protected Options options;
  protected Properties properties;
  protected CascadingLoadPlatform platform;

  public Load( Options options, Properties properties )
    {
    this.options = options;
    this.properties = new Properties( properties );
//    this.properties = properties;
    this.platform = new PlatformLoader().loadPlatform( options.getPlatformName() );

    if( options.isWriteTraceFiles() )
      {
      String path = "trace/" + getClass().getSimpleName();

      properties.setProperty( FlowPlanner.TRACE_PLAN_TRANSFORM_PATH, Util.join( "/", path, "planner" ) );
      properties.setProperty( FlowPlanner.TRACE_PLAN_PATH, path );
      properties.setProperty( FlowPlanner.TRACE_STATS_PATH, path );
      }
    }

  public abstract Flow createFlow() throws Exception;

  public abstract String[] getInputPaths();

  public abstract String[] getOutputPaths();
  }
