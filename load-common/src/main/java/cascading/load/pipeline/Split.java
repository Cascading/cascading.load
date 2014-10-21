/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.pipeline;

import java.util.Map;
import java.util.Properties;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** Class Split sets up a simple copy operation to two outputs */
public class Split extends Load
  {
  public Split( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap lhs = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );
    Tap rhs = platform.newTap( platform.newTextLine(), getOutputPaths()[ 1 ], SinkMode.REPLACE );

    Pipe pipe = new Pipe( "copy" );

    Pipe lhsPipe = new Pipe( "lhs", pipe );
    Pipe rhsPipe = new Pipe( "rhs", pipe );

    Pipe[] tails = Pipe.pipes( lhsPipe, rhsPipe );
    Map<String, Tap> sinks = Cascades.tapsMap( tails, Tap.taps( lhs, rhs ) );

    return platform.newFlowConnector( properties ).connect( "split", source, sinks, lhsPipe, rhsPipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "lhs", options.getOutputRoot() + "rhs"};
    }
  }