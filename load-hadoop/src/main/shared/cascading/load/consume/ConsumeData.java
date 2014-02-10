/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load.consume;

import java.util.Properties;

import cascading.flow.Flow;
import load.Options;
import load.common.Load;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** Class ConsumeData loads a test corpus of random words. */
public class ConsumeData extends Load
  {
  public ConsumeData( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap sink = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe pipe = new Pipe( "load-consumer" );

    ExpressionFilter filter = new ExpressionFilter( "true", Boolean.TYPE );

    pipe = new Each( pipe, new Fields( "line" ), filter );

    return platform.newFlowConnector( properties ).connect( "consume-data", source, sink, pipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getInputRoot() + "consumedata"};
    }
  }