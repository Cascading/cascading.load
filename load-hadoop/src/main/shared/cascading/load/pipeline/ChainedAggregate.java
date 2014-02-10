/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load.pipeline;

import java.util.Properties;

import cascading.flow.Flow;
import load.Options;
import load.common.Load;
import cascading.operation.Function;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;


/**
 * Class ChainedAggregate sets up a simple pipeline of operations to test the hand off between operations
 */
public class ChainedAggregate extends Load
  {
  public ChainedAggregate( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap sink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe pipe = new Pipe( "chainedaggregate" );

    Function function = new ExpressionFunction( new Fields( "count" ), "line.split( \"\\s\").length", String.class );
    pipe = new Each( pipe, new Fields( "line" ), function, Fields.ALL );

    int modulo = 1000000;

    if( options.getHashModulo() != -1 )
      modulo = options.getHashModulo();

    pipe = new Each( pipe, new Fields( "line" ), new ExpressionFunction( new Fields( "hash" ), "line.hashCode() % " + modulo, String.class ), Fields.ALL ); // want some collisions

    pipe = new GroupBy( pipe, new Fields( "hash" ) );

    for( int i = 0; i < 50; i++ )
      pipe = new Every( pipe, new Fields( "count" ), new Sum( new Fields( "sum" + ( i + 1 ) ) ) );

    return platform.newFlowConnector( properties ).connect( "chainedaggregate", source, sink, pipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "chainedaggregate"};
    }
  }