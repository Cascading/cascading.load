/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.pipeline;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.load.Options;
import cascading.load.common.Load;


/** Class Pipeline sets up a simple pipeline of operations to test the hand off between operations * */
public class Pipeline extends Load
  {
  public Pipeline( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap sink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipeline" );

    Function function = new ExpressionFunction( new Fields( "count" ), "line.split( \"\\\\s\" ).length", String.class );
    pipe = new Each( pipe, new Fields( "line" ), function, Fields.ALL );

    for( int i = 0; i < 50; i++ )
      {
      pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "count" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "line" ), new Identity(), Fields.REPLACE );
      pipe = new Each( pipe, new Fields( "count" ), new Identity(), Fields.REPLACE );
      pipe = new Each( pipe, new Fields( "line", "count" ), new Identity() );
      pipe = new Each( pipe, new Fields( "line", "count" ), new Identity( new Fields( "line2", "count2" ) ), new Fields( "line", "count2" ) );
      pipe = new Each( pipe, new Fields( "count2" ), new Identity( new Fields( "count" ) ), new Fields( "line", "count" ) );
      }

    int modulo = 1000000;

    if( options.getHashModulo() != -1 )
      modulo = options.getHashModulo();

    pipe = new Each( pipe, new Fields( "line" ), new ExpressionFunction( new Fields( "hash" ), "line.hashCode() % " + modulo, String.class ), Fields.ALL ); // want some collisions

    pipe = new GroupBy( pipe, new Fields( "hash" ) );

    for( int i = 0; i < 50; i++ )
      pipe = new Every( pipe, new Fields( "count" ), new Sum( new Fields( "sum" + ( i + 1 ) ) ) );

    for( int i = 0; i < 50; i++ )
      {
      pipe = new Each( pipe, new Fields( "hash" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "sum1" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "hash", "sum1" ), new Identity(), Fields.SWAP );
      }

    return platform.newFlowConnector( properties ).connect( "pipeline", source, sink, pipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "pipeline"};
    }
  }