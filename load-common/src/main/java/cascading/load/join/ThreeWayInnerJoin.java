/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.join;

import java.util.Map;
import java.util.Properties;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.operation.filter.Sample;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Class OnlyInnerJoin uses the test corpus and performs both a split of of all the words into tuples and uniques all the
 * words, and then finally joins the two streams as an inner join.
 */
public class ThreeWayInnerJoin extends Load
  {
  public ThreeWayInnerJoin( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line", String.class ) ), getInputPaths()[ 0 ] );
    Tap innerSink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    int dataMaxWords = options.getDataMaxWords();

    Pipe uniques = new Pipe( "unique" );

    uniques = new Each( uniques, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word", String.class ), "\\s" ) );

    uniques = new Unique( uniques, new Fields( "word" ) );

    uniques = new Each( uniques, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe lhs = new Pipe( "lhs" );

    lhs = new Each( lhs, new Fields( "line" ), new RegexSplitter( Fields.size( dataMaxWords, String.class ), "\\s" ) );

    lhs = new Each( lhs, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe rhs = new Pipe( "rhs" );

    rhs = new Each( rhs, new Fields( "line" ), new RegexSplitter( Fields.size( dataMaxWords, String.class ), "\\s" ) );

    rhs = new Each( rhs, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe[] pipes = {lhs, uniques, rhs};
    Fields[] fields = {new Fields( 0 ), new Fields( "word" ), new Fields( 0 )};

    Pipe inner = new CoGroup( "inner", pipes, fields, Fields.size( dataMaxWords * 2 + 1, String.class ), new InnerJoin() );

    Pipe[] heads = Pipe.pipes( uniques, lhs, rhs );
    Map<String, Tap> sources = Cascades.tapsMap( heads, Tap.taps( source, source, source ) );

    return platform.newFlowConnector( properties ).connect( "3-way-join", sources, innerSink, inner );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "threewayinner"};
    }
  }