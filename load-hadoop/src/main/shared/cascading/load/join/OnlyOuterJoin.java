/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load.join;

import java.util.Map;
import java.util.Properties;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import load.Options;
import load.common.Load;
import cascading.operation.filter.Sample;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.OuterJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Class OnlyOuterJoin uses the test corpus and performs both a split of of all the words into tuples and uniques all the
 * words, and then finally joins the two streams as an outer join.
 */
public class OnlyOuterJoin extends Load
  {
  public OnlyOuterJoin( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap outerSink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe uniques = new Pipe( "unique" );

    uniques = new Each( uniques, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word" ), "\\s" ) );

    uniques = new Unique( uniques, new Fields( "word" ) );

    uniques = new Each( uniques, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe fielded = new Pipe( "fielded" );

    fielded = new Each( fielded, new Fields( "line" ), new RegexSplitter( Fields.size( options.getDataMaxWords() ), "\\s" ) );

    fielded = new Each( fielded, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe outer = new CoGroup( "outer", fielded, new Fields( 0 ), uniques, new Fields( "word" ), new OuterJoin() );

    Pipe[] heads = Pipe.pipes( uniques, fielded );
    Map<String, Tap> sources = Cascades.tapsMap( heads, Tap.taps( source, source ) );

    return platform.newFlowConnector( properties ).connect( "outer-join", sources, outerSink, outer );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "onlyouter"};
    }
  }