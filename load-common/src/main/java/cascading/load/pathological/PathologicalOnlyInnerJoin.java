/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.pathological;

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
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Pathological version of OnlyInnerJoin that does not unique the words so that it can spill.
 *
 * Set -TS ( cascading.spill.list.threshold ) to some low number ( default is 100000 ) to induce CoGroup spillage.
 */
public class PathologicalOnlyInnerJoin extends Load
  {
  public PathologicalOnlyInnerJoin( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap innerSink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe words = new Pipe( "words" );

    words = new Each( words, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word" ), "\\s" ) );

    Pipe fielded = new Pipe( "fielded" );

    fielded = new Each( fielded, new Fields( "line" ), new RegexSplitter( Fields.size( options.getDataMaxWords() ), "\\s" ) );

    fielded = new Each( fielded, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe inner = new CoGroup( "inner", fielded, new Fields( 0 ), words, new Fields( "word" ), new InnerJoin() );

    Pipe[] heads = Pipe.pipes( words, fielded );
    Map<String, Tap> sources = Cascades.tapsMap( heads, Tap.taps( source, source ) );

    return platform.newFlowConnector( properties ).connect( PathologicalOnlyInnerJoin.class.getSimpleName(), sources, innerSink, inner );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "onlyinner"};
    }
  }
