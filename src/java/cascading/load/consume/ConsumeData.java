/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.consume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.load.util.Util;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.Limit;
import cascading.operation.filter.Sample;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.mapred.JobConf;

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
    Tap sink = platform.newTap( platform.newTextLine( new Fields( "line" ) ), options.getOutputRoot() );

    Pipe pipe = new Pipe( "load-consumer" );

    //ExpressionFilter filter = new ExpressionFilter( "true", Boolean.TYPE );
    Limit filter = new Limit( 1L );

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
    return new String[]{options.getInputRoot()};
    }
  }