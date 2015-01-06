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

import java.util.Collections;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.load.Options;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;

/**
 *
 */
public class CrashOnAggregation extends BreakingLoad
  {

  public CrashOnAggregation( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Pipe getPipe()
    {
    Pipe pipe = new Pipe( getLoadName() );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "word" ) );

    pipe = new Every( pipe, new Fields( "word" ), new BreakingAggregator() );

    Fields groupFields = new Fields( "count" );

    groupFields.setComparator( "count", Collections.reverseOrder() );

    pipe = new GroupBy( pipe, groupFields, new Fields( "word" ) );

    return pipe;
    }

  public static class BreakingAggregator extends BreakingOperation<Pair<Long[], Tuple>> implements Aggregator<Pair<Long[], Tuple>>
    {

    @Override
    public void start( FlowProcess flowProcess, AggregatorCall<Pair<Long[], Tuple>> aggregatorCall )
      {
      aggregatorCall.getContext().getLhs()[ 0 ] = 0L;
      }

    @Override
    public void aggregate( FlowProcess flowProcess, AggregatorCall<Pair<Long[], Tuple>> aggregatorCall )
      {
      aggregatorCall.getContext().getLhs()[ 0 ] += 1L;
      setEventCount( aggregatorCall.getContext().getLhs()[ 0 ] );
      if( needsToBreak() )
        throw new BreakingException( "aggregation count exceeded " );

      }

    @Override
    public void complete( FlowProcess flowProcess, AggregatorCall<Pair<Long[], Tuple>> aggregatorCall )
      {
      // Since this code errors out, this should never get hit but allow graceful termination anyway
      aggregatorCall.getContext().getRhs().set( 0, aggregatorCall.getContext().getLhs()[ 0 ] );
      aggregatorCall.getOutputCollector().add(  aggregatorCall.getContext().getRhs() );
      }

    }

  }
