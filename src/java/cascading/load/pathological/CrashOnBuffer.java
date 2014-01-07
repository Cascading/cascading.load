/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import java.util.Iterator;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.load.Options;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class CrashOnBuffer extends BreakingLoad
  {
  public CrashOnBuffer( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  protected Pipe getPipe()
    {
    Pipe pipe = new Pipe( getLoadName() );
    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word" ), "\\s" ) );
    pipe = new GroupBy( pipe, new Fields( "word" ) );
    pipe = new Every( pipe, new Fields( "word" ),  new CrashingBuffer());
    return pipe;
    }

  public static class CrashingBuffer extends BreakingOperation implements Buffer
    {

    @Override
    public void operate( FlowProcess flowProcess, BufferCall bufferCall )
      {
      Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
      while( iterator.hasNext() )
        {
        incrementEventCount();
        if( needsToBreak() )
          throw new BreakingException( "buffer has reached max count" );
        else
          bufferCall.getOutputCollector().add( iterator.next() );
        }
      }
    }

  }
