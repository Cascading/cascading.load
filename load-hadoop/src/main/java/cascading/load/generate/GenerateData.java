/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.load.generate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.load.util.Util;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.mapred.JobConf;

/** Class GenerateData creates a test corpus of random words. */
public class GenerateData extends Load
  {
  private String dictionaryPath;

  public GenerateData( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    dictionaryPath = writeDictionaryTuples();

    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), dictionaryPath );
    Tap sink = platform.newTap( platform.newTextLine( new Fields( "line" ) ), options.getInputRoot(), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "load-generator" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( "\t" ) );
    pipe = new Each( pipe, new TupleGenerator( options, new Fields( "line" ) ) );

    return platform.newFlowConnector( properties ).connect( "generate-data", source, sink, pipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{getDictionaryPath()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  public String getDictionaryPath()
    {
    return dictionaryPath;
    }

  private String writeDictionaryTuples() throws IOException
    {
    List<String> dictionary = new ArrayList<String>();
    Util.populateCollection( GenerateData.class, "dictionary.txt", dictionary );

    Tuple output = new Tuple();

    output.addAll( (Object[]) dictionary.toArray( new String[ dictionary.size() ] ) );

    String workingPath = options.getWorkingRoot() + "dictionary/";
    Tap tap = platform.newTap( platform.newTextLine(), workingPath );

    if( options.isLocalMode() )
      {
      TupleEntryCollector writer = platform.newTupleEntryCollector( tap );

      writer.add( output );

      writer.close();
      }
    else
      {
      JobConf jobConf = new JobConf();

      for( int i = 0; i < options.getDataNumFiles(); i++ )
        {
        jobConf.setInt( "mapred.task.partition", i );

        TupleEntryCollector writer = tap.openForWrite( new HadoopFlowProcess( jobConf ) );

        writer.add( output );

        writer.close();
        }
      }

    return workingPath;
    }
  }
