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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.load.Options;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 *
 */
public class CrashOnMemory extends BreakingLoad
  {
  public CrashOnMemory( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  protected Pipe getPipe()
    {
    Pipe pipe = new Pipe( getLoadName() );
    pipe = new Each( pipe, new Fields( "line" ), new CrashingOnMemory() );
    return pipe;
    }

  public static class CrashingOnMemory extends BreakingOperation implements Function
    {
    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      incrementEventCount();
      List howBigCanWeGet = new ArrayList(  );
      if( needsToBreak() )
        while (true)
          howBigCanWeGet.add( new Object() );
      }
    }

  }
