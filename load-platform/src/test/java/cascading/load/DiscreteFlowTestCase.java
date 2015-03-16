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

package cascading.load;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class DiscreteFlowTestCase extends LoadTestCase
  {
  String output;

  @Before
  public void setUp() throws Exception
    {
    super.setUp();
    output = getOutputPath() + "/"; //+ "/build/test/output/load/";
    }
  @Test
  public void testAllDiscreteFlows() throws Exception
    {
    String output = this.output + "mainadf/";

    String[] args = new String[]{
      "-CVMO", "-Xmx3g",
      "--platform", getPlatformName(),
      "-S", output + "status",
      "-I", output + "input",
      "-W", output + "working",
      "-O", output + "output",

      "-MXCF", "0",                 // Serial execution

      "-DH", "yarn.timeline-service.enabled=false",

      "-g",
      "-gf", "1",
      "-gs", "1",

      "-ss", "-fg",                 // Every discrete flow
      "-ij", "-oj", "-lj", "-rj",
      "-cf", "-ca",

      //TODO own test
      "-gwm", "0",                  // Normal distribution
      "-gws", "0.2",

      "-SLS"
    };

    assertTrue( new Main( args ).execute() );

    assertEquals( 8, new File( output + "output" ).list().length );
    }
  }
