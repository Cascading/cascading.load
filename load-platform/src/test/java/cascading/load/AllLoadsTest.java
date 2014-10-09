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
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.load.Main;
import cascading.load.Options;
import cascading.load.countsort.CountSort;
import cascading.load.generate.GenerateData;
import cascading.load.join.MultiJoin;
import cascading.load.pipeline.Pipeline;
import cascading.load.platform.PlatformLoader;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class AllLoadsTest extends LoadTestCase
  {
  String output = "";

  @Before
  public void setUp()
    {
    output = getRootPath() + "/build/test/output/load/";
    }

  @Test
  public void testAllLoads() throws Exception
    {
    String output = this.output + "api/";

    Options options = new Options();

    options.setPlatformName( getPlatformName() );

    options.setDataGenerate( true );
    options.setDataNumFiles( 3 );
    options.setDataFileSizeMB( 1 );
    options.setWorkingRoot( output + "working" );
    options.setInputRoot( output + "input" );
    options.setOutputRoot( output + "output" );

    GenerateData generate = new GenerateData( options, getProperties() );

    Flow generateFlow = generate.createFlow();

    generateFlow.complete();

    File inputPath = new File( generate.getInputPaths()[ 0 ] );
    assertTrue( inputPath.exists() );
    // it is a file on the local platform
    if( inputPath.isDirectory() )
      assertEquals( inputPath.toString(), 6, inputPath.list().length );

    File outputPath = new File( generate.getOutputPaths()[ 0 ] );
    assertTrue( outputPath.exists() );
    if( outputPath.isDirectory() )
      assertEquals( outputPath.toString(), 8, outputPath.list().length ); // includes _SUCCESS and its crc

    options.setCountSort( true );

    CountSort countSort = new CountSort( options, getProperties() );

    Flow countSortFlow = countSort.createFlow();

    countSortFlow.complete();

    outputPath = new File( countSort.getOutputPaths()[ 0 ] );
    assertTrue( outputPath.exists() );
    if( outputPath.isDirectory() )
      assertEquals( outputPath.toString(), 4, outputPath.list().length );

    MultiJoin multiJoin = new MultiJoin( options, getProperties() );

    Flow multiJoinFlow = multiJoin.createFlow();

    multiJoinFlow.complete();

    assertEquals( 4, multiJoin.getOutputPaths().length );

    for( String path : multiJoin.getOutputPaths() )
      {
      outputPath = new File( path );
      assertTrue( outputPath.exists() );
      if( outputPath.isDirectory() )
        assertEquals( outputPath.toString(), 4, outputPath.list().length );
      }

    Pipeline pipeline = new Pipeline( options, getProperties() );

    Flow pipelineFlow = pipeline.createFlow();

    pipelineFlow.complete();

    outputPath = new File( pipeline.getOutputPaths()[ 0 ] );
    assertTrue( outputPath.exists() );
    if( outputPath.isDirectory() )
      assertEquals( outputPath.toString(), 4, outputPath.list().length );
    }

  @Test
  public void testMain() throws Exception
    {
    String output = this.output + "main/";

    String[] args = new String[]{
      "--platform", getPlatformName(),
      "-CVMO", "-Xmx512m",
      "-S", output + "status",
      "-I", output + "input",
      "-W", output + "working",
      "-O", output + "output",

      "-g",
      "-gf", "3",
      "-gs", "1",

      "-c",

      "-m",

      "-p"

    };

    assertTrue( new Main( args ).execute() );

    assertEquals( 6, new File( output + "output" ).list().length );
    }

  @Test
  public void testCleanWorkFiles() throws Exception
    {
    String output = this.output + "maincwf/";

    String[] args = new String[]{
      "--platform", getPlatformName(),
      "-CVMO", "-Xmx512m",
      "-S", output + "status",
      "-I", output + "input",
      "-W", output + "working",
      "-O", output + "output",

      "-g",
      "-gf", "1",
      "-gs", "1",

      "-c",

      "-m",

      "-p",

      "-CWF"
    };

    assertTrue( new Main( args ).execute() );

    assertEquals( 1, new File( output ).list().length );
    }

  @Test
  public void testSingleLineStatus() throws Exception
    {
    String output = this.output + "mainsls/";

    String[] args = new String[]{
      "--platform", getPlatformName(),
      "-CVMO", "-Xmx512m",
      "-S", output + "status",
      "-I", output + "input",
      "-W", output + "working",
      "-O", output + "output",

      "-g",
      "-gf", "1",
      "-gs", "1",

      "-c",

      "-m",

      "-p",

      "-SLS"
    };

    Main main = new Main( args );

    assertTrue( main.execute() );

    String fileName = main.getFullStatsRoot();

    File file = new File( fileName );

    if( file.isDirectory() )
      {
      String[] list = file.list( new FilenameFilter()
      {
      @Override
      public boolean accept( File dir, String name )
        {
        return name.startsWith( "part-" );
        }
      } );

      fileName += "/" + list[ 0 ];
      }

    FileReader fr = new FileReader( fileName );
    LineNumberReader ln = new LineNumberReader( fr );
    int lineNo = ln.getLineNumber();
    while( ln.readLine() != null )
      lineNo = ln.getLineNumber();
    ln.close();

    if( getPlatformName().equals( "local" ) )
      assertEquals( 10, lineNo );
    else
      assertEquals( 16, lineNo );
    }

  @Test
  public void testAllDiscreteFlows() throws Exception
    {
    String output = this.output + "mainadf/";

    String[] args = new String[]{
      "-CVMO", "-Xmx512m",
      "--platform", getPlatformName(),
      "-S", output + "status",
      "-I", output + "input",
      "-W", output + "working",
      "-O", output + "output",

      "-MXCF", "0",                 // Serial execution

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
