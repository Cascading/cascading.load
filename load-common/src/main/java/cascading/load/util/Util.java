/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class Util
  {

  public static String[] join( String[]... values )
    {
    List<String> list = new ArrayList<String>();

    for( String[] strings : values )
      Collections.addAll( list, strings );

    return list.toArray( new String[ list.size() ] );
    }

  public static String extractOrNull( Pattern pattern, String value )
    {
    Matcher matcher = pattern.matcher( value );

    if( matcher.matches() )
      return matcher.replaceFirst( "$1" );

    return null;
    }

  public static void load( Class type, String resource, Properties map ) throws IOException
    {
    load( type, resource, map, true );
    }

  public static void load( Class type, String resource, Properties map, boolean trim ) throws IOException
    {
    InputStream inputStream = type.getResourceAsStream( resource );

    if( inputStream == null )
      throw new IllegalArgumentException( "unable to load resource: " + resource + ", from: " + type.getPackage().getName() );

    map.load( inputStream );
    inputStream.close();

    if( !trim )
      return;

    Set<String> names = map.stringPropertyNames();

    for( String name : names )
      {
      String value = map.getProperty( name );

      if( value != null )
        value = value.trim();

      map.remove( name );
      map.setProperty( name.trim(), value );
      }
    }

  public static void populateCollection( Class type, String resource, List<String> list ) throws IOException
    {
    InputStream inputStream = type.getResourceAsStream( resource );

    if( inputStream == null )
      throw new IllegalArgumentException( "unable to load resource: " + resource + ", from: " + type.getPackage().getName() );

    BufferedReader reader = new BufferedReader( new InputStreamReader( inputStream ) );

    try
      {
      String line = reader.readLine();

      while( line != null )
        {
        list.add( line );

        line = reader.readLine();
        }
      }
    finally
      {
      try
        {
        reader.close();
        }
      catch( IOException exception )
        {
        // do nothing
        }
      }
    }




  /**
   * By default File#delete fails for non-empty directories, it works like "rm".
   * We need something a little more brutual - this does the equivalent of "rm -r"
   *
   * @param path Root File Path
   * @return true iff the file and all sub files/directories have been removed
   * @throws FileNotFoundException
   */
  public static boolean deleteRecursive( File path ) throws FileNotFoundException
    {
    if( !path.exists() )
      throw new FileNotFoundException( path.getAbsolutePath() );
    boolean ret = true;
    if( path.isDirectory() )
      {
      for( File f : path.listFiles() )
        {
        ret = ret && Util.deleteRecursive( f );
        }
      }
    return ret && path.delete();
    }
  }
