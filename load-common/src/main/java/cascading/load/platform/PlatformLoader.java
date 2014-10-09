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

package cascading.load.platform;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import cascading.util.Util;

/**
 * A simple class to load the Platform from the CLASSPATH by looking for a file called "multitool/platform.properties"
 * and loading the class defined in there. A platform is required to have a no-arg Constructor to be properly loaded.
 */
public class PlatformLoader
  {
  public static final String PLATFORM_PROPERTIES_FILE_NAME = "cascading/load/platform.properties";
  public static final String PLATFORM_NAME_PROPERTY = "platform.name";
  public static final String PLATFORM_CLASS_NAME_PROPERTY = "platform.classname";

  /**
   * Loads and returns the platform given by its name. If the platform cannot be loaded a RuntimeException is thrown.
   *
   * @param platformName The name of the platform to load.
   * @return A @{Platform} instance for the given name.
   */
  public CascadingLoadPlatform loadPlatform( String platformName )
    {
    Set<String> found = new HashSet<String>();

    ClassLoader classLoader = getClass().getClassLoader();

    Enumeration<URL> urls;

    try
      {
      urls = classLoader.getResources( PLATFORM_PROPERTIES_FILE_NAME );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to load resources" );
      }

    while( urls.hasMoreElements() )
      {
      URL url = urls.nextElement();
      Properties props = new Properties();

      try
        {
        InputStream stream = url.openStream();

        if( stream == null )
          throw new PlatformNotFoundException( String.format( "unable to locate '%s' on the classpath.", PLATFORM_PROPERTIES_FILE_NAME ) );

        props.load( stream );
        }
      catch( IOException exception )
        {
        throw new PlatformNotFoundException( exception );
        }

      String name = props.getProperty( PLATFORM_NAME_PROPERTY );

      found.add( name );

      if( !platformName.equals( name ) )
        continue;

      String type = props.getProperty( PLATFORM_CLASS_NAME_PROPERTY );

      try
        {
        return (CascadingLoadPlatform) Class.forName( type ).newInstance();
        }
      catch( ClassNotFoundException exception )
        {
        throw new PlatformNotFoundException( exception );
        }
      catch( InstantiationException exception )
        {
        throw new PlatformNotFoundException( exception );
        }
      catch( IllegalAccessException exception )
        {
        throw new PlatformNotFoundException( exception );
        }
      }

    throw new PlatformNotFoundException( "Invalid platform. Trying to load " + platformName + " but found " + Util.join( found ) );
    }
  }
