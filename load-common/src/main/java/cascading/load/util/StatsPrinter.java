/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.util;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.stats.CascadeStats;
import cascading.stats.CascadingStats;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;

/**
 *
 */
public class StatsPrinter
  {
  public static void printStats( PrintWriter writer, CascadeStats cascadeStats, boolean singlelineStats )
    {
    if( singlelineStats )
      {
      writer.printf( "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%n",
        "type",
        "name",
        "status",
        "start-time",
        "start-time-long",
        "finished-time",
        "finished-time-long",
        "duration",
        "duration-long",
        "children"
      );
      }

    printCascadeStats( writer, cascadeStats, singlelineStats );
    }

  public static void printCascadeStats( PrintWriter writer, CascadeStats cascadeStats )
    {
    printCascadeStats( writer, cascadeStats, false );
    }

  public static void printCascadeStats( PrintWriter writer, CascadeStats cascadeStats, boolean singlelineStats )
    {
    printSummaryFor( writer, "Cascade", cascadeStats, null, singlelineStats );

    Collection<FlowStats> flowStats = cascadeStats.getChildren();

    for( FlowStats flowStat : flowStats )
      {
      if( !singlelineStats )
        writer.println();

      printFlowStats( writer, flowStat, singlelineStats );
      }
    }

  public static void printFlowStats( PrintWriter writer, FlowStats flowStat, boolean singlelineStats )
    {
    printSummaryFor( writer, "Flow", flowStat, null, singlelineStats );

    Collection<FlowStepStats> stepStats = flowStat.getChildren();

    for( FlowStepStats stepStat : stepStats )
      {
      if( !singlelineStats )
        writer.println();

      printStepStats( writer, stepStat, flowStat, singlelineStats );
      }
    }

  public static void printStepStats( PrintWriter writer, FlowStepStats stepStat, FlowStats flowStat, boolean singlelineStats )
    {
    String overrideName =
      flowStat == null ? null : getStepStatsName( stepStat, flowStat.getName(), singlelineStats );
    printSummaryFor( writer, "Step", stepStat, overrideName, singlelineStats );
    }

  private static void printSummaryFor( PrintWriter writer, String type, CascadingStats cascadingStats, String overrideName, boolean singlelineStats )
    {
    if( singlelineStats )
      printRecordSummaryFor( writer, type, cascadingStats, overrideName );
    else
      printDisplaySummaryFor( writer, type, cascadingStats, overrideName );
    }

  private static void printRecordSummaryFor( PrintWriter writer, String type, CascadingStats cascadingStats, String overrideName )
    {
    int childCount = 0;

    if( !cascadingStats.getChildren().isEmpty() )
      childCount = cascadingStats.getChildren().size();

    long duration = cascadingStats.getDuration() / 1000;

    writer.printf( "%s\t%s\t%s\t%tT\t%d\t%tT\t%d\t%s\t%d\t%d%n",
      type,
      overrideName == null ? cascadingStats.getName() : overrideName,
      cascadingStats.getStatus(),
      cascadingStats.getStartTime(),
      cascadingStats.getStartTime(),
      cascadingStats.getFinishedTime(),
      cascadingStats.getFinishedTime(),
      String.format( "%d:%02d:%02d", duration / 3600, duration % 3600 / 60, duration % 60 ),
      duration,
      childCount
    );

    writer.flush();
    }

  private static void printDisplaySummaryFor( PrintWriter writer, String type, CascadingStats cascadingStats, String overrideName )
    {
    writer.printf( "%s: %s%n", type, overrideName == null ? cascadingStats.getName() : overrideName );

    writer.printf( "  finish status: %s%n", cascadingStats.getStatus() );
    writer.printf( "  start:    %tT%n", cascadingStats.getStartTime() );
    writer.printf( "  finished: %tT%n", cascadingStats.getFinishedTime() );

    long duration = cascadingStats.getDuration() / 1000;

    writer.printf( "  duration: %d:%02d:%02d%n", duration / 3600, duration % 3600 / 60, duration % 60 );

    if( !cascadingStats.getChildren().isEmpty() )
      writer.printf( "  num children: %d%n", cascadingStats.getChildren().size() );

    writer.flush();
    }

  private static final Pattern stepStatSeqNumPatt = Pattern.compile( "^(\\(\\d+/\\d+\\)) " );

  private static String getStepStatsName( FlowStepStats stepStat, String parent, boolean uniqueName )
    {
    String name = stepStat.getName();

    if( !uniqueName )
      return name;

    Matcher matchSeqNum = stepStatSeqNumPatt.matcher( name );

    if( matchSeqNum.find() )
      return parent + " " + matchSeqNum.group();

    return parent + name;
    }
  }
