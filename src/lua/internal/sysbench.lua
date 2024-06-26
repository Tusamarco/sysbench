-- Copyright (C) 2016-2018 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

ffi = require("ffi")

ffi.cdef[[
void sb_event_start(int thread_id);
void sb_event_stop(int thread_id);
bool sb_more_events(int thread_id);
]]

-- ----------------------------------------------------------------------
-- Main event loop. This is a Lua version of sysbench.c:thread_run()
-- ----------------------------------------------------------------------
function thread_run(thread_id)
   while ffi.C.sb_more_events(thread_id) do
      ffi.C.sb_event_start(thread_id)

      local success, ret
      repeat
         success, ret = pcall(event, thread_id)

         if not success then
            if type(ret) == "table" and
               ret.errcode == sysbench.error.RESTART_EVENT
            then
               if sysbench.hooks.before_restart_event then
                  sysbench.hooks.before_restart_event(ret)
               end
            else
               error(ret, 2) -- propagate unknown errors
            end
         end
      until success

      -- Stop the benchmark if event() returns a value other than nil or false
      if ret then
         break
      end

      ffi.C.sb_event_stop(thread_id)
   end
end

-- ----------------------------------------------------------------------
-- Hooks
-- ----------------------------------------------------------------------

sysbench.hooks = {
   -- sql_error_ignorable = <func>,
   -- report_intermediate = <func>,
   -- report_cumulative = <func>
}

-- Report statistics in the CSV format. Add the following to your
-- script to replace the default human-readable reports
--
-- sysbench.hooks.report_intermediate = sysbench.report_csv
function sysbench.report_csv(stat)
   local seconds = stat.time_interval
   print(string.format("%.0f,%u,%4.2f," ..
                          "%4.2f,%4.2f,%4.2f,%4.2f," ..
                          "%4.2f,%4.2f," ..
                          "%4.2f",
                       stat.time_total,
                       stat.threads_running,
                       stat.events / seconds,
                       (stat.reads + stat.writes + stat.other) / seconds,
                       stat.reads / seconds,
                       stat.writes / seconds,
                       stat.other / seconds,
                       stat.latency_pct * 1000,
                       stat.errors / seconds,
                       stat.reconnects / seconds
   ))
end

-- Report statistics in the JSON format. Add the following to your
-- script to replace the default human-readable reports
--
-- sysbench.hooks.report_intermediate = sysbench.report_json
function sysbench.report_json(stat)
   if not gobj then
      io.write('[\n')
      -- hack to print the closing bracket when the Lua state of the reporting
      -- thread is closed
      gobj = newproxy(true)
      getmetatable(gobj).__gc = function () io.write('\n]\n') end
   else
      io.write(',\n')
   end

   local seconds = stat.time_interval
   io.write(([[
  {
    "time": %4.0f,
    "threads": %u,
    "tps": %4.2f,
    "qps": {
      "total": %4.2f,
      "reads": %4.2f,
      "writes": %4.2f,
      "other": %4.2f
    },
    "latency": %4.2f,
    "errors": %4.2f,
    "reconnects": %4.2f
  }]]):format(
            stat.time_total,
            stat.threads_running,
            stat.events / seconds,
            (stat.reads + stat.writes + stat.other) / seconds,
            stat.reads / seconds,
            stat.writes / seconds,
            stat.other / seconds,
            stat.latency_pct * 1000,
            stat.errors / seconds,
            stat.reconnects / seconds
   ))
end

-- Report statistics in the default human-readable format. You can use it if you
-- want to augment default reports with your own statistics. Call it from your
-- own report hook, e.g.:
--
-- function sysbench.hooks.report_intermediate(stat)
--   print("my stat: ", val)
--   sysbench.report_default(stat)
-- end
function sysbench.report_default(stat)
   local seconds = stat.time_interval
   print(string.format("[ %.0fs ] thds: %u tps: %4.2f qps: %4.2f " ..
                          "(r/w/o: %4.2f/%4.2f/%4.2f) lat (ms,%u%%): %4.2f " ..
                          "err/s %4.2f reconn/s: %4.2f",
                       stat.time_total,
                       stat.threads_running,
                       stat.events / seconds,
                       (stat.reads + stat.writes + stat.other) / seconds,
                       stat.reads / seconds,
                       stat.writes / seconds,
                       stat.other / seconds,
                       sysbench.opt.percentile,
                       stat.latency_pct * 1000,
                       stat.errors / seconds,
                       stat.reconnects / seconds
   ))
end

function sysbench.report_cumulative_csv(stat)
      print("TEST SUMMARY:")
      print("TotalTime,RunningThreads,totalEvents,Events/s," ..
            "Tot Operations,operations/s,tot reads,reads/s,Tot writes,writes/s," ..
            "oterOps/s,latencyPct95(μs) ,Tot errors,errors/s,Tot reconnects,reconnects/s," ..
            "Latency(ms) min, Latency(ms) max, Latency(ms) avg, Latency(ms) sum"
      )
      local seconds = stat.time_interval
      print(string.format("%.0f,%u,%4.2f,%4.2f," ..
                          "%4.2f,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f," ..
                          "%4.2f,%4.2f,%4.2f,%4.2f,%4.2f," ..
                          "%4.2f," ..
                          "%4.2f,%4.2f,%4.2f," ..
                          "%4.2f",
                       stat.time_total,
                       sysbench.opt.threads,
                       stat.events,
                       stat.events / seconds,
                       (stat.reads + stat.writes + stat.other),
                       (stat.reads + stat.writes + stat.other) / seconds,
                       stat.reads,
                       stat.reads / seconds,
                       stat.writes,
                       stat.writes / seconds,
                       stat.other / seconds,
                       stat.latency_pct * 1000,
                       stat.errors,
                       stat.errors / seconds,
                       stat.reconnects,
                       stat.reconnects / seconds,
                       stat.latency_min,
                       stat.latency_max,
                       stat.latency_avg,
                       stat.latency_sum
    ))
end

function sysbench.report_cumulative_default(stat)
   local seconds = stat.time_interval
   local queries = stat.reads + stat.writes + stat.other

   print(string.format(
      "SQL statistics:\n" ..
      "    queries performed:\n" ..
      "        read:                            %u\n" ..
      "        write:                           %u\n" ..
      "        other:                           %u\n" ..
      "        total:                           %u\n" ..
      "    transactions:                        %-6u (%.2f per sec.)\n" ..
      "    queries:                             %-6u (%.2f per sec.)\n" ..
      "    ignored errors:                      %-6u (%.2f per sec.)\n" ..
      "    reconnects:                          %-6u (%.2f per sec.)\n" ..
      "\n" ..
      "Throughput:\n" ..
      "    events/s (eps):                      %.4f\n" ..
      "    time elapsed:                        %.4fs\n" ..
      "    total number of events:              %u\n" ..
      "\n" ..
      "Latency (ms):\n" ..
      "         min: %39.2f\n" ..
      "         avg: %39.2f\n" ..
      "         max: %39.2f",
         stat.reads,
         stat.writes,
         stat.other,
         queries,
         stat.events, stat.events / seconds,
         queries, queries / seconds,
         stat.errors, stat.errors / seconds,
         stat.reconnects, stat.reconnects / seconds,
         stat.events / seconds,
         stat.time_total,
         stat.events,
         stat.latency_min * 1000,
         stat.latency_avg * 1000,
         stat.latency_max * 1000
   ))

   if sysbench.opt.percentile > 0 then
      print(string.format("        %3dth percentile: %27.2f", sysbench.opt.percentile, stat.latency_pct * 1000))
   else
      print(string.format("         percentile stats:               disabled"))
   end

   print(string.format("         sum: %39.2f\n", stat.latency_sum * 1000))
end

