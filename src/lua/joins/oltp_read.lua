#!/usr/bin/env sysbench
-- Copyright (C) 2006-2017 Alexey Kopytov <akopytov@gmail.com>

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

-- ----------------------------------------------------------------------
-- Read/Write OLTP benchmark
-- ----------------------------------------------------------------------

require("joins/oltp_common")

local joins = {}

function prepare_statements()
   if not sysbench.opt.skip_trx then
      prepare_begin()
      prepare_commit()
   end
   
   
   -- Given the list of all joins identify which joins are enabled and add them to join list
   for _, join_name in ipairs(all_joins) do
      if sysbench.opt[join_name] and sysbench.opt[join_name] ~= 0 then
         -- print(sysbench.opt[join_name] .. " of join enabled: " .. join_name) --- IGNORE ---
         table.insert(joins, join_name)
      -- else
      --     print("Join not enabled: " .. join_name) --- IGNORE ---
      end
   end
   local count = 0
   for _ in pairs(joins) do
      count = count + 1
   end
   -- print("Number of items in map after filtering:", count)

   
end

function event()
   if not sysbench.opt.skip_trx then
      begin()
   end

  -- Execute enabled join queries
  for _, join_name in ipairs(joins) do
       if not (join_name:find("update") or join_name:find("insert") or join_name:find("delete")) then
         execute_joins(join_name)
       end
  end


   if not sysbench.opt.skip_trx then
      commit()
   end
   check_reconnect()
end

function sysbench.hooks.report_intermediate(stat)
   if sysbench.opt.stats_format == "human" then
         sysbench.report_default(stat)
   elseif sysbench.opt.stats_format == "csv" then
         sysbench.report_csv(stat)
   elseif sysbench.opt.stats_format == "json" then      
         sysbench.report_json(stat)
   else
      sysbench.report_default(stat)
   end
end
