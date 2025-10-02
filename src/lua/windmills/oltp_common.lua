-- Copyright (C) 2006-2018 Marco Tusa <tusa.marco@gmail.com>

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

-- -----------------------------------------------------------------------------
-- Common code for OLTP benchmarks.
-- -----------------------------------------------------------------------------

function init()
   assert(event ~= nil,
          "this script is meant to be included by other OLTP scripts and " ..
             "should not be called directly.")
end

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: prepare, warmup, run, " ..
            "cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
   table_size =
      {"Number of rows per table", 10000},
   range_size =
      {"Range size for range SELECT queries", 100},
   tables =
      {"Number of tables", 1},
   from_table =
      {"Prepare from this table number", 0},
   from_row =
      {"Prepare tables starting from the given row number", 1},
   report_loaded_rows_every = 
      {"Report how many rows were inserted during the data preparation. If 0 [default] it is disable." .. 
      "If chunk_size_in_prepare is used, interval cannot be larger than the chunk_size_in_prepare dimension", 0},   
      point_selects =
      {"Number of point SELECT queries per transaction", 10},
   simple_ranges =
      {"Number of simple range SELECT queries per transaction", 1},
   sum_ranges =
      {"Number of SELECT SUM() queries per transaction", 1},
   order_ranges =
      {"Number of SELECT ORDER BY queries per transaction", 1},
   distinct_ranges =
      {"Number of SELECT DISTINCT queries per transaction", 1},
   index_updates =
      {"Number of UPDATE index queries per transaction", 1},
   non_index_updates =
      {"Number of UPDATE non-index queries per transaction", 1},
   delete_inserts =
      {"Number of DELETE/INSERT combination per transaction", 1},
   range_selects =
      {"Enable/disable all range SELECT queries", true},
   auto_inc =
   {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
       "or its alternatives in other DBMS. When disabled, use " ..
       "client-generated IDs", true},
   skip_trx =
      {"Don't start explicit transactions and execute all queries " ..
          "in the AUTOCOMMIT mode", false},
   secondary =
      {"Use a secondary index in place of the PRIMARY KEY", false},
   create_secondary =
      {"Create a secondary index esin addition to the PRIMARY KEY", true},
   create_compound =
      {"Create compound indexes in addition to the PRIMARY KEY", true},
   use_replace =
      {"Use replace instead Insert", false},
   no_primary_key =
      {"We will not create an explicit primary key on the tables. Keep in mind INNODB will generate anyhow", false},
   reconnect =
      {"Reconnect after every N events. The default (0) is to not reconnect",
       0},      
   mysql_storage_engine =
      {"Storage engine, if MySQL is used", "innodb"},
   pgsql_variant =
      {"Use this PostgreSQL variant when running with the " ..
          "PostgreSQL driver. The only currently supported " ..
          "variant is 'redshift'. When enabled, " ..
          "create_secondary is automatically disabled, and " ..
          "delete_inserts is set to 0"},
   mysql_table_options=
   {"Add specific table instructions like charset and ROW format", " CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC "},
   table_name=
   {"Specify a table name instead sbtest", "sbtest"},
   stats_format=
   {"Specify how you want the statistics written [default=human (readable); csv; json] ", "human"},
   create_indexes_before_dataload =
   {"Create all imdexes before loading data. This can be useful when in the need to avoid the operation with table filled", false},
   chunk_size_in_prepare = 
      {"Split the data load by chunk instead loading all in one transactions. Using 0 means disable chunk," ..
         "Chunk size cannot be larger than 1/4 of the number of rows",0}
}

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = (sysbench.tid % sysbench.opt.threads + 1) + sysbench.opt.from_table, sysbench.opt.tables,
   sysbench.opt.threads do
     create_table(drv, con, i)
   end
end

-- Preload the dataset into the server cache. This command supports parallel
-- execution, i.e. will benefit from executing with --threads > 1 as long as
-- --tables > 1
--
-- PS. Currently, this command is only meaningful for MySQL/InnoDB benchmarks
function cmd_warmup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   assert(drv:name() == "mysql", "warmup is currently MySQL only")

   -- Do not create on disk tables for subsequent queries
   con:query("SET tmp_table_size=2*1024*1024*1024")
   con:query("SET max_heap_table_size=2*1024*1024*1024")

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
   sysbench.opt.threads do
      local t = sysbench.opt.table_name .. i
      print("Preloading table " .. t)
      con:query("ANALYZE TABLE ".. t)
      con:query(string.format(
                   "SELECT AVG(id) FROM " ..
                      "(SELECT * FROM %s FORCE KEY (PRIMARY) " ..
                      "LIMIT %u) t",
                   t, sysbench.opt.table_size))
      con:query(string.format(
                   "SELECT COUNT(*) FROM " ..
                      "(SELECT * FROM %s WHERE kwatts_s LIKE '%%0%%' LIMIT %u) t",
                   t, sysbench.opt.table_size))
   end
end

-- Implement parallel prepare and warmup commands, define 'prewarm' as an alias
-- for 'warmup'
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
   warmup = {cmd_warmup, sysbench.cmdline.PARALLEL_COMMAND},
   prewarm = {cmd_warmup, sysbench.cmdline.PARALLEL_COMMAND}
}


-- Template strings of random digits with 11-digit groups separated by dashes

-- 3 characters
local c_value_template = "@@@"

-- 5 groups, 59 characters
local pad_value_template = "###########-###########-###########-" ..
   "###########-###########"

function get_c_value()
   return sysbench.rand.string(c_value_template)
end

function get_pad_value()
   return sysbench.rand.string(pad_value_template)
end

function create_indexes(drv, con, table_num)
   if sysbench.opt.create_secondary then
	  print(string.format("Creating a secondary index on '%s%d'...",
						  sysbench.opt.table_name,table_num))
					
	  con:query(string.format("CREATE INDEX kuuid_x ON %s%d(uuid)",
							  sysbench.opt.table_name,table_num, table_num))
	  con:query(string.format("CREATE INDEX millid_x ON %s%d(millid)",
							  sysbench.opt.table_name,table_num, table_num))
	  con:query(string.format("CREATE INDEX active_x ON %s%d(active)",
							  sysbench.opt.table_name,table_num, table_num))
						  
   end
   if sysbench.opt.create_compound then
	  print(string.format("Creating a compound index on '%s%d'...",
						  sysbench.opt.table_name,table_num))
					  
	  con:query(string.format("CREATE INDEX IDX_millid ON %s%d(`millid`,`active`)",
							  sysbench.opt.table_name,table_num, table_num))                    

	  con:query(string.format("CREATE INDEX IDX_active ON %s%d(`id`,`active`)",
							  sysbench.opt.table_name,table_num, table_num))                    

	  con:query(string.format("CREATE INDEX kcontinent_x ON %s%d(`continent`,`id`)",
							  sysbench.opt.table_name,table_num, table_num))                    

   end

end

function create_table(drv, con, table_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query
   local chunck = 0
   local rows = sysbench.opt.table_size 
   

   -- If chunk is defined then we calculate if it is too large and resize 
   if sysbench.opt.chunk_size_in_prepare > 0 then
      chunck = sysbench.opt.chunk_size_in_prepare
      max_cunck_size = rows / 4 
      print(string.format("Max chunk size '%d'...", max_cunck_size))

      if chunck > max_cunck_size then
         chunck = max_cunck_size
      end
      print(string.format("Using chunks to load data chunk size'%d'...", chunck))
   end
   if sysbench.opt.secondary then
     id_index_def = "KEY xid"
   else
     id_index_def = "PRIMARY KEY"
   end

   if drv:name() == "mysql" or drv:name() == "attachsql" or
      drv:name() == "drizzle"
   then
      if sysbench.opt.auto_inc then
         id_def = "BIGINT(11) NOT NULL AUTO_INCREMENT"
      else
         id_def = "BIGINT NOT NULL"
      end
      engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
      extra_table_options = sysbench.opt.mysql_table_options or ""
   elseif drv:name() == "pgsql"
   then
      if not sysbench.opt.auto_inc then
         id_def = "INTEGER NOT NULL"
      elseif pgsql_variant == 'redshift' then
        id_def = "INTEGER IDENTITY(1,1)"
      else
        id_def = "SERIAL"
      end
   else
      error("Unsupported database driver:" .. drv:name())
   end

   print(string.format("Creating table '%s%d'...", sysbench.opt.table_name,table_num))
   
   --print("DEBUG TABLE OPTION" .. sysbench.opt.mysql_table_options)
   
   if sysbench.opt.from_row < 2 then
      con:query(string.format([[DROP TABLE IF EXISTS %s%d]],sysbench.opt.table_name, table_num))
   
   
      local primaryKeyDefinition = ", PRIMARY KEY (`id`)"
      
      if sysbench.opt.no_primary_key and not sysbench.opt.auto_inc then
         primaryKeyDefinition = ""
      end
      
      query = string.format([[   
      CREATE TABLE `%s%d` (
   `id` %s,
   `uuid` char(36) NOT NULL,
   `millid` smallint(6) NOT NULL,
   `kwatts_s` int(11) NOT NULL,
   `date` date NOT NULL ,
   `location` varchar(50) NOT NULL,
   `continent` varchar(50) NOT NULL,
   `active` smallint UNSIGNED NOT NULL DEFAULT '1',
   `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   `strrecordtype` char(3) COLLATE utf8_bin NOT NULL %s
   ) %s ROW_FORMAT=DYNAMIC  %s]],
   sysbench.opt.table_name, table_num, id_def, primaryKeyDefinition,engine_def, extra_table_options)
      
      
      --print("DEBUG :" .. query)
      con:query(query)
      
      if sysbench.opt.create_indexes_before_dataload then 
         create_indexes(drv, con, table_num)
      end
   end   

   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into '%s%d'",
                          sysbench.opt.table_size, sysbench.opt.table_name, table_num))
   end

   local query_pre = ""
   if sysbench.opt.auto_inc then
      query_pre = "INSERT INTO " ..  sysbench.opt.table_name .. table_num .. "(uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES"
   else
      query_pre = "INSERT INTO " ..  sysbench.opt.table_name .. table_num .. "(id,uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES"
   end

   con:bulk_insert_init(query_pre)

   local c_val
   local pad_val
   
   
   local uuid = "UUID()"
   local millid 
   local kwatts_s
   local date = "NOW()"
   local location
   local continent
   local active
   local strrecordtype = "@@@"
   local row_counter = 0
   local start_row = sysbench.opt.from_row
   local end_row = sysbench.opt.table_size
   local report_interval = sysbench.opt.report_loaded_rows_every
   local report_counter = 0

   if start_row < 2 then 
      end_row = end_row - 1
   end

   print(string.format("Start filling tables from row# %d to row# %d ", sysbench.opt.from_row, sysbench.opt.table_size))

--sysbench.opt.table_size
   con:bulk_insert_init(query_pre)
   query = ""

   for i = start_row, end_row do

      c_val = get_c_value()
      strrecordtype =  sysbench.rand.string("@@@")
      location =sysbench.rand.varstringalpha(5, 50)
      continent =sysbench.rand.continent(7)
      active = sysbench.rand.default(0,65535)
      millid = sysbench.rand.default(1,400)
      kwatts_s = sysbench.rand.default(0,4000000)
 
                                                                                                                                  
      if (sysbench.opt.auto_inc) then
        -- "(uuid,millid,kwatts_s,date,location,continent,active,strrecordtyped)
         query = string.format("(%s, %d, %d,%s,'%s','%s',%d,'%s')",
                               uuid,
                               millid,
                               kwatts_s,
                               date,
                               location,
                               continent,
                               active,
                               strrecordtype
                               )
      else
         query = string.format("(%d,%s, %d, %d,%s,'%s','%s',%d,'%s')",
                               i,
                               uuid,
                               millid,
                               kwatts_s,
                               date,
                               location,
                               continent,
                               active,
                               strrecordtype
                               )
      end
     -- print("DEBUG :" .. continent)
      con:bulk_insert_next(query)

      if report_interval > 0 then
         report_counter = report_counter + 1
         if report_counter >= report_interval then
            report_counter = 0
            print(string.format("Data for table '%s%d' inserted rows {%d}", sysbench.opt.table_name, table_num, i))
         end
      end 

      if chunck > 0 then
         row_counter = row_counter + 1

         if row_counter >= chunck then
            row_counter = 0
            con:bulk_insert_done()
            con:query("COMMIT")
            print(string.format("Flushing chunk data for table '%s%d' inserted rows {%d}", sysbench.opt.table_name, table_num, i))
            con:bulk_insert_init(query_pre)
         end
      end
   end

   con:bulk_insert_done()

   if not sysbench.opt.create_indexes_before_dataload then 
	   create_indexes(drv, con, table_num)
	end


end

local t = sysbench.sql.type
local insertAction = "INSERT"
local onDuplicateKeyAction = " ON DUPLICATE KEY UPDATE kwatts_s=kwatts_s+1"

local stmt_defs = {
   point_selects = {
      "SELECT id, millid, date,continent,active,kwatts_s FROM %s%u WHERE id=?",
      t.INT},
   simple_ranges = {
      "SELECT id, millid, date,continent,active,kwatts_s FROM %s%u WHERE id BETWEEN ? AND ?",
      t.INT, t.INT},
   sum_ranges = {
      "SELECT SUM(kwatts_s) FROM %s%u WHERE id BETWEEN ? AND ?",
        t.INT, t.INT},
   order_ranges = {
      "SELECT id, millid, date,continent,active,kwatts_s  FROM %s%u WHERE id BETWEEN ? AND ? ORDER BY millid",
       t.INT, t.INT},
   distinct_ranges = {
      "SELECT DISTINCT millid,continent,active,kwatts_s   FROM %s%u WHERE id BETWEEN ? AND ? ",
      t.INT, t.INT},
   index_updates = {
      "UPDATE %s%u SET active=? WHERE id=?",
      t.INT,t.INT},
   non_index_updates = {
      "UPDATE %s%u SET location=? WHERE id=?",
       {t.VARCHAR,50},t.INT},
   deletes = {
      "DELETE FROM %s%u WHERE id=?",
      t.INT},
   inserts = {
      "INSERT INTO %s%u (id,uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES (?, UUID(), ?, ?, NOW(), ?, ?, ?, ?) ON DUPLICATE KEY UPDATE kwatts_s=kwatts_s+1",
      t.BIGINT, t.INT,t.INT, {t.VARCHAR, 50},{t.CHAR, 20},t.INT, {t.CHAR, 3}},
   replace = {
      "REPLACE INTO %s%u (id,uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES (?, UUID(), ?, ?, NOW(), ?, ?, ?, ?)",
      t.BIGINT, t.INT,t.INT, {t.VARCHAR, 50},{t.CHAR, 20},t.INT, {t.CHAR, 3}},
  
}


function prepare_begin()
   stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
   stmt.commit = con:prepare("COMMIT")
end

function prepare_for_each_table(key)
   for t = 1, sysbench.opt.tables do
   
      stmt[t][key] = con:prepare(string.format(stmt_defs[key][1], sysbench.opt.table_name,t))
-- print("DEBUG: " .. string.format(stmt_defs[key][1], sysbench.opt.table_name,t)

      local nparam = #stmt_defs[key] - 1

      if nparam > 0 then
         param[t][key] = {}
      end

      for p = 1, nparam do
         local btype = stmt_defs[key][p+1]
         local len

         if type(btype) == "table" then
            len = btype[2]
            btype = btype[1]
         end
         if btype == sysbench.sql.type.VARCHAR or
            btype == sysbench.sql.type.CHAR then
               param[t][key][p] = stmt[t][key]:bind_create(btype, len)
         else
            param[t][key][p] = stmt[t][key]:bind_create(btype)
         end
      end

      if nparam > 0 then
         stmt[t][key]:bind_param(unpack(param[t][key]))
      end
   end
end

function prepare_point_selects()
   prepare_for_each_table("point_selects")
end

function prepare_simple_ranges()
   prepare_for_each_table("simple_ranges")
end

function prepare_sum_ranges()
   prepare_for_each_table("sum_ranges")
end

function prepare_order_ranges()
   prepare_for_each_table("order_ranges")
end

function prepare_distinct_ranges()
   prepare_for_each_table("distinct_ranges")
end

function prepare_index_updates()
   prepare_for_each_table("index_updates")
end

function prepare_non_index_updates()
   prepare_for_each_table("non_index_updates")
end

function prepare_delete_inserts()
   prepare_for_each_table("deletes")
   if sysbench.opt.use_replace then
	   prepare_for_each_table("replace")
	else   
	   prepare_for_each_table("inserts")
   end 
end

function prepare_inserts()
   if sysbench.opt.use_replace then
	   prepare_for_each_table("replace")
	else   
	   prepare_for_each_table("inserts")
   end 
end



function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   -- Create global nested tables for prepared statements and their
   -- parameters. We need a statement and a parameter set for each combination
   -- of connection/table/query
   stmt = {}
   param = {}

   for t = 1, sysbench.opt.tables do
      stmt[t] = {}
      param[t] = {}
   end

   -- This function is a 'callback' defined by individual benchmark scripts
   prepare_statements()
end

-- Close prepared statements
function close_statements()
   for t = 1, sysbench.opt.tables do
      for k, s in pairs(stmt[t]) do
         stmt[t][k]:close()
      end
   end
   if (stmt.begin ~= nil) then
      stmt.begin:close()
   end
   if (stmt.commit ~= nil) then
      stmt.commit:close()
   end
end

function thread_done()
   close_statements()
   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.tables do
      print(string.format("Dropping table '%s%d'...", sysbench.opt.table_name,i))
      con:query("DROP TABLE IF EXISTS " .. sysbench.opt.table_name .. i )
   end
end

local function get_table_num()
   return sysbench.rand.uniform(1, sysbench.opt.tables)
end

local function get_id()
   return sysbench.rand.default(1, sysbench.opt.table_size)
end

function begin()
   stmt.begin:execute()
end

function commit()
   stmt.commit:execute()
end

function execute_point_selects()
   local tnum = get_table_num()
   local i

   for i = 1, sysbench.opt.point_selects do
      param[tnum].point_selects[1]:set(get_id())

      stmt[tnum].point_selects:execute()
   end
end

local function execute_range(key)
   local tnum = get_table_num()

   for i = 1, sysbench.opt[key] do
      local id = get_id()

      param[tnum][key][1]:set(id)
      param[tnum][key][2]:set(id + sysbench.opt.range_size - 1)

      stmt[tnum][key]:execute()
   end
end

function execute_simple_ranges()
   execute_range("simple_ranges")
end

function execute_sum_ranges()
   execute_range("sum_ranges")
end

function execute_order_ranges()
   execute_range("order_ranges")
end

function execute_distinct_ranges()
   execute_range("distinct_ranges")
end

function execute_index_updates()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.index_updates do
      param[tnum].index_updates[1]:set(sysbench.rand.default(0,65535))
      param[tnum].index_updates[2]:set(get_id())
      stmt[tnum].index_updates:execute()
      
--      param[tnum].index_updates[1]:set(1)
--      param[tnum].index_updates[2]:set(get_id())
--      stmt[tnum].index_updates:execute()      
      
   end
end

function execute_non_index_updates()
   local tnum = get_table_num()
    
   for i = 1, sysbench.opt.non_index_updates do
      param[tnum].non_index_updates[1]:set_rand_str_alpha("")
      param[tnum].non_index_updates[2]:set(get_id())

      stmt[tnum].non_index_updates:execute()
   end
end

function execute_delete_inserts()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.delete_inserts do
      local id = get_id()

      local query = get_query_insert(id, tnum)

      param[tnum].deletes[1]:set(id)
   
      stmt[tnum].deletes:execute()
      con:query(query)

   end
end

function get_query_insert(id,tnum)

  local query_prefix="" 	

--      "INSERT INTO %s%u (id,uuid,millid,kwatts_s,date,location,active,strrecordtyped) VALUES (?, UUID(), ?, ?, NOW(), ?, ?, ?)",
--      t.BIGINT, t.TINYINT, t.INT, {t.VARCHAR, 50},t.TINYINT, {t.CHAR, 3}},

  if not sysbench.opt.use_replace then
	  query_prefix= string.format("INSERT INTO %s%u (id,uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES ",sysbench.opt.table_name, tnum)
  else
	  query_prefix= string.format("REPLACE INTO %s%u (id,uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES ",sysbench.opt.table_name, tnum)
  end
  
  if id == 0  then
		  query_prefix= string.format("INSERT INTO %s%u (uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES ",sysbench.opt.table_name, tnum)
  else
	  if not sysbench.opt.use_replace then
		  query_prefix= string.format("INSERT INTO %s%u (id,uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES ",sysbench.opt.table_name, tnum)
	  else
		  query_prefix= string.format("REPLACE INTO %s%u (id,uuid,millid,kwatts_s,date,location,continent,active,strrecordtype) VALUES ",sysbench.opt.table_name, tnum)
	  end
  
  end
  
  local uuid = "UUID()"
  local date = "NOW()"
  
  millid = sysbench.rand.default(1,400)
  kwatts_s = sysbench.rand.default(0,4000000)
  location =sysbench.rand.varstringalpha(5, 50)
  continent =sysbench.rand.continent(7)
  active = sysbench.rand.default(0,65535)
--	(id,uuid,millid,kwatts_s,date,location,continent,active,strrecordtype)
  
  if id == 0 then    
	  query_values = string.format("(%s, %d, %d,%s,'%s','%s',%d,'%s')",
					   uuid,
					   millid,
					   kwatts_s,
					   date,
					   location,
					   continent,
					   active,
					   strrecordtype
					   )

  else 
	  query_values = string.format("(%d,%s, %d, %d,%s,'%s','%s',%d,'%s')",
					   id,
					   uuid,
					   millid,
					   kwatts_s,
					   date,
					   location,
					   continent,
					   active,
					   strrecordtype
					   )
	end
				   
	local query = query_prefix .. query_values
	return query			   
end


function execute_inserts()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.delete_inserts do
      local id = get_id()
      local query = get_query_insert(0, tnum)		
      con:query(query)
   end
end

-- Re-prepare statements if we have reconnected, which is possible when some of
-- the listed error codes are in the --mysql-ignore-errors list
function sysbench.hooks.before_restart_event(errdesc)
   if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
      errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
      errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
      errdesc.sql_errno == 2011    -- CR_TCP_CONNECTION
   then
      close_statements()
      prepare_statements()
   end
end

function check_reconnect()
   if sysbench.opt.reconnect > 0 then
      transactions = (transactions or 0) + 1
      if transactions % sysbench.opt.reconnect == 0 then
         close_statements()
         con:reconnect()
         prepare_statements()
      end
   end
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

function sysbench.hooks.report_cumulative(stat)
   if sysbench.opt.stats_format == "csv" then
      sysbench.report_cumulative_csv(stat)
   elseif sysbench.opt.stats_format == "json" then
      sysbench.report_cumulative_csv(stat)   
      -- sysbench.report_cumulative_json(stat)
   else
      sysbench.report_cumulative_default(stat)
   end
end

