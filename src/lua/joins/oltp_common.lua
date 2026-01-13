-- Copyright (C) 2006-present Marco Tusa <tusamarco@gmail.com>

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

require("joins/join_queries")


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
   -- options that govern the joins tests [START]
   simple_inner_pk =
      {"Number of simple_inner joins by pk queries per transaction", 0},
   simple_inner_pk_GB =
      {"Number of simple_inner joins by pk queries with Group By per transaction", 0},
      multilevel_inner_pk =
      {"Number of multilevel_inner joins by pk queries per transaction", 0},  
   simple_inner_index =
      {"Number of simple_inner joins by index queries per transaction", 0},
   simple_inner_index_GB =
      {"Number of simple_inner joins by index queries with Group By per transaction", 0},
   multilevel_inner_index =
      {"Number of multilevel_inner joins by index queries per transaction", 0},
   simple_inner_forcing_order_GB = 
      {"Number of simple_inner joins by index queries with forcing order and Group By per transaction", 0},
   multilevel_inner_forcing_order_index =
      {"Number of multilevel_inner joins by index queries with forcing order per transaction", 0},
   simple_inner_straight_GB =
      {"Number of simple_inner joins by index queries with STRAIGHT_JOIN and Group By per transaction", 0},
   multilevel_inner_straight_index =
      {"Number of multilevel_inner joins by index queries with STRAIGHT_JOIN per transaction", 0},
   simple_left_pk =
      {"Number of simple_left joins by pk queries per transaction", 0},
   simple_left_pk_GB =
      {"Number of simple_left joins by pk queries with Group By per transaction", 0},
   multi_left_pk =
      {"Number of multilevel_left joins by pk queries per transaction", 0},
   simple_left_index =
      {"Number of simple_left joins by index queries per transaction", 0},
   simple_left_index_GB =
      {"Number of simple_left joins by index queries with Group By per transaction", 0},
   multi_left_index =
      {"Number of multilevel_left joins by index queries per transaction", 0},
   simple_left_forcing_order_GB =
      {"Number of simple_left joins by index queries with forcing order and Group By per transaction", 0},
   multi_left_forcing_order_GB =
      {"Number of multilevel_left joins by index queries with forcing order and Group By per transaction", 0},
   simple_left_straight =
      {"Number of simple_left joins by index queries with STRAIGHT_JOIN per transaction", 0},
   multi_left_straight =
      {"Number of multilevel_left joins by index queries with STRAIGHT_JOIN per transaction", 0},
   simple_left_exclude =
      {"Number of simple_left joins by index queries with exclude per transaction", 0},
   simple_right_pk =
      {"Number of simple_right joins by pk queries per transaction", 0},
   simple_right_pk_GB =
      {"Number of simple_right joins by pk queries with Group By per transaction", 0},
   multi_right_pk =
      {"Number of multilevel_right joins by pk queries per transaction", 0},
   simple_right_index =
      {"Number of simple_right joins by index queries per transaction", 0},
   simple_right_index_GB =
      {"Number of simple_right joins by index queries with Group By per transaction", 0},
   multi_right_index =
      {"Number of multilevel_right joins by index queries per transaction", 0},
   simple_right_forcing_order_GB =
      {"Number of simple_right joins by index queries with forcing order and Group By per transaction", 0},
   multi_right_forcing_order_GB =
      {"Number of multilevel_right joins by index queries with forcing order and Group By per transaction", 0},
   simple_right_straight_GB =
      {"Number of simple_right joins by index queries with STRAIGHT_JOIN and Group By per transaction", 0},
   multi_right_straight =
      {"Number of multilevel_right joins by index queries with STRAIGHT_JOIN per transaction", 0},
   inner_subquery_multi_pk =
      {"Number of inner_subquery joins by multi pk queries per transaction", 0},
   left_subquery_multi_pk =
      {"Number of left_subquery joins by multi pk queries per transaction", 0},
   right_subquery_multi_pk =
      {"Number of right_subquery joins by multi pk queries per transaction", 0},
   semi_join_exists_pk =
      {"Number of semi_join_exists joins by pk queries per transaction", 0},
   anti_join_not_exists_pk =
      {"Number of anti_join_not_exists joins by pk queries per transaction", 0},
   anti_join_left_join_pk =
      {"Number of anti_join_left_join joins by pk queries per transaction", 0},
   conditional_join_pk =
      {"Number of conditional_join joins by pk queries per transaction", 0},
   update_multi_right_join_pk =
      {"Number of update_multi_right_join by pk queries per transaction", 0},
   update_multi_left_join_pk =
      {"Number of update_multi_left_join by pk queries per transaction", 0},
   update_multi_inner_join_pk =
      {"Number of update_multi_inner_join by pk queries per transaction", 0},
   join_levels =
      {"[PLACE HOLDER THIS IS NOT ACTIVE YET] Number of levels to use in the joins tests (from 1 to 5)", 5},
  -- options that govern the joins tests [END]
   delete_inserts =
      {"Number of DELETE/INSERT combination per transaction", 0},
   index_updates =
      {"Number of index-based UPDATE statements per transaction", 0},
   auto_inc =
   {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
       "or its alternatives in other DBMS. When disabled, use " ..
       "client-generated IDs", true},
   skip_trx =
      {"Don't start explicit transactions and execute all queries " ..
          "in the AUTOCOMMIT mode", false},
   use_replace =
      {"Use replace instead Insert", false},
   no_primary_key =
      {"We will not create an explicit primary key on the tables. Keep in mind INNODB will generate it anyhow", false},
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
   {"Specify a table name instead main", "main"},
   stats_format=
   {"Specify how you want the statistics written [default=human (readable); csv; json] ", "human"},
--   create_indexes_before_dataload =
--   {"Create all imdexes before loading data. This can be useful when in the need to avoid the operation with table filled", false},
   chunk_size_in_prepare = 
      {"Split the data load by chunk instead loading all in one transactions. Using 0 means disable chunk," ..
         "Chunk size cannot be larger than 1/4 of the number of rows",0},
   debug_lua = 
      {"Enable debug messages during data preparation", 0}
}


-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
   load_global_variables()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = (sysbench.tid % sysbench.opt.threads + 1) + sysbench.opt.from_table, sysbench.opt.tables,
   sysbench.opt.threads do
      create_table_main(drv, con, i)
   end

   -- Creaate Level tables for a maximum of 5 levels
   for i = (sysbench.tid % sysbench.opt.threads + 1), 5, sysbench.opt.threads do
      create_table_level(drv, con, i)
   end

end

-- Preload the dataset into the server cache. This command supports parallel
-- execution, i.e. will benefit from executing with --threads > 1 as long as
-- --tables > 1
--
-- PS. Currently, this command is only meaningful for MySQL/InnoDB benchmarks
function cmd_warmup()
   load_global_variables()
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
   end

   for i = sysbench.tid % sysbench.opt.threads + 1, 5,
   sysbench.opt.threads do
      local t = "level" .. i
      print("Preloading table level" .. i)
      con:query("ANALYZE TABLE ".. t)
      con:query(string.format(
                   "SELECT AVG(id) FROM " ..
                      "(SELECT * FROM %s FORCE KEY (PRIMARY) " ..
                      "LIMIT %u) t",
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


-- Function to create the level table and populate it with data
function create_table_level(drv, con, table_num)
   local engine_def = ""
   local query
   local chunck = 0
   local rows = sysbench.opt.table_size 
   local table_name_level = "level"

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
   
   if drv:name() == "mysql" 
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

   print(string.format("Creating table '%s%d'...", table_name_level,table_num))
   

   --print("DEBUG TABLE OPTION" .. sysbench.opt.mysql_table_options)
   
   if sysbench.opt.from_row < 2 then
      con:query(string.format([[DROP TABLE IF EXISTS %s%d]],table_name_level, table_num))
   
      local primaryKeyDefinition = ", PRIMARY KEY (`id`)"
      
      if sysbench.opt.no_primary_key and not sysbench.opt.auto_inc then
         primaryKeyDefinition = ""
      end

      query = string.format([[   
      CREATE TABLE `%s%d` (
      `id` %s,
      continent VARCHAR(45) NOT NULL,
      parent_id BIGINT,  -- For hierarchical structure if needed
      time_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      l1_id INT,
      l2_id INT,
      l3_id INT,
      l4_id INT,
      l5_id INT,    
      record_name CHAR(36),
      record_code CHAR(5),
      record_value BIGINT,
      record_status ENUM('active', 'inactive', 'pending'),
      record_priority INT NOT NULL,
      INDEX idx_country (continent),
      INDEX idx_parent_id (parent_id),
      INDEX idx_l1_id (l1_id),
      INDEX idx_l2_id (l2_id),
      INDEX idx_l3_id (l3_id),
      INDEX idx_l4_id (l4_id),
      INDEX idx_l5_id (l5_id),
      INDEX idx_time_accessed (time_accessed),
      INDEX idx_record_status (record_status),
      INDEX idx_record_priority (record_priority),
      INDEX comp_record_continent_status_priority(continent,record_status,record_priority)

      
      -- Foreign key constraints (commented - enable when tables exist)
      -- FOREIGN KEY (parent_id) REFERENCES level1(id) ON DELETE CASCADE,
      -- FOREIGN KEY (l2_id) REFERENCES level2(id) ON DELETE SET NULL,
      -- FOREIGN KEY (l3_id) REFERENCES level3(id) ON DELETE SET NULL,
      -- FOREIGN KEY (l4_id) REFERENCES level4(id) ON DELETE SET NULL,
      -- FOREIGN KEY (l5_id) REFERENCES level5(id) ON DELETE SET NULL
) %s, %s]],
   table_name_level, table_num, id_def .. primaryKeyDefinition,engine_def, extra_table_options)      
      
      --print("DEBUG :" .. query)
      con:query(query)
      
   end   

   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into '%s%d'",
                          sysbench.opt.table_size, table_name_level, table_num))
   end

   local query_pre = ""
   local auto_inc = 0
   if sysbench.opt.auto_inc then
      query_pre = string.format(query_level_pre_auto_global, table_name_level, table_num)
      auto_inc = 1
   else
      query_pre = string.format(query_level_pre_global, table_name_level, table_num)
   end
   con:bulk_insert_init(query_pre)
   
   local row_counter = 0
   local start_row = sysbench.opt.from_row
   local end_row = sysbench.opt.table_size
   local report_interval = sysbench.opt.report_loaded_rows_every
   local report_counter = 0

   if start_row < 2 and rows > 1 then 
      end_row = end_row - 1
   end

   print(string.format("Start filling tables from row# %d to row# %d ", sysbench.opt.from_row, sysbench.opt.table_size))

   con:bulk_insert_init(query_pre)
   query = ""

   for i = start_row, end_row do

      -- Generate the values for the insert query
      query = initialize_values_level(i,auto_inc)

--      print("DEBUG query: " .. query_pre .. query)

      con:bulk_insert_next(query)

      if report_interval > 0 then
         report_counter = report_counter + 1
         if report_counter >= report_interval then
            report_counter = 0
            print(string.format("Data for table '%s%d' inserted rows {%d}", table_name_level, table_num, i))
         end
      end 

      if chunck > 0 then
         row_counter = row_counter + 1

         if row_counter >= chunck then
            row_counter = 0
            con:bulk_insert_done()
            con:query("COMMIT")
            print(string.format("Flushing chunk data for table '%s%d' inserted rows {%d}", table_name_level, table_num, i))
            con:bulk_insert_init(query_pre)
         end
      end
   end

   con:bulk_insert_done()
   con:query("ANALYZE TABLE " .. table_name_level .. table_num)

end


-- Function to create the main table and populate it with data
function create_table_main(drv, con, table_num)
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
   
   if drv:name() == "mysql" 
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
      l1_id INT,           -- Foreign key to level1.id
      l2_id INT,           -- Foreign key to level2.id
      l3_id INT,           -- Foreign key to level3.id
      l4_id INT,           -- Foreign key to level4.id
      l5_id INT,           -- Foreign key to level5.id
      
      -- Numeric data types
      small_number SMALLINT,
      integer_number INT,
      myvalue BIGINT,
      decimal_number DECIMAL(10, 2),
      float_number FLOAT,

      -- String data types
      char_field CHAR(10),
      varchar_field VARCHAR(255),
      color VARCHAR(50),
      continent VARCHAR(255),
      uuid VARCHAR(36) CHARACTER SET latin1,
      uuid_bin BINARY(16),
      text_field TEXT,
      
      -- Date and time data types
      datetime_field DATETIME,
      timestamp_field TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      year_field YEAR,
      
      -- Binary data types
      binary_field BINARY(50),
      varbinary_field VARBINARY(255),
      
      -- Special data types
      enum_field ENUM('active', 'inactive', 'pending'),
      set_field SET('read', 'write', 'execute', 'delete'),
      
      -- Boolean type
      is_active BOOLEAN DEFAULT TRUE,
      
      -- Spatial data type (if using GIS)
      -- point_field POINT,
      
      -- Constraints
      UNIQUE KEY unique_varchar (uuid),
      INDEX idx_l1_id (l1_id),
      INDEX idx_l2_id (l2_id),
      INDEX idx_l3_id (l3_id),
      INDEX idx_l4_id (l4_id),
      INDEX idx_l5_id (l5_id),
      INDEX idx_date (datetime_field),
      INDEX idx_time (timestamp_field),
      INDEX idx_enum_field (enum_field),
      INDEX idx_set_field (set_field),
      INDEX idx_year_field (year_field),
      INDEX comp_attributes(continent,enum_field,set_field),
      INDEX comp_color(color,continent,enum_field,year_field)
      
      -- Foreign key constraints (commented out - enable as needed)
      -- FOREIGN KEY (l1_id) REFERENCES level1(id),
      -- FOREIGN KEY (l2_id) REFERENCES level2(id),
      -- FOREIGN KEY (l3_id) REFERENCES level3(id),
      -- FOREIGN KEY (l4_id) REFERENCES level4(id),
      -- FOREIGN KEY (l5_id) REFERENCES level5(id),
      
      -- CONSTRAINT chk_numbers CHECK (small_number >= 0)
) %s, %s]],
   sysbench.opt.table_name, table_num, id_def .. primaryKeyDefinition,engine_def, extra_table_options)      
      
      --print("DEBUG :" .. query)
      con:query(query)
      
   end   

   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into '%s%d'",
                          sysbench.opt.table_size, sysbench.opt.table_name, table_num))
   end

   local query_pre = ""
   local auto_inc = 0
   if sysbench.opt.auto_inc then
      query_pre = string.format(query_main_pre_auto_global, sysbench.opt.table_name, table_num)
      auto_inc = 1
   else
      query_pre = string.format(query_main_pre_global, sysbench.opt.table_name, table_num)
   end

   con:bulk_insert_init(query_pre)
   
   local row_counter = 0
   local start_row = sysbench.opt.from_row
   local end_row = sysbench.opt.table_size
   local report_interval = sysbench.opt.report_loaded_rows_every
   local report_counter = 0

   if start_row < 2 and rows > 1 then 
      end_row = end_row - 1
   end

   print(string.format("Start filling tables from row# %d to row# %d ", sysbench.opt.from_row, sysbench.opt.table_size))

   con:bulk_insert_init(query_pre)
   query = ""

   for i = start_row, end_row do

      -- Generate the values for the insert query
      query = initialize_values_main(i,auto_inc)

      if sysbench.opt.debug_lua > 0 then
            print("DEBUG query: " .. query_pre .. query)
      end
      -- error("Givwe me a break")

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

   con:query("ANALYZE TABLE " .. sysbench.opt.table_name .. table_num)

end

-- Initialize the value for each field and return the string to be used in the insert query
function initialize_values_level(i,auto_inc)
 local query = ""
   id = i
   continent = get_continent()
   parent_id = sysbench.rand.default(1,sysbench.opt.table_size)
   time_accessed = "NOW()"
   l1_id = sysbench.rand.default(1,sysbench.opt.table_size)
   l2_id = sysbench.rand.default(1,sysbench.opt.table_size)
   l3_id = sysbench.rand.default(1,sysbench.opt.table_size)
   l4_id = sysbench.rand.default(1,sysbench.opt.table_size)
   l5_id = sysbench.rand.default(1,sysbench.opt.table_size)    
   record_name = "UUID()"
   record_code = get_record_code()
   record_value = sysbench.rand.default(0,2147483647)
   record_status = get_record_status()  
   record_priority = sysbench.rand.default(1,10)
   
   if (auto_inc == 1) then
         query = string.format("('%s', %d, %s, %d, %d, %d, %d,%d,%s, '%s', %d, '%s', %d)",
            continent,
            parent_id,
            time_accessed,
            l1_id,
            l2_id,
            l3_id,
            l4_id,
            l5_id,
            record_name,
            record_code,
            record_value,
            record_status,
            record_priority
         )
   else
         query = string.format("(%d, '%s', %d, %s, %d, %d,%d, %d, %d, %s, '%s', %d, '%s', %d)",
            id,
            continent,
            parent_id,
            time_accessed,
            l1_id,
            l2_id,
            l3_id,
            l4_id,
            l5_id,
            record_name,
            record_code,
            record_value,
            record_status,
            record_priority
         )
   end
   
   return query

end

-- Initialize the value for each field and return the string to be used in the insert query
function initialize_values_main(i,auto_inc)
 local query = ""
   id = i
   l1_id = sysbench.rand.default(1,sysbench.opt.table_size)
   l2_id = sysbench.rand.default(1,sysbench.opt.table_size)
   l3_id = sysbench.rand.default(1,sysbench.opt.table_size)
   l4_id = sysbench.rand.default(1,sysbench.opt.table_size)
   l5_id = sysbench.rand.default(1,sysbench.opt.table_size)
   small_number = sysbench.rand.default(0,32767)
   integer_number = sysbench.rand.default(0,2147483647)
   myvalue = sysbench.rand.default(0,2147483647)
   decimal_number = sysbench.rand.default(0,99999999)
   float_number = sysbench.rand.default(0,34028234663852886)
   char_field = sysbench.rand.varstringalpha(0,10)
   varchar_field = sysbench.rand.varstringalpha(0,255)
   color = get_color()
   continent = get_continent()
   uuid = "REVERSE(UUID())"
   uuid_bin = "UNHEX(REPLACE(UUID(),'-',''))"
   text_field = sysbench.rand.varstringalpha(0,6)
   date = "NOW()"
   timestamp_field = "NOW()"
   year_field = sysbench.rand.default(1999,2035)
   binary_field = sysbench.rand.varstringalpha(5, 50) 
   varbinary_field = sysbench.rand.varstringalpha(5, 50)
   enum_field = get_record_status()  
   set_field = get_field()
   is_active = sysbench.rand.default(0,1)
   
                                                                                                                                 
   if (auto_inc == 1) then
      -- "(l1_id, l2_id, l3_id, l4_id, l5_id, small_number, integer_number, myvalue, decimal_number, float_number,  char_field, varchar_field, color, continent, uuid, uuid_bin, text_field, datetime_field, timestamp_field, year_field, binary_field, varbinary_field, blob_field, medium_blob, long_blob, enum_field, set_field, is_active)
      
      query = string.format("(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, '%s', '%s', '%s', '%s', %s, %s, '%s', %s, %s, %d, '%s', '%s','%s', '%s', %d)",
                           l1_id,
                           l2_id,
                           l3_id,
                           l4_id,
                           l5_id,
                           small_number,
                           integer_number,
                           myvalue,
                           decimal_number,
                           float_number,
                           char_field,
                           varchar_field,
                           color,
                           continent,
                           uuid,
                           uuid_bin,
                           text_field,
                           date,
                           timestamp_field,
                           year_field,
                           binary_field,
                           varbinary_field,
                           enum_field,
                           set_field,
                           is_active
                              )
   else
      -- "(id, l1_id, l2_id, l3_id, l4_id, l5_id, small_number, integer_number, myvalue, decimal_number, float_number,  char_field, varchar_field, continent, uuid, uuid_bin, text_field, datetime_field, timestamp_field, year_field, binary_field, varbinary_field, blob_field, medium_blob, long_blob, enum_field, set_field, json_field, is_active)
      query = string.format("(%d,%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, '%s', '%s', '%s', '%s', %s, %s, '%s', %s, %s, %d, '%s', '%s','%s', '%s', %d)",
                              i,
                           l1_id,
                           l2_id,
                           l3_id,
                           l4_id,
                           l5_id,
                           small_number,
                           integer_number,
                           myvalue,
                           decimal_number,
                           float_number,
                           char_field,
                           varchar_field,
                           color,
                           continent,
                           uuid,
                           uuid_bin,
                           text_field,
                           date,
                           timestamp_field,
                           year_field,
                           binary_field,
                           varbinary_field,
                           enum_field,
                           set_field,
                           is_active
                              )
   end

   return query
end

-- Helper functions to get random values for specific fields
function get_continent()
     return getRandomValue("Africa, Antarctica, Asia, Europe, North America, Oceania, South America")
end
function get_record_status()
     return getRandomValue("active,inactive,pending")
end
function get_color()
     return getRandomValue("red, blue, yellow, green, orange, purple, black, white, brown, gray, pink, teal, magenta, cyan, maroon, indigo, lavender, turquoise, gold, silver")  
end
function get_field()
     return getRandomValue("read,write,execute,delete") 
end
function get_record_code()
       return getRandomValue("ZKLPM, TRXQD, BVHNY, FWJGS, MCPRD, KQWLN, XJTHF, GBNYS, PDQMZ, HRTVK, LSZQF, YWNXM, EFKPT, AVCRG, IBODU, JINSE, RTGOL, HMYZX, CKVAP, QFJWB, NUEMD, WXSIZ, OLTYG, VMBPJ, SAEQK, DNRHU, FTGIV, RYWCL, PHZOX, KSBMA, EJGNQ, WLVRD, QTCHU, IPBFS, NZMOY, XAGPK, UDRJV, COWZS, HETFN, BLIYG, MKRXQ, PVSAE, DJNUT, ZOCFW, GYHVQ, XQBTK, LISUP, WENMF, CZRAJ, TDVGH") 
end   

-- Function to get a random value from a comma-separated string
function getRandomValue(csvString)
    -- Split the comma-separated string
    local values = {}
    for value in csvString:gmatch("([^,]+)") do
        table.insert(values, value:match("^%s*(.-)%s*$"))
    end
    
    if #values == 0 then
        return nil
    end
    
    -- Better random seeding (only once at program start)
    if not _randomSeeded then
        math.randomseed(os.time() * 1000)
        _randomSeeded = true
    end
    
    return values[math.random(1, #values)]
end

-- Function to split a comma-separated string into a table of values
function split_comma_separated_string(inputString)
    local result = {}
    for value in inputString:gmatch("([^,]+)") do
        table.insert(result, value:match("^%s*(.-)%s*$"))
    end
    return result
end

function prepare_begin()
   stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
   stmt.commit = con:prepare("COMMIT")
end

-- Initialize thread-specific variables and prepare statements
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
   load_global_variables()
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
   for i = 1, 5 do
      print(string.format("Dropping table '%s%d'...", 'level',i))
      con:query("DROP TABLE IF EXISTS " .. 'level' .. i )
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

-- *****************************************************************
-- Functions to execute the JOINS


-- Generic join executor
function execute_joins(join_name)
   local i

   if join_name:find("exclude") then
      for i = 1, sysbench.opt[join_name] do
         local tnum = get_table_num()
         query = string.format(query_map[join_name .. "_query"], sysbench.opt.table_name, tnum, get_record_status(), get_color())
         -- print("DEBUG JOIN QUERY B: " .. query .." Join Name: " .. join_name)
         con:query(query)
      end
   elseif join_name:find("forcing_order") then
         local tnum = get_table_num()
         local tablename = sysbench.opt.table_name .. tnum
         query = string.format(query_map[join_name .. "_query"], tablename, tablename, get_record_status(), get_continent())
         -- print("DEBUG JOIN QUERY : " .. query .." Join Name: " .. join_name)
         con:query(query)
   elseif join_name:find("update_multi") then
      for i = 1, sysbench.opt[join_name] do
         query = get_update_multi(join_name)
         -- print("DEBUG JOIN QUERY A: " .. query .." Join Name: " .. join_name)
         con:query(query)
      end   
   else
      for i = 1, sysbench.opt[join_name] do
         local tnum = get_table_num()
         query = string.format(query_map[join_name .. "_query"], sysbench.opt.table_name, tnum, get_record_status(), get_continent())
         -- print("DEBUG JOIN QUERY : " .. query .." Join Name: " .. join_name)
         con:query(query)
      end
   end
end

-- *****************************************************************
-- Function to get the multi update query for different types of multi updates
function get_update_multi(join_name)
   local query = ""
   local tnum = get_table_num()
   local table_name = sysbench.opt.table_name .. tnum

   if join_name:find("update_multi_right_join_pk") then
       query = string.format(query_map[join_name .. "_query"],
         table_name, 
         table_name, 
         get_record_status(),
         get_record_status(),
         get_record_status(),
         get_record_status(),
         table_name,
         table_name,
         table_name,
         get_continent(),
         table_name,
         table_name,
         table_name
         )
   else 
      query = string.format(query_map[join_name .. "_query"],
         table_name,
         table_name,
         table_name,
         table_name,
         get_record_status(),
         get_record_status(),
         get_record_status(),
         get_record_status(),
         table_name,
         get_continent(),
         table_name,
         table_name,
         table_name
         )
      end
   return query
end

function execute_index_updates()
    local tnum = get_table_num()
    local table_name = sysbench.opt.table_name
    local query_update = "update %s%u set varchar_field='%s' where color='%s' and continent='%s' and enum_field='%s' limit 10"
    for i = 1, sysbench.opt.index_updates do
         local new_varchar = sysbench.rand.varstringalpha(5, 255)
         local color = get_color()
         local continent = get_continent()
         local enum_field = get_record_status()
         
         local query = string.format(query_update,
                                    table_name,
                                    tnum,
                                    new_varchar,
                                    color,
                                    continent,
                                    enum_field
                                    )
         -- print("DEBUG INDEX UPDATE QUERY : " .. query)
         con:query(query)

    end
 end

 
function execute_delete_inserts()
   local tnum = get_table_num()
   local table_name = sysbench.opt.table_name 

   for i = 1, sysbench.opt.delete_inserts do
      local id = get_id()
      local query_delete = string.format("DELETE FROM %s%u WHERE id=%d", table_name, tnum, id)
      
      local query_prefix= string.format(query_main_pre_global, table_name, tnum) 	 
      local query_values = initialize_values_main(id,0)
      local query_insert = query_prefix .. query_values .. " ON DUPLICATE KEY UPDATE varchar_field=varchar_field"
      -- print ("DEBUG DELETE INSERT QUERY DELETE: " .. query_delete)
      -- print ("DEBUG DELETE INSERT QUERY INSERT: " .. query_insert)   
      con:query(query_delete)
      con:query(query_insert)
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
      load_global_variables()
   end
end

function check_reconnect()
   if sysbench.opt.reconnect > 0 then
      transactions = (transactions or 0) + 1
      if transactions % sysbench.opt.reconnect == 0 then
         close_statements()
         con:reconnect()
         prepare_statements()
         load_global_variables()
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

