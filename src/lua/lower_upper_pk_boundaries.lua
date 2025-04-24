#!/usr/bin/env sysbench
-- -------------------------------------------------------------------------- --
-- Bulk insert benchmark: do multi-row INSERTs concurrently in --threads
-- threads with each thread inserting into its own table. The number of INSERTs
-- executed by each thread is controlled by either --time or --events.
-- -------------------------------------------------------------------------- --
-- Command line options
sysbench.cmdline.options = {
    stats_format=
    {"Specify how you want the statistics written [default=human readable; csv; json ", "human"},
    table_size={"Specify the number of rows to be inserted", 2000000}
}

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
end

function prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   
   
   for i = 1, 2 do
      print("Creating table 'brange" .. i .. "'...")
      con:query(string.format([[
        CREATE TABLE IF NOT EXISTS brange%d (
          a INTEGER NOT NULL,
          b INTEGER DEFAULT '0' NOT NULL,
          PRIMARY KEY (a,b))]], i))
    end
    print("Filling table brange1")
    con:query("INSERT INTO brange1 VALUES (1, 1), (2, 2);")

    print("Filling table brange2")
    
    local reporter = 0
    local loop = (sysbench.opt.table_size / 10)
    
    local globalid =0
    for r = 1, loop do
        local insert = "INSERT INTO brange2 VALUES "
        for i = 1 , 10 do
         if i > 1 then
            insert = insert .. ","   
         end
         globalid = globalid + 1
         insert = insert .. "(2," .. globalid  ..")"
        end
        con:query(insert .. ";")

        reporter = (reporter + 1)
        if reporter >= 5000 then
            print(".... brange2 inserted records " .. (r * 10))
            reporter = 0
        end
    end
    con:query("analyze table brange2;")
end

function event()
    con:query("SELECT brange1.b FROM brange1 JOIN brange2 WHERE brange1.a = brange2.a AND brange2.a = 2 AND brange2.b >= brange1.b AND brange2.b <= brange1.b+2;")
    -- con:query("EXPLAIN FORMAT=TREE SELECT brange1.b FROM brange1 JOIN brange2 WHERE brange1.a = brange2.a AND brange2.a = 2 AND brange2.b >= brange1.b AND brange2.b <= brange1.b+2;")
end

function thread_done()
   con:disconnect()
end

function cleanup()
      local drv = sysbench.sql.driver()
   local con = drv:connect()
    for i = 1, 2 do
      print("Dropping table 'brange" .. i .. "'...")
      con:query("DROP TABLE IF EXISTS brange" .. i )
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
 