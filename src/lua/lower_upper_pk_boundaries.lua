#!/usr/bin/env sysbench
-- -------------------------------------------------------------------------- --
-- This test is designed to check a specific bug and feature (see https://bugs.mysql.com/bug.php?id=112737)
-- it works quering the primary key and giving the request for a specific range of records. 
-- when the query is executed the bounderies must appear also in the explain plan 
-- without the patch this doesn't happen as below:
-- EXPLAIN FORMAT=TREE
-- SELECT t1.b FROM t1 JOIN t2
--   WHERE t1.a = t2.a AND t2.a = 2 AND t2.b >= t1.b AND t2.b <= t1.b+2;

-- b<=4 was not added to RANGE SCAN
-- +----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
-- | EXPLAIN                                                                                                                                                                                          |
-- +----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
-- | -> Filter: ((t2.a = 2) and (t2.b >= '2') and (t2.b <= <cache>(('2' + 2))))  (cost=0.86 rows=3)
--     -> Covering index range scan on t2 using PRIMARY over (a = 2 AND 2 <= b)  (cost=0.86 rows=3)
--  |
-- +----------------------------------------------------------------------------------------------------------------------------------------------------------------------+

-- The test is required to run ONCE and with 1 thread
-- possible difference is the number of rows in the table so it is good to load a decent ammount of data like more than 1 million rows. 

-- -------------------------------------------------------------------------- --
-- Command line options
sysbench.cmdline.options = {
    stats_format=
    {"Specify how you want the statistics written [default=human readable; csv; json ", "human"},
    table_size={"Specify the number of rows to be inserted", 10000000},
    batch_insert_dimension={"if use batching insert instead one for insert comamnd. Default of the dimension is 0, \
    if a number is given that number will be used as dimension of the batch  ", 0},
    use_transactions={"Encapsulate the operations inside an esplicti transaction with Begin/commit",0}
}

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
end

function prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   
   
  print("Creating table 'brange1...")
  con:query(string.format([[
    CREATE TABLE IF NOT EXISTS brange1 (
      a INTEGER,
      b INTEGER,
      PRIMARY KEY (a))]]))

  print("Creating table 'brange2...")
  con:query(string.format([[
    CREATE TABLE IF NOT EXISTS brange2 (
      a INTEGER,
      b INTEGER,
      PRIMARY KEY (a,b))]]))


    print("Filling table brange1")
    con:query("INSERT INTO brange1 VALUES (1, 1), (2, 2);")

    print("Filling table brange2")
    
    local reporter = 0
    local loop = (sysbench.opt.table_size)
    local trx = (sysbench.opt.use_transactions)
    local batch_dimension = (sysbench.opt.batch_insert_dimension)
    local globalid =0
    
    local start_time = os.time()

    if batch_dimension == 0 then
         if trx > 0 then
            con:query("BEGIN;");
         end
         for r = 1, loop do
            local insert = "INSERT INTO brange2 VALUES "
               globalid = globalid + 1
               insert = insert .. "(2," .. globalid  ..")"
            con:query(insert .. ";")

            reporter = (reporter + 1)
            if reporter >= 100000 then
               if trx > 0 then
                  con:query("COMMIT;");
                  con:query("BEGIN;");
               end
               print_progress_bar(r,sysbench.opt.table_size)
               reporter = 0
            end
         end
         con:query("COMMIT;")
      else
         local loop = (sysbench.opt.table_size / batch_dimension)

         local globalid =0
         for r = 1, loop do
             local insert = "INSERT INTO brange2 VALUES "
             if trx > 0 then
               con:query("BEGIN;");     
             end
             for i = 1 , batch_dimension do
                  if i > 1 then
                     insert = insert .. ","   
                  end
                  globalid = globalid + 1
                  insert = insert .. "(2," .. globalid  ..")"
             end
             con:query(insert .. ";")
             if trx > 0 then
               con:query("COMMIT;");     
             end
             reporter = (reporter + 1)
             print_progress_bar(globalid,sysbench.opt.table_size)
         end
      end

      local elapsed_time = os.time() - start_time
      print("Data batch size: " .. sysbench.opt.batch_insert_dimension)
      print("Using transactions: " .. sysbench.opt.use_transactions)
      print("Data load total time taken(sec): " .. elapsed_time)
      print("Total records loaded: " .. sysbench.opt.table_size)
      print("records/s: " .. (sysbench.opt.table_size/elapsed_time))
      
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
 
 function print_progress_bar(current, total, width)
   width = width or 50 -- Default width if not provided
   local percent = math.floor((current / total) * 100)
   local filled = math.floor((current / total) * width)
   
   -- Build progress bar string
   local bar = "["
   for i = 1, filled do bar = bar .. "=" end
   for i = filled + 1, width do bar = bar .. " " end
   bar = bar .. "]"
   
   -- Print progress
   io.write(string.format("\r%s %3d%%", bar, percent))
   io.flush()
   
   if current == total then
       print() -- Newline when complete
   end
end