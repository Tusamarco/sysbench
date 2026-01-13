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
-- Common code for join queries
-- -----------------------------------------------------------------------------
function load_global_variables()
-- GLOBAL VARIABLES
   query_main_pre_auto_global = "INSERT INTO %s%u(l1_id, l2_id, l3_id, l4_id, l5_id, small_number, integer_number, myvalue, decimal_number, float_number,  char_field, varchar_field,color, continent, uuid, uuid_bin, text_field, datetime_field, timestamp_field, year_field, binary_field, varbinary_field, enum_field, set_field, is_active) VALUES"

   query_main_pre_global = "INSERT INTO %s%u(id, l1_id, l2_id, l3_id, l4_id, l5_id, small_number, integer_number, myvalue, decimal_number, float_number,  char_field, varchar_field, color, continent, uuid, uuid_bin, text_field, datetime_field, timestamp_field, year_field, binary_field, varbinary_field, enum_field, set_field, is_active) VALUES"

   query_level_pre_auto_global = "INSERT INTO %s%u(continent, parent_id, time_accessed, l1_id, l2_id, l3_id, l4_id, l5_id, record_name, record_code, record_value, record_status, record_priority) VALUES"
   query_level_pre_global = "INSERT INTO %s%u(id, continent, parent_id, time_accessed, l1_id, l2_id, l3_id, l4_id, l5_id, record_name, record_code, record_value, record_status, record_priority) VALUES"

-- END GLOBAL VARIABLES
end




-- Join SQL code and mappings
-- *****************************************************************
-- ** INNER JOIN QUERIES **
inner_queries = { 
    ["simple_inner_pk_query"] = [[SELECT m.continent,year_field, m.enum_field, level1.record_value l1
FROM %s%u as m
INNER JOIN level1 ON m.l1_id = level1.id and level1.record_status = '%s'
WHERE m.continent = '%s'
ORDER BY m.year_field DESC, l1 DESC
LIMIT 100;]],
    
    ["simple_inner_pk_GB_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM %s%u as m
INNER JOIN level1 ON m.l1_id = level1.id and level1.record_status = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multilevel_inner_pk_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy,
m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1,SUM(level2.record_value) l2,
SUM(level3.record_value) l3,SUM(level3.record_value) l3,SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM %s%u as m
INNER JOIN level1 ON m.l1_id = level1.id and level1.record_status = '%s'
INNER JOIN level2 ON level1.l2_id = level2.id
INNER JOIN level3 ON level2.l3_id = level3.id
INNER JOIN level4 ON level3.l4_id = level4.id
INNER JOIN level5 ON level4.l5_id = level5.id
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_inner_index_query"] = [[SELECT m.continent,year_field, m.enum_field, level1.record_value l1
FROM %s%u as m
INNER JOIN level1 ON m.id = level1.parent_id and level1.record_status = '%s'
WHERE m.continent = '%s'
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_inner_index_GB_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM %s%u as m
INNER JOIN level1 ON m.id = level1.parent_id and level1.record_status = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multilevel_inner_index_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs,
SUM(level1.record_value) l1,SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,
SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM %s%u as m
INNER JOIN level1 ON m.id = level1.parent_id and level1.record_status = '%s'
INNER JOIN level2 ON level1.id = level2.parent_id
INNER JOIN level3 ON level2.id = level3.parent_id
INNER JOIN level4 ON level3.id = level4.parent_id
INNER JOIN level5 ON level4.id = level5.parent_id
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_inner_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(level1,%s%u) */ m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM %s%u as m
INNER JOIN level1 ON m.id = level1.parent_id and level1.record_status = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multilevel_inner_forcing_order_index_query"] = [[SELECT /*+ JOIN_ORDER(level2,level3,level4,level5,%s%u,level1) */ m.continent, count(m.continent) cc,
year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1,
SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM %s%u as m
INNER JOIN level1 ON m.id = level1.parent_id and level1.record_status = '%s'
INNER JOIN level2 ON m.id = level2.parent_id
INNER JOIN level3 ON m.id = level3.parent_id
INNER JOIN level4 ON m.id = level4.parent_id
INNER JOIN level5 ON m.id = level5.parent_id
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_inner_straight_GB_query"] = [[SELECT STRAIGHT_JOIN m.continent, count(m.continent) cc,
year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM level1
INNER JOIN %s%u ON m.id = level1.parent_id and level1.record_status = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multilevel_inner_straight_index_query"] = [[SELECT STRAIGHT_JOIN m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs,
SUM(level1.record_value) l1,SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM level1
INNER JOIN %s%u ON m.id = level1.parent_id and level1.record_status = '%s'
INNER JOIN level2 ON m.id = level2.parent_id
INNER JOIN level3 ON m.id = level3.parent_id
INNER JOIN level4 ON m.id = level4.parent_id
INNER JOIN level5 ON m.id = level5.parent_id
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]]
}

-- *****************************************************************

-- ** LEFT JOIN QUERIES **
left_queries = {
    ["simple_left_pk_query"] = [[SELECT m.continent,year_field, m.enum_field, level1.record_value l1
FROM %s%u as m
LEFT JOIN level1 ON m.l1_id = level1.id and m.enum_field = '%s'
WHERE m.continent = '%s'
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_left_pk_GB_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM %s%u as m
LEFT JOIN level1 ON m.l1_id = level1.id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_left_pk_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs,
SUM(level1.record_value) l1,SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,
SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM %s%u as m
LEFT JOIN level1 ON m.l1_id = level1.id and m.enum_field = '%s'
LEFT JOIN level2 ON level1.l2_id = level2.id
LEFT JOIN level3 ON level2.l3_id = level3.id
LEFT JOIN level4 ON level3.l4_id = level4.id
LEFT JOIN level5 ON level4.l5_id = level5.id
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_left_index_query"] = [[SELECT m.continent,year_field, m.enum_field, level1.record_value l1
FROM %s%u as m
LEFT JOIN level1 ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_left_index_GB_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM %s%u as m
LEFT JOIN level1 ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_left_index_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs,
SUM(level1.record_value) l1,SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,
SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM %s%u as m
LEFT JOIN level1 ON m.id = level1.parent_id and m.enum_field = '%s'
LEFT JOIN level2 ON level1.id = level2.parent_id
LEFT JOIN level3 ON level2.id = level3.parent_id
LEFT JOIN level4 ON level3.id = level4.parent_id
LEFT JOIN level5 ON level4.id = level5.parent_id
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_left_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(level1,%s%u) */ m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM %s%u as m
LEFT JOIN level1 ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_left_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(level2,%s%u,level1,level3,level4,level5) */ m.continent, count(m.continent) cc,year_field,
count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1,SUM(level2.record_value) l2,
SUM(level3.record_value) l3,SUM(level3.record_value) l3,SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM %s%u as m
LEFT JOIN level2 ON m.id = level2.parent_id and m.enum_field = '%s'
LEFT JOIN level1 ON m.id = level1.parent_id
LEFT JOIN level3 ON m.id = level3.parent_id
LEFT JOIN level4 ON m.id = level4.parent_id
LEFT JOIN level5 ON m.id = level5.parent_id
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_left_straight_query"] = [[SELECT STRAIGHT_JOIN m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM level1
LEFT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_left_straight_query"] = [[SELECT STRAIGHT_JOIN m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs,
SUM(level1.record_value) l1,SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,
SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM level2
LEFT JOIN %s%u as m ON m.id = level2.parent_id and m.enum_field = '%s'
LEFT JOIN level1 ON m.id = level1.parent_id
LEFT JOIN level3 ON m.id = level3.parent_id
LEFT JOIN level4 ON m.id = level4.parent_id
LEFT JOIN level5 ON m.id = level5.parent_id
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_left_exclude_query"] = [[SELECT m.continent continent, count(m.id) null_val
FROM %s%u as m LEFT JOIN level1 ON m.l1_id = level1.id and m.enum_field = '%s'
WHERE m.color='%s' and level1.id IS NULL
Group By m.continent;]]
}

-- *****************************************************************


-- ** RIGHT JOIN QUERIES **

right_queries = {
    ["simple_right_pk_query"] = [[SELECT m.continent,year_field, m.enum_field, level1.record_value l1
FROM level1
RIGHT JOIN %s%u as m ON m.l1_id = level1.id AND m.enum_field = '%s'
WHERE level1.continent = '%s'
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_right_pk_GB_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM level1
RIGHT JOIN %s%u as m ON m.l1_id = level1.id AND m.enum_field = '%s'
WHERE level1.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_right_pk_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs,
SUM(level1.record_value) l1,SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM level5
RIGHT JOIN level4 ON level5.l4_id = level4.id
RIGHT JOIN level3 ON level4.l3_id = level3.id
RIGHT JOIN level2 ON level3.l2_id = level2.id
RIGHT JOIN level1 ON level2.l1_id = level1.id
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_right_index_query"] = [[SELECT m.continent,year_field, m.enum_field, level1.record_value l1
FROM level1
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE level1.continent = '%s'
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_right_index_GB_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM level1
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE level1.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_right_index_query"] = [[SELECT m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1,
SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM level5
RIGHT JOIN level4 ON level5.id = level4.parent_id
RIGHT JOIN level3 ON level4.id = level3.parent_id
RIGHT JOIN level2 ON level3.id = level2.parent_id
RIGHT JOIN level1 ON level2.id = level1.parent_id
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_right_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(%s%u,level1) */ m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM level1
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE level1.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_right_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(%s%u,level1,level2,level3,level4,level5) */ m.continent, count(m.continent) cc,year_field,
count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1,SUM(level2.record_value) l2,
SUM(level3.record_value) l3,SUM(level3.record_value) l3,SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM level5
RIGHT JOIN level4 ON level5.id = level4.parent_id
RIGHT JOIN level3 ON level4.id = level3.parent_id
RIGHT JOIN level2 ON level3.id = level2.parent_id
RIGHT JOIN level1 ON level2.id = level1.parent_id
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["simple_right_straight_GB_query"] = [[SELECT STRAIGHT_JOIN m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM level1
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_right_straight_query"] = [[SELECT STRAIGHT_JOIN m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs,
SUM(level1.record_value) l1,SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,
SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM level5
RIGHT JOIN level4 ON level5.id = level4.parent_id
RIGHT JOIN level3 ON level4.id = level3.parent_id
RIGHT JOIN level2 ON level3.id = level2.parent_id
RIGHT JOIN level1 ON level2.id = level1.parent_id
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]]
}
-- END RIGHT JOIN QUERIES **


-- Join with a subquery
subquery_queries = {
 ["inner_subquery_multi_pk_query"] = [[SELECT 
    m.continent, 
    COUNT(m.continent) AS cc,
    m.year_field,
    COUNT(m.year_field) AS cy, 
    m.enum_field, 
    COUNT(m.enum_field) AS cs, 
    SUM(l1.record_value) AS l1,
    SUM(l2.record_value) AS l2,
    SUM(l3.record_value) AS l3,
    SUM(l4.record_value) AS l4,
    SUM(l5.record_value) AS l5
FROM 
    (SELECT l1_id,continent,year_field,enum_field FROM %s%u WHERE continent = '%s' and enum_field = '%s') AS m
    INNER JOIN (SELECT id,l2_id,record_value FROM level1) AS l1 ON m.l1_id = l1.id
    INNER JOIN level2 AS l2 ON l1.l2_id = l2.id
    INNER JOIN level3 AS l3 ON l2.l3_id = l3.id
    INNER JOIN level4 AS l4 ON l3.l4_id = l4.id
    INNER JOIN level5 AS l5 ON l4.l5_id = l5.id
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],

["left_subquery_multi_pk_query"] = [[SELECT 
    m.continent, 
    COUNT(m.continent) AS cc,
    m.year_field,
    COUNT(m.year_field) AS cy, 
    m.enum_field, 
    COUNT(m.enum_field) AS cs, 
    SUM(l1.record_value) AS l1,
    SUM(l2.record_value) AS l2,
    SUM(l3.record_value) AS l3,
    SUM(l4.record_value) AS l4,
    SUM(l5.record_value) AS l5
FROM 
    (SELECT l1_id,continent,year_field,enum_field FROM %s%u WHERE continent = '%s' and enum_field = '%s') AS m
    LEFT JOIN (SELECT id,l2_id,record_value FROM level1) AS l1 ON m.l1_id = l1.id
    LEFT JOIN level2 AS l2 ON l1.l2_id = l2.id
    LEFT JOIN level3 AS l3 ON l2.l3_id = l3.id
    LEFT JOIN level4 AS l4 ON l3.l4_id = l4.id
    LEFT JOIN level5 AS l5 ON l4.l5_id = l5.id
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],

["right_subquery_multi_pk_query"] = [[SELECT 
    m.continent, 
    COUNT(m.continent) AS cc,
    m.year_field,
    COUNT(m.year_field) AS cy, 
    m.enum_field, 
    COUNT(m.enum_field) AS cs, 
    SUM(l1.record_value) AS l1,
    SUM(l2.record_value) AS l2,
    SUM(l3.record_value) AS l3,
    SUM(l4.record_value) AS l4,
    SUM(l5.record_value) AS l5
FROM 
    level5 AS l5 
    RIGHT JOIN level4 AS l4 ON l5.l4_id = l4.id
    RIGHT JOIN level3 AS l3 ON l4.l3_id = l3.id
    RIGHT JOIN level2 AS l2 ON l3.l2_id = l2.id
    RIGHT JOIN level1 AS l1 ON l2.l1_id = l1.id
    RIGHT JOIN (SELECT l1_id,continent,year_field,enum_field FROM %s%u WHERE continent = '%s' and enum_field = '%s') AS m ON l1.parent_id = m.l1_id
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]]
}

semi_anti_condition_queries = {
 ["semi_join_exists_pk_query"] = [[SELECT 
    m.continent, 
    COUNT(m.continent) AS cc,
    m.year_field,
    COUNT(m.year_field) AS cy, 
    m.enum_field, 
    COUNT(m.enum_field) AS cs
FROM %s%u m
WHERE EXISTS (
    SELECT record_value
    FROM level1 l1 
    WHERE l1.id = m.l1_id
    AND l1.record_status = '%s'
) 
AND m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],

["anti_join_not_exists_pk_query"] = [[SELECT 
    m.continent, 
    COUNT(m.continent) AS cc,
    m.year_field,
    COUNT(m.year_field) AS cy, 
    m.enum_field, 
    COUNT(m.enum_field) AS cs
FROM %s%u m
WHERE NOT EXISTS (
    SELECT record_value
    FROM level1 l1 
    WHERE l1.id = m.l1_id
    AND l1.record_status = '%s'
) 
AND m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],

["anti_join_left_join_pk_query"] = [[SELECT 
    m.continent, 
    COUNT(m.continent) AS cc,
    m.year_field,
    COUNT(m.year_field) AS cy, 
    m.enum_field, 
    COUNT(m.enum_field) AS cs
FROM %s%u m
LEFT JOIN level1 l ON m.l1_id = l.id AND m.enum_field = '%s'
WHERE l.id IS NULL
AND m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],

["conditional_join_pk_query"] = [[SELECT 
     m.continent, 
     COUNT(m.continent) AS cc,
     m.year_field,
     COUNT(m.year_field) AS cy, 
     m.enum_field, 
     COUNT(m.enum_field) AS cs,
     SUM(CASE WHEN l.record_status = 'active' THEN 1 ELSE 0 END) AS l1,
     COUNT(l.record_status) AS cl1,
     SUM(CASE WHEN l.record_status = 'active' THEN 1 ELSE 0 END)/COUNT(l.record_status) AS percentage
FROM %s%u m
LEFT JOIN level1 l ON m.l1_id = l.id 
WHERE m.continent = '%s' AND m.enum_field = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC;]]
}

insert_update_delete_queries = {
 ["update_multi_right_join_pk_query"] = [[UPDATE %s 
JOIN (
SELECT 
%s.id AS match_id,
SUM(level1.record_value) AS sum_l1
     FROM level5     
     RIGHT JOIN level4 ON level5.l4_id = level4.id AND level4.record_status = '%s'     
     RIGHT JOIN level3 ON level4.l3_id = level3.id AND level3.record_status = '%s'
     RIGHT JOIN level2 ON level3.l2_id = level2.id AND level2.record_status = '%s'
     RIGHT JOIN level1 ON level2.l1_id = level1.id AND level1.record_status = '%s'
     RIGHT JOIN %s ON %s.l1_id = level1.parent_id
     WHERE %s.continent = '%s' AND level5.record_priority > 9
 GROUP BY %s.id ) AS calculated_data 
 ON %s.id = calculated_data.match_id 
 SET %s.myvalue = calculated_data.sum_l1;]],
  ["update_multi_left_join_pk_query"] = [[UPDATE %s 
JOIN (
SELECT 
%s.id AS match_id,
SUM(level1.record_value) AS sum_l1
     FROM %s     
     LEFT JOIN level1 ON %s.l1_id = level1.id
     LEFT JOIN level2 ON level1.l2_id = level2.id AND level1.record_status = '%s'
     LEFT JOIN level3 ON level2.l3_id = level3.id AND level2.record_status = '%s'
     LEFT JOIN level4 ON level3.l4_id = level4.id AND level3.record_status = '%s'
     LEFT JOIN level5 ON level4.l5_id = level5.id AND level4.record_status = '%s'     
 WHERE %s.continent = '%s' AND level5.record_priority > 9
 GROUP BY %s.id ) AS calculated_data 
 ON %s.id = calculated_data.match_id 
 SET %s.myvalue = calculated_data.sum_l1;]],
 
 ["update_multi_inner_join_pk_query"] = [[UPDATE %s 
JOIN (
SELECT 
%s.id AS match_id,
SUM(level1.record_value) AS sum_l1
     FROM %s     
     INNER JOIN level1 ON %s.l1_id = level1.id
     INNER JOIN level2 ON level1.l2_id = level2.id AND level1.record_status = '%s'
     INNER JOIN level3 ON level2.l3_id = level3.id AND level2.record_status = '%s'
     INNER JOIN level4 ON level3.l4_id = level4.id AND level3.record_status = '%s'
     INNER JOIN level5 ON level4.l5_id = level5.id AND level4.record_status = '%s'     
 WHERE %s.continent = '%s' AND level5.record_priority > 9
 GROUP BY %s.id ) AS calculated_data 
 ON %s.id = calculated_data.match_id 
 SET %s.myvalue = calculated_data.sum_l1;]]
}

-- END all join queries section
-- *****************************************************************

-- *****************************************************************
-- Function section and mapping
-- *****************************************************************

-- Function to merge multiple tables into one
function mergeMultiple(...)
    local result = {}
    local tables = {...}
    
    for _, tbl in ipairs(tables) do
        for key, value in pairs(tbl) do
            result[key] = value
        end
    end
    
    return result
end


-- Merge all query maps
local count = 0
for key in pairs(left_queries) do
    count = count + 1
end
-- print("Debug: Number of items in map:", count)

query_map = mergeMultiple(inner_queries, left_queries, right_queries, subquery_queries, semi_anti_condition_queries, insert_update_delete_queries)

-- Extract the names and remove "_query" suffix while adding to the all_joins list
all_joins = {}
count = 0
for key in pairs(query_map) do
    -- Remove "_query" suffix if it exists
    local base_name = key:gsub("_query$", "")
    table.insert(all_joins, base_name)
    count = count + 1
end
-- print("Debug: Number of items ported in array:", count)

-- *****************************************************************