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
    
    ["simple_inner_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(level1,main1) */ m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM %s%u as m
INNER JOIN level1 ON m.id = level1.parent_id and level1.record_status = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multilevel_inner_forcing_order_index_query"] = [[SELECT /*+ JOIN_ORDER(level2,level3,level4,level5,main1,level1) */ m.continent, count(m.continent) cc,
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
    
    ["simple_left_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(level1,main1) */ m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM %s%u as m
LEFT JOIN level1 ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_left_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(level2,main1,level1,level3,level4,level5) */ m.continent, count(m.continent) cc,year_field,
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
LEFT JOIN main1 ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE m.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_left_straight_query"] = [[SELECT STRAIGHT_JOIN m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs,
SUM(level1.record_value) l1,SUM(level2.record_value) l2,SUM(level3.record_value) l3,SUM(level3.record_value) l3,
SUM(level4.record_value) l4,SUM(level5.record_value) l5
FROM level2
LEFT JOIN main1 ON m.id = level2.parent_id and m.enum_field = '%s'
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
    
    ["simple_right_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(main1,level1) */ m.continent, count(m.continent) cc,year_field,count(year_field) cy, m.enum_field, count(m.enum_field) cs, SUM(level1.record_value) l1
FROM level1
RIGHT JOIN %s%u as m ON m.id = level1.parent_id and m.enum_field = '%s'
WHERE level1.continent = '%s'
GROUP BY m.continent, m.year_field, m.enum_field
ORDER BY m.year_field DESC, cc DESC, cs DESC
LIMIT 100;]],
    
    ["multi_right_forcing_order_GB_query"] = [[SELECT /*+ JOIN_ORDER(main1,level1,level2,level3,level4,level5) */ m.continent, count(m.continent) cc,year_field,
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
print("Number of items in map:", count)

query_map = mergeMultiple(inner_queries, left_queries, right_queries)

-- Extract the names and remove "_query" suffix while adding to the all_joins list
all_joins = {}
count = 0
for key in pairs(query_map) do
    -- Remove "_query" suffix if it exists
    local base_name = key:gsub("_query$", "")
    table.insert(all_joins, base_name)
    count = count + 1
end
print("Number of items ported in array:", count)

-- *****************************************************************