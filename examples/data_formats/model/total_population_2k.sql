SELECT DISTINCT
       p.state_code,
       SUM(p.population) FILTER(WHERE p.year=2000 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2000,
       SUM(p.population) FILTER(WHERE p.year=2001 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2001,
       SUM(p.population) FILTER(WHERE p.year=2002 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2002,
       SUM(p.population) FILTER(WHERE p.year=2003 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2003,
       SUM(p.population) FILTER(WHERE p.year=2004 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2004,
       SUM(p.population) FILTER(WHERE p.year=2005 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2005,
       SUM(p.population) FILTER(WHERE p.year=2006 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2006,
       SUM(p.population) FILTER(WHERE p.year=2007 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2007,
       SUM(p.population) FILTER(WHERE p.year=2008 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2008,
       SUM(p.population) FILTER(WHERE p.year=2009 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2009,
       SUM(p.population) FILTER(WHERE p.year=2010 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2010,
       SUM(p.population) FILTER(WHERE p.year=2011 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2011,
       SUM(p.population) FILTER(WHERE p.year=2012 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2012,
       SUM(p.population) FILTER(WHERE p.year=2013 AND p.ages = 'total') OVER(PARTITION BY p.state_code) as total_2013
    FROM population p