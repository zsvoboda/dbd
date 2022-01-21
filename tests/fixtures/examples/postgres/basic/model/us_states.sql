SELECT
        state.abbrev AS state_code,
        state.state AS state_name,
        CAST(population.population AS INTEGER) AS state_population,
        CAST(area.area_sq_mi AS INTEGER) AS state_area_sq_mi
    FROM state
        JOIN population ON population.state = state.abbrev
        JOIN area on area.state_name = state.state
