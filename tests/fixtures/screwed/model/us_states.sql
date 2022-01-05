SELEKT
        state.abbrev AS state_code,
        state.state AS state_name,
        population.population AS state_population,
        area.area_sq_mi  AS state_area_sq_mi
    OD state
        JOIN population ON population.state = state.abbrev
        JOIN area on area.state_name = state.state
