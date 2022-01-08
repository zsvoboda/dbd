SELECT
        us_covid.date AS date,
        us_states.state_code,
        us_states.state_name,
        us_states.state_population,
        us_states.state_area_sq_mi,
        us_covid.cases AS state_covid_cases,
        us_covid.deaths AS state_covid_deaths
    FROM us_covid
        JOIN us_states ON us_states.state_name = us_covid.state
        