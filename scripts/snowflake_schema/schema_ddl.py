DROP_TABLES = """
DROP TABLE IF EXISTS fact_episode;
DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_show;
"""



DIM_COUNTRY  = """ 
CREATE TABLE dim_country (
    country_id INTEGER AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE
);
"""


DIM_SHOW = """
CREATE TABLE dim_show (
    show_id INTEGER AUTO_INCREMENT PRIMARY KEY,
    tvmaze_id INTEGER UNIQUE,
    name VARCHAR(255)
);
""" 