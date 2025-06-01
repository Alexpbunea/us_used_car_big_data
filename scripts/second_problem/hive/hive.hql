DROP TABLE IF EXISTS autos_parquet;

CREATE EXTERNAL TABLE autos_parquet (
  vin STRING,
  daysonmarket STRING,
  price STRING,
  year STRING,
  engine_type STRING,
  engine_displacement FLOAT,
  city STRING,
  make_name STRING,
  model_name STRING,
  engine_cylinders INT,
  power FLOAT,
  torque FLOAT,
  description STRING
)
STORED AS PARQUET
LOCATION "/user/spark/output2/used_cars_data_cleaned/";

WITH filtered_data AS (
  SELECT 
    model_name,
    CAST(price AS INT),
    power AS horsepower,
    engine_displacement AS displacement
  FROM autos_parquet
  WHERE power IS NOT NULL
    AND power > 0 
    AND engine_displacement > 0 
    AND price IS NOT NULL
),

binned_cars AS (
  SELECT 
    model_name,
    price,
    horsepower,
    displacement,
    FLOOR(LOG(displacement) / LOG(1.1)) AS bin_disp,
    FLOOR(LOG(horsepower) / LOG(1.1)) AS bin_hp
  FROM filtered_data
),

group_stats AS (
  SELECT 
    bin_hp,
    bin_disp,
    AVG(price) AS avg_price,
    MAX(horsepower) AS max_horsepower
  FROM binned_cars
  GROUP BY bin_hp, bin_disp
),

top_models AS (
  SELECT 
    bc.bin_hp,
    bc.bin_disp,
    bc.model_name,
    bc.horsepower,
    ROW_NUMBER() OVER (
      PARTITION BY bc.bin_hp, bc.bin_disp 
      ORDER BY bc.horsepower DESC, bc.model_name
    ) AS rank
  FROM binned_cars bc
  INNER JOIN group_stats gs 
    ON bc.bin_hp = gs.bin_hp 
    AND bc.bin_disp = gs.bin_disp
    AND bc.horsepower = gs.max_horsepower
)

SELECT 
  gs.bin_hp,
  gs.bin_disp,
  gs.avg_price,
  gs.max_horsepower,
  tm.model_name AS top_model
FROM group_stats gs
INNER JOIN top_models tm 
  ON gs.bin_hp = tm.bin_hp 
  AND gs.bin_disp = tm.bin_disp
WHERE tm.rank = 1
ORDER BY gs.bin_hp, gs.bin_disp;