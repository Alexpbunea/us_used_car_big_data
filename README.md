# us_used_car_big_data
This project explores the US Used Cars Dataset from Kaggle, which contains approximately 3 million records with detailed information about used cars listed for sale up to the year 2020. The original dataset is provided in CSV format and includes 66 columns of attributes such as make, model, year, price, mileage, and technical specifications.

After performing a thorough preprocessing phase—removing incomplete, incorrect, or irrelevant data entries—the dataset was cleaned and transformed into a more analysis-friendly format. For the analysis, three big data technologies were selected: Spark SQL, Spark Core, and Hive. These tools were used to implement two different analytical workflows aimed at uncovering insights from the dataset related to vehicle performance, market trends, and price distribution.

## First problem
A job that generates statistics for each car brand (make_name) in the dataset, indicating, for each brand:
(a) the brand name and (b) a list of models (model_name) for that brand including, for each model: (i)
the number of cars in the dataset, (ii) the minimum, maximum, and average price (price) of that model
in the dataset, and (iii) the list of years in which the model appears in the dataset.

#### Results with Spark SQL on local

```text
-RECORD 0-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | cl
 count           | 29
 min_price       | 650.0
 max_price       | 6000.0
 avg_price       | 3480.689655172414
 total_years     | 6
 years_available | [1997, 1998, 1999, 2001, 2002, 2003]
-RECORD 1-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | ilx
 count           | 2319
 min_price       | 6995.0
 max_price       | 39305.0
 avg_price       | 24713.41181543769
 total_years     | 8
 years_available | [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
-RECORD 2-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | ilx hybrid
 count           | 18
 min_price       | 8398.0
 max_price       | 17999.0
 avg_price       | 12410.888888888889
 total_years     | 2
 years_available | [2013, 2014]
-RECORD 3-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | integra
 count           | 8
 min_price       | 2695.0
 max_price       | 47900.0
 avg_price       | 9659.25
 total_years     | 4
 years_available | [1993, 1997, 2000, 2001]
-RECORD 4-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | legend
 count           | 4
 min_price       | 4995.0
 max_price       | 15999.0
 avg_price       | 8972.25
 total_years     | 4
 years_available | [1989, 1990, 1993, 1994]
-RECORD 5-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | mdx
 count           | 8936
 min_price       | 1900.0
 max_price       | 63745.0
 avg_price       | 39562.56149174512
 total_years     | 20
 years_available | [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
-RECORD 6-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | mdx hybrid sport
 count           | 170
 min_price       | 28941.0
 max_price       | 61175.0
 avg_price       | 54421.94117647059
 total_years     | 4
 years_available | [2017, 2018, 2019, 2020]
-RECORD 7-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | nsx
 count           | 43
 min_price       | 39950.0
 max_price       | 198795.0
 avg_price       | 119584.0
 total_years     | 12
 years_available | [1991, 1992, 1993, 1995, 1997, 2003, 2004, 2005, 2017, 2018, 2019, 2020]
-RECORD 8-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | rdx
 count           | 6750
 min_price       | 4499.0
 max_price       | 49525.0
 avg_price       | 35025.33466666667
 total_years     | 15
 years_available | [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
-RECORD 9-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | rl
 count           | 134
 min_price       | 250.0
 max_price       | 15454.0
 avg_price       | 6810.70895522388
 total_years     | 14
 years_available | [1997, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011]
only showing top 10 rows

=== Time comparative (dataset ~1.3 GB vs subsets) ===
Dataset                    Rows    Valid groups     Time [s]
------------------------------------------------------------
100% (1 GB completo)    1811032            1450         6.05
50% (~0.5 GB)            906917            1450         2.29
25% (~0.3 GB)            453221            1450         3.37
```


#### Results with Spark SQL on yarn

```text
-RECORD 0-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | cl
 count           | 29
 min_price       | 650.0
 max_price       | 6000.0
 avg_price       | 3480.689655172414
 total_years     | 6
 years_available | [1997, 1998, 1999, 2001, 2002, 2003]
-RECORD 1-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | ilx
 count           | 2319
 min_price       | 6995.0
 max_price       | 39305.0
 avg_price       | 24713.41181543769
 total_years     | 8
 years_available | [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
-RECORD 2-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | ilx hybrid
 count           | 18
 min_price       | 8398.0
 max_price       | 17999.0
 avg_price       | 12410.888888888889
 total_years     | 2
 years_available | [2013, 2014]
-RECORD 3-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | integra
 count           | 8
 min_price       | 2695.0
 max_price       | 47900.0
 avg_price       | 9659.25
 total_years     | 4
 years_available | [1993, 1997, 2000, 2001]
-RECORD 4-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | legend
 count           | 4
 min_price       | 4995.0
 max_price       | 15999.0
 avg_price       | 8972.25
 total_years     | 4
 years_available | [1989, 1990, 1993, 1994]
-RECORD 5-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | mdx
 count           | 8936
 min_price       | 1900.0
 max_price       | 63745.0
 avg_price       | 39562.56149174512
 total_years     | 20
 years_available | [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
-RECORD 6-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | mdx hybrid sport
 count           | 170
 min_price       | 28941.0
 max_price       | 61175.0
 avg_price       | 54421.94117647059
 total_years     | 4
 years_available | [2017, 2018, 2019, 2020]
-RECORD 7-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | nsx
 count           | 43
 min_price       | 39950.0
 max_price       | 198795.0
 avg_price       | 119584.0
 total_years     | 12
 years_available | [1991, 1992, 1993, 1995, 1997, 2003, 2004, 2005, 2017, 2018, 2019, 2020]
-RECORD 8-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | rdx
 count           | 6750
 min_price       | 4499.0
 max_price       | 49525.0
 avg_price       | 35025.33466666667
 total_years     | 15
 years_available | [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
-RECORD 9-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | rl
 count           | 134
 min_price       | 250.0
 max_price       | 15454.0
 avg_price       | 6810.70895522388
 total_years     | 14
 years_available | [1997, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011]
only showing top 10 rows

=== Time comparative (dataset 1.3 GB vs subsets) ===
Dataset                    Rows    Valid groups     Time [s]
------------------------------------------------------------
100% (1 GB completo)    1811032            1450         7.53
50% (~0.5 GB)            906917            1450         4.09
25% (~0.3 GB)            453221            1450         2.31
```


#### Results with Spark SQL on cluster

```text
-RECORD 0-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | cl
 count           | 29
 min_price       | 650.0
 max_price       | 6000.0
 avg_price       | 3480.689655172414
 total_years     | 6
 years_available | [1997, 1998, 1999, 2001, 2002, 2003]
-RECORD 1-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | ilx
 count           | 2319
 min_price       | 6995.0
 max_price       | 39305.0
 avg_price       | 24713.41181543769
 total_years     | 8
 years_available | [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
-RECORD 2-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | ilx hybrid
 count           | 18
 min_price       | 8398.0
 max_price       | 17999.0
 avg_price       | 12410.888888888889
 total_years     | 2
 years_available | [2013, 2014]
-RECORD 3-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | integra
 count           | 8
 min_price       | 2695.0
 max_price       | 47900.0
 avg_price       | 9659.25
 total_years     | 4
 years_available | [1993, 1997, 2000, 2001]
-RECORD 4-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | legend
 count           | 4
 min_price       | 4995.0
 max_price       | 15999.0
 avg_price       | 8972.25
 total_years     | 4
 years_available | [1989, 1990, 1993, 1994]
-RECORD 5-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | mdx
 count           | 8936
 min_price       | 1900.0
 max_price       | 63745.0
 avg_price       | 39562.56149174512
 total_years     | 20
 years_available | [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
-RECORD 6-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | mdx hybrid sport
 count           | 170
 min_price       | 28941.0
 max_price       | 61175.0
 avg_price       | 54421.94117647059
 total_years     | 4
 years_available | [2017, 2018, 2019, 2020]
-RECORD 7-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | nsx
 count           | 43
 min_price       | 39950.0
 max_price       | 198795.0
 avg_price       | 119584.0
 total_years     | 12
 years_available | [1991, 1992, 1993, 1995, 1997, 2003, 2004, 2005, 2017, 2018, 2019, 2020]
-RECORD 8-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | rdx
 count           | 6750
 min_price       | 4499.0
 max_price       | 49525.0
 avg_price       | 35025.33466666667
 total_years     | 15
 years_available | [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
-RECORD 9-----------------------------------------------------------------------------------------------------------------------------------
 make_name       | acura
 model_name      | rl
 count           | 134
 min_price       | 250.0
 max_price       | 15454.0
 avg_price       | 6810.70895522388
 total_years     | 14
 years_available | [1997, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011]
only showing top 10 rows

=== Time comparative (dataset 1.3 GB vs subsets) ===
Dataset                    Rows    Valid groups     Time [s]
------------------------------------------------------------
100% (1 GB completo)    1811032            1450         5.45
50% (~0.5 GB)            906917            1450         1.84
25% (~0.3 GB)            453221            1450         1.68
```


----
## Second problem
A job that groups car models with “similar” engine characteristics—i.e., models whose horsepower and
engine displacement values differ by at most 10%.For each group, report the average price and the
model with the highest horsepower.

#### Results with Spark core on local
```text
Total time: 1.56 seconds
{'bin_hp': 54, 'bin_disp': 76, 'avg_price': 23024.760787141615, 'max_horsepower': 181.0, 'top_model': 'elr'}
{'bin_hp': 62, 'bin_disp': 84, 'avg_price': 56634.58354239471, 'max_horsepower': 405.0, 'top_model': 'm2'}
{'bin_hp': 57, 'bin_disp': 85, 'avg_price': 11665.669401091407, 'max_horsepower': 250.0, 'top_model': '300'}
{'bin_hp': 61, 'bin_disp': 89, 'avg_price': 39958.711003450924, 'max_horsepower': 367.0, 'top_model': 'sierra 1500'}
{'bin_hp': 52, 'bin_disp': 82, 'avg_price': 10441.37032842582, 'max_horsepower': 155.0, 'top_model': 'grand vitara'}
{'bin_hp': 59, 'bin_disp': 79, 'avg_price': 34465.77518322822, 'max_horsepower': 303.0, 'top_model': 'lancer evolution'}
{'bin_hp': 49, 'bin_disp': 77, 'avg_price': 9811.630924267101, 'max_horsepower': 116.0, 'top_model': 'mx5 miata'}
{'bin_hp': 66, 'bin_disp': 88, 'avg_price': 78470.21457489878, 'max_horsepower': 570.0, 'top_model': '458 italia'}
{'bin_hp': 53, 'bin_disp': 81, 'avg_price': 10803.35116034679, 'max_horsepower': 171.0, 'top_model': 'cobalt'}
{'bin_hp': 63, 'bin_disp': 83, 'avg_price': 92495.34368737476, 'max_horsepower': 444.0, 'top_model': 'rs 5'}
```

#### Results with Spark core on yarn

```text
Total time: 1.98 seconds
{'bin_hp': 54, 'bin_disp': 76, 'avg_price': 23024.760787141615, 'max_horsepower': 181.0, 'top_model': 'elr'}
{'bin_hp': 62, 'bin_disp': 84, 'avg_price': 56634.58354239471, 'max_horsepower': 405.0, 'top_model': 'm2'}
{'bin_hp': 57, 'bin_disp': 85, 'avg_price': 11665.669401091407, 'max_horsepower': 250.0, 'top_model': '300'}
{'bin_hp': 61, 'bin_disp': 89, 'avg_price': 39958.711003450924, 'max_horsepower': 367.0, 'top_model': 'sierra 1500'}
{'bin_hp': 52, 'bin_disp': 82, 'avg_price': 10441.37032842582, 'max_horsepower': 155.0, 'top_model': 'grand vitara'}
{'bin_hp': 59, 'bin_disp': 79, 'avg_price': 34465.77518322822, 'max_horsepower': 303.0, 'top_model': 'lancer evolution'}
{'bin_hp': 49, 'bin_disp': 77, 'avg_price': 9811.630924267101, 'max_horsepower': 116.0, 'top_model': 'mx5 miata'}
{'bin_hp': 66, 'bin_disp': 88, 'avg_price': 78470.21457489878, 'max_horsepower': 570.0, 'top_model': '458 italia'}
{'bin_hp': 53, 'bin_disp': 81, 'avg_price': 10803.35116034679, 'max_horsepower': 171.0, 'top_model': 'cobalt'}
{'bin_hp': 63, 'bin_disp': 83, 'avg_price': 92495.34368737476, 'max_horsepower': 444.0, 'top_model': 'rs 5'}
```

#### Results with Spark core on cluster

```text
Total time: 1.99 seconds
{'bin_hp': 54, 'bin_disp': 76, 'avg_price': 23024.760787141615, 'max_horsepower': 181.0, 'top_model': 'elr'}
{'bin_hp': 62, 'bin_disp': 84, 'avg_price': 56634.58354239471, 'max_horsepower': 405.0, 'top_model': 'm2'}
{'bin_hp': 57, 'bin_disp': 85, 'avg_price': 11665.669401091407, 'max_horsepower': 250.0, 'top_model': '300'}
{'bin_hp': 61, 'bin_disp': 89, 'avg_price': 39958.711003450924, 'max_horsepower': 367.0, 'top_model': 'sierra 1500'}
{'bin_hp': 52, 'bin_disp': 82, 'avg_price': 10441.37032842582, 'max_horsepower': 155.0, 'top_model': 'grand vitara'}
{'bin_hp': 59, 'bin_disp': 79, 'avg_price': 34465.77518322822, 'max_horsepower': 303.0, 'top_model': 'lancer evolution'}
{'bin_hp': 49, 'bin_disp': 77, 'avg_price': 9811.630924267101, 'max_horsepower': 116.0, 'top_model': 'mx5 miata'}
{'bin_hp': 66, 'bin_disp': 88, 'avg_price': 78470.21457489878, 'max_horsepower': 570.0, 'top_model': '458 italia'}
{'bin_hp': 53, 'bin_disp': 81, 'avg_price': 10803.35116034679, 'max_horsepower': 171.0, 'top_model': 'cobalt'}
{'bin_hp': 63, 'bin_disp': 83, 'avg_price': 92495.34368737476, 'max_horsepower': 444.0, 'top_model': 'rs 5'}
```

#### Results wih Hive

```text
42      72      1000.0                  55.0    metro
43      75      2747.5                  63.0    aspire
43      77      7500.0                  65.0    chevette
44      72      6179.307339449541       70.0    fortwo
44      77      6900.0                  69.0    sentra
44      78      15995.0                 72.0    accord
44      81      8828.5                  67.0    240
45      72      3048.3333333333335      73.0    insight
45      74      14424.272238163558      78.0    mirage
45      75      2983.0                  79.0    metro
Time taken: 27.32 seconds, Fetched: 313 row(s)
```

***The diference of the results between Spark and Hive is because, on Spark, the result is not sorted**

