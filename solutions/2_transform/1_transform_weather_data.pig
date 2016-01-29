/** *************************************************************************************************************************
 ** transform_weather_data.pig : Transform the weather data into one record per fact
 ** ********************************************************************************************************************** */

/*
    Load the facts data so we can start rocking.
*/
wide_facts =
    LOAD '/data/raw/weather/facts'
    USING PigStorage(',')
    AS (
        station_id:chararray,
        year:int,
        month:int,
        element:chararray,
        day_1:int, day_2:int, day_3:int, day_4:int, day_5:int, day_6:int, day_7:int, day_8:int, day_9:int, day_10:int,
        day_11:int, day_12:int, day_13:int, day_14:int, day_15:int, day_16:int, day_17:int, day_18:int, day_19:int, day_20:int,
        day_21:int, day_22:int, day_23:int, day_24:int, day_25:int, day_26:int, day_27:int, day_28:int, day_29:int, day_30:int,
        day_31:int
    );

/* 
    In order to be able to process the data in a sensible way we will need to transform the wide 
    facts table into a long facts table with a record per day. Translations like this are quite 
    tedious in pig but they are possible.
*/
long_facts =
    FOREACH wide_facts {
        data = TOBAG(
            TOTUPLE(station_id, year, month, 1, element, day_1),
            TOTUPLE(station_id, year, month, 2, element, day_2),
            TOTUPLE(station_id, year, month, 3, element, day_3),
            TOTUPLE(station_id, year, month, 4, element, day_4),
            TOTUPLE(station_id, year, month, 5, element, day_5),
            TOTUPLE(station_id, year, month, 6, element, day_6),
            TOTUPLE(station_id, year, month, 7, element, day_7),
            TOTUPLE(station_id, year, month, 8, element, day_8),
            TOTUPLE(station_id, year, month, 9, element, day_9),
            TOTUPLE(station_id, year, month, 10, element, day_10),
            TOTUPLE(station_id, year, month, 11, element, day_11),
            TOTUPLE(station_id, year, month, 12, element, day_12),
            TOTUPLE(station_id, year, month, 13, element, day_13),
            TOTUPLE(station_id, year, month, 14, element, day_14),
            TOTUPLE(station_id, year, month, 15, element, day_15),
            TOTUPLE(station_id, year, month, 16, element, day_16),
            TOTUPLE(station_id, year, month, 17, element, day_17),
            TOTUPLE(station_id, year, month, 18, element, day_18),
            TOTUPLE(station_id, year, month, 19, element, day_19),
            TOTUPLE(station_id, year, month, 20, element, day_20),
            TOTUPLE(station_id, year, month, 21, element, day_21),
            TOTUPLE(station_id, year, month, 22, element, day_22),
            TOTUPLE(station_id, year, month, 23, element, day_23),
            TOTUPLE(station_id, year, month, 24, element, day_24),
            TOTUPLE(station_id, year, month, 25, element, day_25),
            TOTUPLE(station_id, year, month, 26, element, day_26),
            TOTUPLE(station_id, year, month, 27, element, day_27),
            TOTUPLE(station_id, year, month, 28, element, day_28),
            TOTUPLE(station_id, year, month, 29, element, day_29),
            TOTUPLE(station_id, year, month, 30, element, day_30),
            TOTUPLE(station_id, year, month, 31, element, day_31)
        );
        
        GENERATE FLATTEN(data) AS ( station_id, year, month, day:int, element:chararray, value:int );
    }
    
/*
    Filter the facts.
    
    We will ignore invalid elements and values.
    Since we are only interested in the temperature and prcp data, we will drop all other data.
*/
facts = 
    FILTER long_facts
            BY (element IS NOT NULL) 
            AND (value IS NOT NULL) 
            AND (value != -9999 )
            AND (
                (element == 'TMAX') OR
                (element == 'TAVG') OR
                (element == 'TMIN') OR
                (element == 'PRCP')
            );
        
/*
    Ok, let that sit there for a while. Next we will load the stations.
*/
stations =
    LOAD '/data/raw/weather/stations'
    USING PigStorage(',')
    AS (
        station_id: chararray,
        latitude:  float,
        longitude: float,
        elevation: float,
        state: chararray,
        name: chararray
    );
    
/*
    Now both the stations as well as the facts are available, it is time to join them together.
*/
linked_facts_stations =
    JOIN facts BY station_id,
         stations BY station_id;

/*
    There is a lot of data in that relation that we will not need. So only get the
    
        year, month, date, state, element and value
        
    out of that relation.
*/
facts =
    FOREACH linked_facts_stations
    GENERATE
        year,
        month,
        day,
        state,
        element,
        value;
        
/*
    But our facts might have duplicates since there might be more 
    than one station per state. In order to fix that, we will need to group our data and calculate averages
    for states with multiple stations on the same date.
    
    NOTE: Averages are indeed not that interesting because of the high error margin in this case.
*/
grouped_facts =
    GROUP facts
    BY (year, month, day, state, element);
    
unique_facts =
    FOREACH grouped_facts
    GENERATE
        group.year,
        group.month,
        group.day,
        group.state,
        group.element,
        AVG(facts.value) AS value;
        
/*
    Well done skipper! Now we will need to write the data to HDFS so we can use it in our further processing
*/
STORE unique_facts
    INTO '/data/master/weather'
    USING PigStorage();
