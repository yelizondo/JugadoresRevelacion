DROP TABLE IF EXISTS tmp_EuropeanRosters;
create table tmp_EuropeanRosters(
    FullName string, 
    PlayerName string, 
    Affiliation string, 
    League string, 
    BirthDate string,
    Age string, 
    Height string, 
    Position string, 
    Position2 string, 
    Nationality string, 
    GamesPlayed string, 
    MarketValue string, 
    LastUpdated string,
    LastUpdated2 string,
    Accumulated string, 
    HighestMartketValue string, 
    HighestMarketValueDate string, 
    NationalTeam string, 
    MostRecentInjury string
) row format delimited fields terminated by ',';

load data inpath '/data/input/European_Rosters_Mini.csv' into table tmp_EuropeanRosters;


DROP TABLE IF EXISTS tmp_EuropeanRosters1; 
CREATE TABLE IF NOT EXISTS tmp_EuropeanRosters1 (
    FullName string, 
    PlayerName string, 
    Affiliation string, 
    League string, 
    BirthDate timestamp,
    Height string, 
    Position string, 
    Position2 string, 
    Nationality string, 
    GamesPlayed string, 
    MarketValue string, 
    LastUpdated timestamp, 
    Accumulated string, 
    HighestMartketValue string, 
    HighestMarketValueDate timestamp, 
    MostRecentInjury string
)
COMMENT 'Con fechas de forma correcta'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert into table tmp_EuropeanRosters1 
    select 
        Fullname, 
        PlayerName, 
        Affiliation, 
        League, 
        CASE 
            WHEN BirthDate rlike '[0-9]{1}/[0-9]{2}/[0-9]{4}' 
            THEN from_unixtime(unix_timestamp(BirthDate, 'MM/d/yyyy'))
            WHEN BirthDate rlike '[0-9]{2}/[0-9]{2}/[0-9]{4}' 
            THEN from_unixtime(unix_timestamp(BirthDate,'MM/dd/yyyy'))
            WHEN BirthDate rlike '[0-9]{2}/[0-9]{1}/[0-9]{4}' 
            THEN from_unixtime(unix_timestamp(BirthDate,'M/d/yyyy'))
            WHEN BirthDate rlike '[0-9]{1}/[0-9]{1}/[0-9]{4}' 
            THEN from_unixtime(unix_timestamp(BirthDate,'M/d/yyyy'))
            WHEN BirthDate rlike '[0-9]{1}/[0-9]{2}/[0-9]{2}' 
            THEN from_unixtime(unix_timestamp(BirthDate,'MM/d/yy'))
            WHEN BirthDate rlike '[0-9]{2}/[0-9]{2}/[0-9]{2}' 
            THEN from_unixtime(unix_timestamp(BirthDate,'MM/dd/yy'))
            WHEN BirthDate rlike '[0-9]{2}/[0-9]{1}/[0-9]{2}' 
            THEN from_unixtime(unix_timestamp(BirthDate,'M/dd/yy'))
            WHEN BirthDate rlike '[0-9]{1}/[0-9]{1}/[0-9]{2}' 
            THEN from_unixtime(unix_timestamp(BirthDate,'M/d/yy'))
            ELSE 78
            END,
        Height,
        Position,
        Position2,
        Nationality,
        GamesPlayed,
        MarketValue,
        from_unixtime(unix_timestamp(concat(substring(LastUpdated,15),substring(LastUpdated2,0,5)), 'MMM dd yyyy')),
        Accumulated,
        HighestMartketValue,
        CASE 
            WHEN HighestMarketValueDate rlike '[0-9]{1}/[0-9]{2}/[0-9]{4}' 
            THEN from_unixtime(unix_timestamp(HighestMarketValueDate, 'MM/d/yyyy'))
            WHEN HighestMarketValueDate rlike '[0-9]{2}/[0-9]{2}/[0-9]{4}' 
            THEN from_unixtime(unix_timestamp(HighestMarketValueDate,'MM/dd/yyyy'))
            WHEN HighestMarketValueDate rlike '[0-9]{2}/[0-9]{1}/[0-9]{4}' 
            THEN from_unixtime(unix_timestamp(HighestMarketValueDate,'M/d/yyyy'))
            WHEN HighestMarketValueDate rlike '[0-9]{1}/[0-9]{1}/[0-9]{4}' 
            THEN from_unixtime(unix_timestamp(HighestMarketValueDate,'M/d/yyyy'))
            WHEN HighestMarketValueDate rlike '[0-9]{1}/[0-9]{2}/[0-9]{2}' 
            THEN from_unixtime(unix_timestamp(HighestMarketValueDate,'MM/d/yy'))
            WHEN HighestMartketValue rlike '[0-9]{2}/[0-9]{2}/[0-9]{2}' 
            THEN from_unixtime(unix_timestamp(HighestMarketValueDate,'MM/dd/yy'))
            WHEN HighestMarketValueDate rlike '[0-9]{2}/[0-9]{1}/[0-9]{2}' 
            THEN from_unixtime(unix_timestamp(HighestMarketValueDate,'M/dd/yy'))
            WHEN HighestMarketValueDate rlike '[0-9]{1}/[0-9]{1}/[0-9]{2}' 
            THEN from_unixtime(unix_timestamp(HighestMarketValueDate,'M/d/yy'))
            ELSE 78
            END,
        MostRecentInjury
    from tmp_EuropeanRosters;

select PlayerName, BirthDate, LastUpdated, HighestMarketValueDate from tmp_EuropeanRosters1;

select substring(LastUpdated,15),substring(LastUpdated2,0,5) from tmp_EuropeanRosters;







DROP TABLE IF EXISTS tmp_country;
CREATE TABLE IF NOT EXISTS tmp_country (
    id int,
    name string
) row format delimited fields terminated by ',';

load data inpath '/data/input/soccer/Country.csv' into table tmp_country;