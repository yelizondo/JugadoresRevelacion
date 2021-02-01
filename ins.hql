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
            WHEN BirthDate rlike '[0-9]{2}/[0-9]{1}/[0-9]{2}' 
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

-- SCHEMAS
create schema soccer;
use soccer;

-- TABLAS

DROP TABLE IF EXISTS tmp_country;
CREATE TABLE IF NOT EXISTS tmp_country (
    id int,
    name string
) row format delimited fields terminated by ',';

load data inpath '/data/input/soccer/Country.csv' into table tmp_country;

DROP TABLE IF EXISTS tmp_league;
CREATE TABLE IF NOT EXISTS tmp_league (
    id int,
    country_id int,
    name string
) row format delimited fields terminated by ',';
load data inpath '/data/input/soccer/League.csv' into table tmp_league;

DROP TABLE IF EXISTS tmp_player;
CREATE TABLE IF NOT EXISTS tmp_player (
    id int,
    player_api_id int,
    player_name string,
    player_fifa_api_id int,
    birthday timestamp,
    height double,
    weight double
) row format delimited fields terminated by ',';
load data inpath '/data/input/soccer/Player.csv' into table tmp_player;

DROP TABLE IF EXISTS tmp_match;
CREATE TABLE IF NOT EXISTS tmp_match (
    id int,
    country_id int,
    league_id int,
    season string,
    stage int,
    datef timestamp,
    match_api_id int,
    home_team_api_id int,
    away_team_api_id int,
    home_team_goal int,
    away_team_goal int,
    home_player_X1 int,
    home_player_X2 int,
    home_player_X3 int,
    home_player_X4 int,
    home_player_X5 int,
    home_player_X6 int,
    home_player_X7 int,
    home_player_X8 int,
    home_player_X9 int,
    home_player_X10 int,
    home_player_X11 int,
    away_player_X1 int,
    away_player_X2 int,
    away_player_X3 int,
    away_player_X4 int,
    away_player_X5 int,
    way_player_X6 int,
    away_player_X7 int,
    away_player_X8 int,
    away_player_X9 int,
    away_player_X10 int,
    away_player_X11 int,
    home_player_Y1 int,
    home_player_Y2 int,
    home_player_Y3 int,
    home_player_Y4 int,
    home_player_Y5 int,
    home_player_Y6 int,
    home_player_Y7 int,
    home_player_Y8 int,
    home_player_Y9 int,
    home_player_Y10 int,
    home_player_Y11 int,
    away_player_Y1 int,
    away_player_Y2 int,
    away_player_Y3 int,
    away_player_Y4 int,
    away_player_Y5 int,
    away_player_Y6 int,
    away_player_Y7 int,
    away_player_Y8 int,
    away_player_Y9 int,
    away_player_Y10 int,
    away_player_Y11 int,
    home_player_1 int,
    home_player_2 int,
    home_player_3 int,
    home_player_4 int,
    home_player_5 int,
    home_player_6 int,
    home_player_7 int,
    home_player_8 int,
    home_player_9 int,
    home_player_10 int,
    home_player_11 int,
    away_player_1 int,
    away_player_2 int,
    away_player_3 int,
    away_player_4 int,
    away_player_5 int,
    away_player_6 int,
    away_player_7 int,
    away_player_8 int,
    away_player_9 int,
    away_player_10 int,
    away_player_11 int,
    goal string,
    shoton string,
    shotoff string,
    foulcommit string,
    card string,
    crossf string,
    corner string,
    possession string,
    B365H double,
    B365D double,
    B365A double,
    BWH double,
    BWD double,
    BWA double,
    IWH double,
    IWD double,
    IWA double,
    LBH double,
    LBD double,
    LBA double,
    PSH double,
    PSD double,
    PSA double,
    WHH double,
    WHD double,
    WHA double,
    SJH double,
    SJD double,
    SJA double,
    VCH double,
    VCD double,
    VCA double,
    GBH double,
    GBD double,
    GBA double,
    BSH double,
    BSD double,
    BSA double
) row format delimited fields terminated by ',';
load data inpath '/data/input/soccer/Match.csv' into table tmp_match;

DROP TABLE IF EXISTS tmp_player_attributes;
CREATE TABLE IF NOT EXISTS tmp_player_attributes (
    id int,
    player_fifa_api_id int,
    player_api_id int,
    datef timestamp,
    overall_rating int,	potential int,
    preferred_foot string,
    attacking_work_rate string,
    defensive_work_rate string,
    crossing int,
    finishing int,
    heading_accuracy int,
    short_passing int,
    volleys int,
    dribbling int,
    curve int,
    free_kick_accuracy int,
    long_passing int,
    ball_control int,
    acceleration int,
    sprint_speed int,
    agility int,
    reactions int,
    balance int,
    shot_power int,
    jumping int,
    stamina int,
    strength int,
    long_shots int,
    aggression int,
    interceptions int,
    positioning int,
    vision int,
    penalties int,
    marking int,
    standing_tackle int,
    sliding_tackle int,
    gk_diving int,
    gk_handling int,
    gk_kicking int,
    gk_positioning int,
    gk_reflexes int
) row format delimited fields terminated by ',';
load data inpath '/data/input/soccer/Player_Attributes.csv' into table tmp_player_attributes;

DROP TABLE IF EXISTS tmp_team;
CREATE TABLE IF NOT EXISTS tmp_team (
    id int,
    team_api_id int,
    team_fifa_api_id int,
    team_long_name string,
    team_short_name string
) row format delimited fields terminated by ',';
load data inpath '/data/input/soccer/Team.csv' into table tmp_team;

DROP TABLE IF EXISTS tmp_team_attributes;
CREATE TABLE IF NOT EXISTS tmp_team_attributes (
    id int,
    team_fifa_api_id int,
    team_api_id int,
    datef timestamp,
    buildUpPlaySpeed int,
    buildUpPlaySpeedClass string,
    buildUpPlayDribbling int,
    buildUpPlayDribblingClass string,
    buildUpPlayPassing int,
    buildUpPlayPassingClass string,
    buildUpPlayPositioningClass string,
    chanceCreationPassing int,
    chanceCreationPassingClass string,
    chanceCreationCrossing int,
    chanceCreationCrossingClass string,
    chanceCreationShooting int,
    chanceCreationShootingClass string,
    chanceCreationPositioningClass string,
    defencePressure int,
    defencePressureClass string,
    defenceAggression int,
    defenceAggressionClass string,
    defenceTeamWidth int,
    defenceTeamWidthClass string,
    defenceDefenderLineClass string
) row format delimited fields terminated by ',';
load data inpath '/data/input/soccer/Team_Attributes.csv' into table tmp_team_attributes;

-- DATA PROCESSING
-- Reduce the amount of matches to process
-- by date and by league
DROP TABLE IF EXISTS limitedMatches;
CREATE TABLE IF NOT EXISTS limitedMatches (
    datef timestamp,
    home_team_name string,
    away_team_name string,
    home_player_1 int,
    home_player_2 int,
    home_player_3 int,
    home_player_4 int,
    home_player_5 int,
    home_player_6 int,
    home_player_7 int,
    home_player_8 int,
    home_player_9 int,
    home_player_10 int,
    home_player_11 int,
    away_player_1 int,
    away_player_2 int,
    away_player_3 int,
    away_player_4 int,
    away_player_5 int,
    away_player_6 int,
    away_player_7 int,
    away_player_8 int,
    away_player_9 int,
    away_player_10 int,
    away_player_11 int
)
COMMENT 'Bundesliga matches between 2014 and 2016'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE limitedMatches 
SELECT tmpm.datef,
    homeTeam.team_long_name,
    awayTeam.team_long_name,
    tmpm.home_player_1,
    tmpm.home_player_2,
    tmpm.home_player_3,
    tmpm.home_player_4,
    tmpm.home_player_5,
    tmpm.home_player_6,
    tmpm.home_player_7,
    tmpm.home_player_8,
    tmpm.home_player_9,
    tmpm.home_player_10,
    tmpm.home_player_11,
    tmpm.away_player_1,
    tmpm.away_player_2,
    tmpm.away_player_3,
    tmpm.away_player_4,
    tmpm.away_player_5,
    tmpm.away_player_6,
    tmpm.away_player_7,
    tmpm.away_player_8,
    tmpm.away_player_9,
    tmpm.away_player_10,
    tmpm.away_player_11
FROM tmp_match tmpm 
    JOIN tmp_league tmpl ON (tmpm.league_id = tmpl.id)
    JOIN tmp_team homeTeam ON (tmpm.home_team_api_id = homeTeam.team_api_id)
    JOIN tmp_team awayTeam ON (tmpm.away_team_api_id = awayTeam.team_api_id)
WHERE tmpl.name = "Germany 1. Bundesliga" 
    AND tmpm.datef BETWEEN '2014-01-01 00:00:00' AND '2016-21-31 23:59:59';

DROP TABLE IF EXISTS limitedPlayers;
CREATE TABLE IF NOT EXISTS limitedPlayers ( 
    id int,
    player_api_id int,
    player_name string,
    player_fifa_api_id int,
    birthday timestamp,
    height double,
    weight double
)
COMMENT 'All players that played a match between 2014-2016 in the Bundesliga'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE limitedPlayers 
SELECT DISTINCT P.* FROM tmp_player P
    JOIN limitedMatches M ON (
        P.player_api_id = M.away_player_1 
        OR P.player_api_id = M.away_player_2
        OR P.player_api_id = M.away_player_3
        OR P.player_api_id = M.away_player_4
        OR P.player_api_id = M.away_player_5
        OR P.player_api_id = M.away_player_6
        OR P.player_api_id = M.away_player_7
        OR P.player_api_id = M.away_player_8
        OR P.player_api_id = M.away_player_9
        OR P.player_api_id = M.away_player_10
        OR P.player_api_id = M.away_player_11
        OR P.player_api_id = M.home_player_1
        OR P.player_api_id = M.home_player_2
        OR P.player_api_id = M.home_player_3
        OR P.player_api_id = M.home_player_4
        OR P.player_api_id = M.home_player_5
        OR P.player_api_id = M.home_player_6
        OR P.player_api_id = M.home_player_7
        OR P.player_api_id = M.home_player_8
        OR P.player_api_id = M.home_player_9
        OR P.player_api_id = M.home_player_10
        OR P.player_api_id = M.home_player_11 
    )
WHERE P.player_api_id is not null;

DROP TABLE IF EXISTS playersWithTeamDates;
CREATE TABLE IF NOT EXISTS playersWithTeamDates ( 
    player_name string,
    datef timestamp,
    team_name string
)
COMMENT 'Players with all the dates of matches for every team'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE playersWithTeamDates
SELECT 
    DISTINCT P.player_name,
    M.datef, 
    CASE 
        WHEN
            P.player_api_id = M.home_player_1
            OR P.player_api_id = M.home_player_2
            OR P.player_api_id = M.home_player_3
            OR P.player_api_id = M.home_player_4
            OR P.player_api_id = M.home_player_5
            OR P.player_api_id = M.home_player_6
            OR P.player_api_id = M.home_player_7
            OR P.player_api_id = M.home_player_8
            OR P.player_api_id = M.home_player_9
            OR P.player_api_id = M.home_player_10
            OR P.player_api_id = M.home_player_11 
        THEN
            M.home_team_name
        WHEN 
            P.player_api_id = M.away_player_1 
            OR P.player_api_id = M.away_player_2
            OR P.player_api_id = M.away_player_3
            OR P.player_api_id = M.away_player_4
            OR P.player_api_id = M.away_player_5
            OR P.player_api_id = M.away_player_6
            OR P.player_api_id = M.away_player_7
            OR P.player_api_id = M.away_player_8
            OR P.player_api_id = M.away_player_9
            OR P.player_api_id = M.away_player_10
            OR P.player_api_id = M.away_player_11 
        THEN 
            M.away_team_name
        END
FROM tmp_player P
    JOIN limitedMatches M ON (
        P.player_api_id = M.away_player_1 
        OR P.player_api_id = M.away_player_2
        OR P.player_api_id = M.away_player_3
        OR P.player_api_id = M.away_player_4
        OR P.player_api_id = M.away_player_5
        OR P.player_api_id = M.away_player_6
        OR P.player_api_id = M.away_player_7
        OR P.player_api_id = M.away_player_8
        OR P.player_api_id = M.away_player_9
        OR P.player_api_id = M.away_player_10
        OR P.player_api_id = M.away_player_11
        OR P.player_api_id = M.home_player_1
        OR P.player_api_id = M.home_player_2
        OR P.player_api_id = M.home_player_3
        OR P.player_api_id = M.home_player_4
        OR P.player_api_id = M.home_player_5
        OR P.player_api_id = M.home_player_6
        OR P.player_api_id = M.home_player_7
        OR P.player_api_id = M.home_player_8
        OR P.player_api_id = M.home_player_9
        OR P.player_api_id = M.home_player_10
        OR P.player_api_id = M.home_player_11 
    )
WHERE P.player_api_id is not null;




DROP TABLE IF EXISTS playersWithTeam;
CREATE TABLE IF NOT EXISTS playersWithTeam ( 
    player_name string,
    team_name string
)
COMMENT 'Players with their latest team'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE playersWithTeam;
SELECT P.player_name, P.team_name
FROM playersWithTeamDates P
    JOIN (SELECT player_name, max(unix_timestamp(datef, 'yyyy-MM-dd')) datef FROM playersWithTeamDates GROUP BY player_name) maxD
        ON maxD.player_name = P.player_name
WHERE from_unixtime(maxD.datef) = P.datef;

DROP TABLE IF EXISTS playersWithScore;
CREATE TABLE IF NOT EXISTS playersWithScore ( 
    player_name string,
    player_score double
)
COMMENT 'Players with their score'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE playersWithScore
SELECT P.player_name,
    AVG(A.overall_rating)/100*0.2 +
    SUM((CASE date_format(A.datef,"yyyy") 
            WHEN '2014' THEN 0.45 
            WHEN '2015' THEN 0.6 
            WHEN '2016' THEN 0.9 END)*A.overall_rating)/(COUNT(A.overall_rating)*100)*0.1+
    (CASE 
        WHEN MAX(A.potential) - AVG(A.potential) >= 0 AND MAX(A.potential) - AVG(A.potential) < 1.5 THEN 0.9
        WHEN MAX(A.potential) - AVG(A.potential) >= 1.5 AND MAX(A.potential) - AVG(A.potential) < 3 THEN 0.6
        ELSE 0.3 END)*0.15+
    (CASE 
        WHEN MAX(A.overall_rating) - AVG(A.overall_rating) >= 0 AND MAX(A.overall_rating) - AVG(A.overall_rating) < 1.5 THEN 0.9
        WHEN MAX(A.overall_rating) - AVG(A.overall_rating) >= 1.5 AND MAX(A.overall_rating) - AVG(A.overall_rating) < 3 THEN 0.6
        ELSE 0.3 END)*0.15 + 
    (SUM((CASE 
        WHEN A.overall_rating >= 85 AND A.overall_rating <= 100 THEN 1
        WHEN A.overall_rating >= 80 AND A.overall_rating < 85 THEN 0.9
        WHEN A.overall_rating >= 75 AND A.overall_rating < 80 THEN 0.890
        WHEN A.overall_rating >= 73 AND A.overall_rating < 75 THEN 0.750
        WHEN A.overall_rating >= 70 AND A.overall_rating < 73 THEN 0.3
        WHEN A.overall_rating >= 65 AND A.overall_rating < 70 THEN 0.25
        WHEN A.overall_rating >= 55 AND A.overall_rating < 65 THEN 0.1
        ELSE 0.05 END))/COUNT(A.overall_rating))*0.4
FROM limitedPlayers P
JOIN tmp_player_attributes A
    ON (P.player_api_id = A.player_api_id)
WHERE 
    P.player_api_id is not null 
    AND A.player_api_id is not null
    AND A.datef BETWEEN '2014-01-01 00:00:00' AND '2016-21-31 23:59:59'
GROUP BY P.player_name;

DROP TABLE IF EXISTS tmp_result;
CREATE TABLE IF NOT EXISTS tmp_result ( 
    playerName string,
    teamName string,
    score double
)
COMMENT 'Result'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE tmp_result
SELECT ps.player_name, pt.team_name, ps.player_score
FROM playersWithScore ps
    JOIN playersWithTeam pt
        ON (ps.player_name = pt.player_name);

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoopuser/result' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
select * from tmp_result;