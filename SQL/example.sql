CREATE TABLE DIRECTIONS(
    DID number primary key,
    NAME varchar2(20)
);


CREATE TABLE WEATHER_STATION (
    WID number primary key ,
    WNAME varchar2(4) ,
    DID number CONSTRAINT fk_id REFERENCES DIRECTIONS(DID)
);

INSERT INTO DIRECTIONS VALUES
                           (1, 'central_west');
INSERT INTO DIRECTIONS VALUES
                           (2, 'north');
INSERT INTO DIRECTIONS VALUES
                           (3, 'south');
INSERT INTO DIRECTIONS VALUES
                           (4, 'northeast');
INSERT INTO DIRECTIONS VALUES
                           (5, 'southeast');


INSERT INTO WEATHER_STATION VALUES
                                 (1, 'A719', 1);
INSERT INTO WEATHER_STATION VALUES
                                (2, 'A724', 1);
INSERT INTO WEATHER_STATION VALUES
                                (3, 'A043', 2);
INSERT INTO WEATHER_STATION VALUES
                                (4, 'A202', 2);
INSERT INTO WEATHER_STATION VALUES
                                (5, 'A841', 3);
INSERT INTO WEATHER_STATION VALUES
                                (6, 'A838', 3);
INSERT INTO WEATHER_STATION VALUES
                                (7, 'A366', 4);
INSERT INTO WEATHER_STATION VALUES
                                (8, 'A443', 4);
INSERT INTO WEATHER_STATION VALUES
                                (9, 'A618', 5);
INSERT INTO WEATHER_STATION VALUES
                                (10, 'A748', 5);


commit;


SELECT min(DID) FROM WEATHER_STATION;

SELECT avg("max. temperature in the previous hour (°c)") FROM SQL_DATA_0
WHERE  SQL_DATA_0.STATION_CODE = 10;


-- Select all stations in the north
SELECT WNAME FROM WEATHER_STATION w
INNER JOIN DIRECTIONS D on w.DID = D.DID
WHERE D.NAME = 'southeast';


-- Select Data from one station
SELECT avg("max. temperature in the previous hour (°c)") FROM SQL_DATA_0
INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
WHERE WS.WNAME = 'A748';

-- Select Data from one station during a time range
SELECT * FROM SQL_DATA_0
INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
WHERE WS.WNAME = 'A724' and SQL_DATA_0.TIMESTAMP BETWEEN TO_TIMESTAMP('01-Jan-10') and TO_TIMESTAMP('20-Jan-10');

-- Select all the data from the north
SELECT * FROM SQL_DATA_0
INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
INNER JOIN DIRECTIONS D on D.DID = WS.DID
WHERE D.NAME = 'north';

-- Simply select data from the station code 2
SELECT * FROM SQL_DATA_0
WHERE SQL_DATA_0.STATION_CODE = 2;

-- Get the average temperature from the north between 2006 and 2007
SELECT avg("max. temperature in the previous hour (°c)") FROM SQL_DATA_0
INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
INNER JOIN DIRECTIONS D on D.DID = WS.DID
WHERE D.NAME = 'north' and SQL_DATA_0.TIMESTAMP BETWEEN TO_TIMESTAMP('01-Jan-06') and TO_TIMESTAMP('31-Dez-07');

-- Get the average temperature from the north between 2006 and 2007
SELECT sum("max. temperature in the previous hour (°c)") FROM SQL_DATA_0
INNER JOIN WEATHER_STATION WS on SQL_DATA_0.STATION_CODE = WS.WID
INNER JOIN DIRECTIONS D on D.DID = WS.DID
WHERE D.NAME = 'north' and SQL_DATA_0.TIMESTAMP BETWEEN TO_TIMESTAMP('01-Jan-06') and TO_TIMESTAMP('31-Dez-07');


