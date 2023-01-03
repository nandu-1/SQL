create database hotel_booking;
use hotel_booking;

/*
Table creation: Hotel, Room, Guest, Booking, (Countries, Locations) for Address
*/

CREATE TABLE countries(country_id varchar(2) PRIMARY KEY, 
country_name VARCHAR(50)) ENGINE=InnoDB;

CREATE TABLE locations(location_id VARCHAR(4) PRIMARY KEY, 
street_address VARCHAR(240),
postal_code VARCHAR(12), city VARCHAR(30), 
state_province VARCHAR(25),
country_id VARCHAR(2), 
FOREIGN KEY(country_id) REFERENCES countries(country_id)) ENGINE=InnoDB;

CREATE TABLE hotel (hotel_no VARCHAR(4) PRIMARY KEY, 
`name` VARCHAR(50) NOT NULL, 
location_ID VARCHAR(4), 
FOREIGN KEY(location_id) REFERENCES locations(location_id)) ENGINE=InnoDB;

CREATE TABLE room (room_no VARCHAR(4), hotel_no VARCHAR(4), 
`type` VARCHAR(20) NOT NULL, 
price DECIMAL(8,2) NOT NULL, 
PRIMARY KEY(room_no,hotel_no), 
FOREIGN KEY room(hotel_no) 
REFERENCES hotel(hotel_no) ON DELETE CASCADE ON UPDATE CASCADE) 
ENGINE=InnoDB;

ALTER TABLE room ADD CONSTRAINT FOREIGN KEY room(hotel_no) 
REFERENCES hotel(hotel_no) ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE guest (guest_no VARCHAR(4) PRIMARY KEY, 
`name` VARCHAR(20) NOT NULL,
location_id VARCHAR(4), 
FOREIGN KEY(location_id) REFERENCES locations(location_id)) ENGINE=InnoDB;

CREATE TABLE booking (hotel_no VARCHAR(4), guest_no VARCHAR(4), date_from
DATETIME NOT NULL, date_to DATETIME DEFAULT NULL, room_no VARCHAR(4),
PRIMARY KEY(hotel_no,guest_no,date_from,room_no), 
FOREIGN KEY(hotel_no) REFERENCES hotel(hotel_no),
FOREIGN KEY(guest_no) REFERENCES guest(guest_no), 
FOREIGN KEY(room_no) REFERENCES room(room_no)) ENGINE=InnoDB;

ALTER TABLE booking ADD CONSTRAINT 
FOREIGN KEY booking(hotel_no) REFERENCES hotel(hotel_no)
ON DELETE CASCADE ON UPDATE CASCADE, 
ADD CONSTRAINT 
FOREIGN KEY(guest_no) REFERENCES guest(guest_no) ON DELETE CASCADE;

-- #############################################################################################################

-- load data to countries
load data infile 'C:/mysql/countries.csv'
into table countries
fields terminated by ','
enclosed by '"'
lines terminated by '\n';

-- load data to locations
load data infile 'C:/mysql/locations.csv'
into table locations
fields terminated by ','
enclosed by '"'
lines terminated by '\n';

-- load data to hotel
load data infile 'C:/mysql/hotel.csv'
into table hotel
fields terminated by ','
enclosed by '"'
lines terminated by '\n';

-- load data to room
load data infile 'C:/mysql/room.csv'
into table room
fields terminated by ','
enclosed by '"'
lines terminated by '\n';

-- load data booking
load data infile 'C:/mysql/booking.csv'
into table booking
fields terminated by ','
enclosed by '"'
lines terminated by '\n';

-- load data guest
load data infile 'C:/mysql/guest.csv'
into table guest
fields terminated by ','
enclosed by '"'
lines terminated by '\n';

-- ###############################################################################################

-- UPDATE 
-- =======
drop table booking_old;
CREATE TABLE booking_old like booking;

DELIMITER $$
create procedure delete_append(IN date_to_delete DATETIME)
BEGIN
INSERT INTO booking_old(select * from booking 
where date_from < date_to_delete);
delete from booking where date_from < date_to_delete;
select * from booking_old;
END$$
call delete_append('2000-01-01');

-- ###############################################################################################

-- SIMPLE QUERIES
-- ===============
-- List full details of all hotels
select * from hotel h inner join room r using (hotel_no) 
inner join booking b using(hotel_no,room_no) 
inner join locations l using(location_id);

-- List full details of all hotels in London
select * from hotel h inner join room r using (hotel_no) 
inner join booking b using(hotel_no,room_no) 
inner join locations l using(location_id) where l.city = 'London';

-- List the names and addresses of all guests in London, alphabetically ordered by name
select name,locations.* from guest,locations 
where 
guest.location_id=locations.location_id and 
locations.city='London' order by guest.name;

-- List all double or family rooms with a price below £40.00 per night, in ascending order of price
select * from room where room.`type` NOT IN('Single') 
and room.price<(150.00*91.98) ;

-- List the bookings for which no date_to has been specified
select * from booking where date_to is NULL;

-- ==============================================================================================

-- AGGREGATE FUNCTIONS
-- ====================
-- How many hotels are there?
select count(*) from hotel;

-- What is the average price of a room?
select avg(price),`type`,hotel_no from room 
where price IN (select sum(price) from room group by hotel_no,`type`) 
group by hotel_no,`type`;

-- What is the total revenue per night from all double rooms?
select sum(price) from room where `type` IN ('Double');

-- How many different guests have made bookings for August?
select distinct(count(*)) as august_bookings from booking 
where monthname(date_from) IN ('August');

-- ==================================================================================================
-- SUBQUERIES AND JOINS
-- =====================

-- List the price and type of all rooms at the Grosvenor Hotel
Select `type`, price from  room inner join hotel using(hotel_no) 
where 
hotel.name='Grosvenor Hotel';

-- List all guests currently staying at the Grosvenor Hotel
select guest_no from booking inner join hotel using(hotel_no) 
where hotel.name='Grosvenor Hotel' and date_from<=current_date() 
and date_to>=current_date();

/* List the details of all rooms at the Grosvenor Hotel, including the name of the guest 
 staying in the room, if the room is occupied */
select room.*,hotel.name as hotel_name,guest.name as guest_name from room 
inner join hotel using(hotel_no) 
inner join booking using(hotel_no,room_no)
inner join guest using(guest_no) where hotel.name='Grosvenor Hotel' 
and date_from<=current_date() and date_to>=current_date();

-- What is the total income from bookings for the Grosvenor Hotel today?
select sum(price) from room 
inner join booking using(room_no,hotel_no) where hotel_no 
IN(select hotel_no from hotel where name='Grosvenor Hotel') 
and date_from<=current_date()
and date_to>=current_date();

-- List the rooms that are currently unoccupied at the Grosvenor Hotel.
select room.* from room where room_no IN (select room_no from booking
inner join hotel using(hotel_no)
where date_from>current_date() and hotel.name='Grosvenor Hotel');

-- What is the lost income from unoccupied rooms at the Grosvenor Hotel?
select sum(price) from room where room_no IN (select room_no from booking
inner join hotel using(hotel_no)
where date_from>current_date() and hotel.name='Grosvenor Hotel');

-- ===============================================================================================

-- GROUPING
-- =========

-- List the number of rooms in each hotel
select count(*) as no_of_rooms,hotel_no from room group by hotel_no;

-- List the number of rooms in each hotel in London
select count(*) as no_of_rooms,name from room 
inner join hotel using(hotel_no)
inner join locations using(location_id) where locations.city='London' 
group by room.hotel_no;

-- What is the average number of bookings for each hotel in August?
select a.hotel_no,avg(a.cnt) avg_booking from 
(select count(distinct august_booking.guest_no) as cnt,hotel_no 
from booking b left outer join (select * from booking inner join hotel 
using(hotel_no) where (monthname(date_from) IN ('August'))) 
august_booking using (hotel_no) group by hotel_no)a group by a.hotel_no;
 
-- What is the most commonly booked room type for each hotel in London?
select max(t.no_of_type) as max_no,t.hotel_no,t.type  from 
(select count(type) as no_of_type, hotel_no,room.type from booking 
inner join room using(room_no,hotel_no) 
inner join hotel using (hotel_no)
inner join locations using(location_id) where locations.city='London' 
group by hotel_no,room.type)t group by t.hotel_no order by t.type;

-- What is the lost income from unoccupied rooms at each hotel today?
select sum(unoc.price), unoc.hotel_no from 
(select * from booking inner join room using(hotel_no,room_no) 
inner join hotel using (hotel_no) where date_from>now() group by hotel_no)
unoc  group by unoc.hotel_no;


