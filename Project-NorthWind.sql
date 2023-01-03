# 1. Calculate average Unit Price for each CustomerId

with Cust_Avg_UnitPrice AS(
SELECT distinct(T.CustomerID) as Cust_ID, avg(T.Order_UnitPrice) over(partition by CustomerID) 
as AVG_UnitPrice FROM (SELECT CustomerID,round(sum(UnitPrice),2) AS Order_UnitPrice 
FROM customers INNER JOIN orders USING(CustomerID) 
INNER JOIN order_details USING(OrderID) GROUP BY CustomerID,OrderID) AS T 
)
SELECT Cust_ID, round(AVG_UnitPrice,2) as Avg_UnitPrice from Cust_Avg_UnitPrice;


# 2. Calculate average Unit Price for each group of CustomerId AND EmployeeId

with AVG_CUST_EMP_UnitPrice AS
(SELECT distinct (T.CustomerID) as Cust_ID, T.EmployeeID as Emp_ID, avg(T.Order_UnitPrice) 
over(partition by CustomerID,EmployeeID) as AVG_UnitPrice
FROM (SELECT CustomerID, EmployeeID, OrderID, round(sum(UnitPrice),2) AS Order_UnitPrice 
FROM customers INNER JOIN orders USING(CustomerID) 
INNER JOIN order_details USING(OrderID) GROUP BY CustomerID, EmployeeID, OrderID) AS T)
SELECT Cust_ID,Emp_ID,round(AVG_UnitPrice,2) as AVG_CUST_EMP_UnitPrice from AVG_CUST_EMP_UnitPrice;


# 3. Rank Unit Price in descending order for each CustomerId

with CTE AS(
SELECT T.CustomerID as Cust_ID, T.total_UnitPrice as total_UnitPrice, 
rank() over(order by T.total_UnitPrice desc) as Price_rank from
(SELECT  CustomerID,sum(UnitPrice) as total_UnitPrice
from customers inner join orders using(CustomerID)
inner join order_details using(OrderID) group by CustomerID) AS T)
SELECT Cust_ID,round(total_UnitPrice,2) as total_UnitPrice, Price_rank from CTE;


# 4(1). How can you pull the previous order date’s Quantity (until previous Date) for each ProductId 

with CTE AS(
SELECT T.ProductID, T.OrderDate, T.tot_quantity, sum(T.tot_quantity)
over (partition by T.ProductID order by T.ProductID rows unbounded preceding) as cum_quantity
FROM (SELECT o.ProductID , p.OrderDate, 
sum(o.Quantity) as tot_quantity
FROM order_details o 
INNER JOIN orders p USING(OrderID) 
GROUP BY ProductID, OrderDate ORDER BY ProductID) as T)
SELECT ProductID, OrderDate, tot_quantity, cum_quantity,
lag(cum_quantity,1,0) 
over(partition by ProductID order by ProductID) as prev_date_quantity FROM CTE;

# 4. How can you pull the previous order date’s Quantity for each ProductId

SELECT T.ProductID, T.OrderDate, T.tot_quantity, lag(T.tot_quantity,1,0)
over (partition by T.ProductID order by T.ProductID) as prev_quantity
FROM (SELECT o.ProductID , p.OrderDate, 
sum(o.Quantity) as tot_quantity
FROM order_details o 
INNER JOIN orders p USING(OrderID) 
GROUP BY ProductID, OrderDate ORDER BY ProductID) as T;


# 5. How can you pull the following order date’s Quantity for each ProductId

SELECT T.ProductID, T.OrderDate, T.tot_quantity, lead(T.tot_quantity,1,0)
over (partition by T.ProductID order by T.ProductID) as following_quantity
FROM (SELECT o.ProductID , p.OrderDate, 
sum(o.Quantity) as tot_quantity
FROM order_details o 
INNER JOIN orders p USING(OrderID) 
GROUP BY ProductID, OrderDate ORDER BY ProductID) as T;


# 6. Pull out the very first Quantity ever ordered for each ProductId

SELECT distinct(T.ProductID), FIRST_VALUE(T.OrderDate) over(partition by T.ProductID order by T.OrderDate) as Order_Date,
FIRST_VALUE(T.tot_quantity) over(partition by T.ProductID order by T.OrderDate) as First_Quantity
FROM (SELECT o.ProductID , p.OrderDate, sum(o.Quantity) as tot_quantity
FROM order_details o 
INNER JOIN orders p USING(OrderID) 
GROUP BY ProductID, OrderDate ORDER BY ProductID) as T;


# 7. Calculate a cumulative moving average UnitPrice for each CustomerId

WITH CTE AS 
(SELECT *, avg(Order_UnitPrice) over(partition by CustomerID 
order by OrderID rows unbounded preceding) as running_avg_UnitPrice
FROM (SELECT CustomerID, OrderID, round(sum(UnitPrice),2) AS Order_UnitPrice 
FROM customers INNER JOIN orders USING(CustomerID) 
INNER JOIN order_details USING(OrderID) group by CustomerID, OrderID)as T)
SELECT CustomerID, OrderID, Order_UnitPrice,
round(running_avg_UnitPrice,2) as running_avg_UnitPrice FROM CTE;

#Theoretical questions:
------------------------

# 1. Can you define a trigger that is invoked automatically before a new row is inserted into a table?

-- 'BEFORE INSERT' Trigger is used to invoke automatically befora a new row is inserted into a table.
-- For example:
DELIMITER $$ 
CREATE TRIGGER before_insert_student BEFORE INSERT ON 
students FOR EACH ROW
BEGIN
INSERT INTO student_log VALUES(new.roll_no, new.name, new.branch,now());
END $$
-- Here, the before_insert trigger is triggered automatically for each insert and the student_log table records the values entered to the student table along with the timestamp
-- Triggers can be used to check for a particular condition and to handle error automatically and accordingly
-- and can also be used for logging

# 2. What are the different types of triggers?

#There are 6 different type of triggers:
-- BEFORE INSERT :- trigger that is invoked automatically before a new row is inserted into a table
-- BEFORE UPDATE :- trigger that is invoked automatically before a row is updated in a table
-- BEFORE DELETE :- trigger that is invoked automatically before a row is deleted from a table
-- AFTER INSERT :- trigger that is invoked automatically after a new row is inserted into a table
-- AFTER UPDATE :- trigger that is invoked automatically after a row is updated in a table
-- AFTER DELETE :- trigger that is invoked automatically after a row is deleted from a table

# 3. How is Metadata expressed and structured?

-- Metadata is the data of the data that is stored in the database. It holds information about
-- tables, columns, data types, table relationship, constraints etc
-- Metadata can be obtained by 'information_schema'
-- Information Schema (information_schema) is an ANSI-standard set of read-only views that provide 
-- information about all of the tables, views, columns, and procedures in a database.
-- information_schema.tables, information_schema.views, information_schema.statistics, 
-- information_schema.columns, information_schema.routines can be used accordingly.
-- Show and Describe commands can also be used to get metadata of the database.

# 4. Explain RDS and AWS key management services

# Amazon AWS KMS:
----------------
-- AWS Key Management Service (AWS KMS) is a Amazon managed service that makes it easy
-- to create and control the cryptographic keys that are used to protect our data. 
-- AWS KMS uses hardware security modules (HSM) to protect and validate the AWS KMS keys.
-- With AWS KMS, we'll have more control over access to data we encrypt. 
-- We can use the key management and cryptographic features directly in our applications 
-- or through AWS services integrated with AWS KMS. Whether we write applications for AWS 
-- or use AWS services, AWS KMS enables us to maintain control over who can use our AWS KMS keys
-- and gain access to our encrypted data.
-- AWS KMS integrates with AWS CloudTrail, a service that delivers log files to the 
-- Amazon S3 bucket. By using CloudTrail we can monitor and investigate how 
-- and when our KMS keys have been used and who used them.

# Amazon RDS:
-------------
-- Amazon Relational Database Service (Amazon RDS) is a managed Database-as-a-Service (DBaaS)
-- that makes it easier to set up, operate, and scale a relational database in the AWS Cloud.
-- It provides cost-efficient, resizable capacity for an industry-standard relational database 
-- and manages common database administration tasks.
-- It extends its service to Oracle, SQL Server, MySQL, MariaDB, PostgreSQL and Amazon Aurora clients.
-- Amazon RDS manages backups, software patching, automatic failure detection, and recovery.
-- These backups can be used to restore a database during DR(Disaster Recovery). 
-- Critical systems should have high availabilty and 
-- Amazon RDS ensures high availability with a primary instance 
-- and a synchronous secondary instance that can  be failed over to when problems occur. 
-- We can use read replicas to increase read scaling.
-- In addition, it provides security by giving us the control of who can access 
-- our RDS databases using AWS Identity and Access Management (IAM) 
-- to define users and permissions. 
-- We can also protect our databases by putting them in a virtual private cloud (VPC) 


# 5. What is the difference between Amazon EC2 and RDS?

-- Amazon Elastic Compute Cloud (Amazon EC2) is a web service that provides 
-- secure access to server instances on demand.

-- Amazon Relational Database Service (Amazon RDS) is a managed Database-as-a-Service (DBaaS) that 
-- makes it easy for IT administrators to set up, operate, and scale relational databases in the
-- Amazon cloud.

-- 1. Administration:
------------------
-- Amazon RDS is easy to set up. AWS automates the entire process of management, maintenance, 
-- and security allowing the customer to focus on essential tasks and not on routine maintenance. 
-- There are multiple options to access the powerful capabilities of a production-ready 
-- relational database like the AWS Management Console, the AWS RDS command-line interface, 
-- or simple REST API calls

-- In Amazon EC2, we are in complete control of the OS installed, 
-- database version and configuration, and other software components. 
-- All routine maintenance activities, including patches, upgrades, backup, replication, 
-- and clustering, are our responsibility.

-- 2. High Availability:
---------------------
-- Amazon RDS has built-in high availability. It automatically creates a primary DB instance
-- and replicates the data side-by-side to a standby instance in a different Amazon Availability Zone. 
-- This feature ensures that if there is an outage in one zone, database can be recovered from 
-- the other zone.

-- In Amazon EC2, it is customer's responsibility to configure the database server in a 
-- highly available cluster.

-- 3. Backups:
---------------
-- In Amazon RDS, we can set up backups to be automated. AWS Cloudwatch can be used to 
-- receive events on backup failures, completion, etc. Also, the database snapshots 
-- can be taken on-demand and can be kept as long as needed.

-- In Amazon EC2, backups have to be enabled manually by us and will need 
-- manual  monitoring to make sure that regular backups are happening. 
-- One cannot use AWS Cloudwatch for this.

-- 4. Scalability:
------------------
-- Amazon RDS integrates seamlessly with Amazon’s scaling tools for both horizontal and 
-- vertical scaling. If needed to scale vertically to a larger or more powerful instance, 
-- it can be done in a few clicks. Also, if needed to scale horizontally, 
-- spinning up additional read replicas can be automated, so the system instantly responds 
-- to increasing usage demands for read-only workloads.

-- Amazon EC2 requires to set up a scalable architecture manually. This process 
-- may involve setting up multiple EC2 instances, load balancing between them, configuring 
-- Availability Groups, Sharding, etc.

-- 5. Support and Control:
--------------------------
-- Amazon RDS is limited to the database engines supported by Amazon and by the versions supported. 
-- Since we do not manage the database server, upgrades and patches are managed by Amazon.
-- No direct access to the system hosting the database; 
-- however, gives access to the database administration tools to perform any activities 
-- required on the database server

-- With Amazon EC2, can install any database engine and version and is not limited 
-- by the version and configuration AWS supports in RDS (e.g., 
-- IBM DB2 is not supported by AWS RDS, but can install DB2 on an EC2 instance and operate it).
-- Full access to the operating system and the database server is granted. Hence, can apply
-- any updates and patches and can configure maintenance windows. 
-- Option to choose whether to run one or multiple instances on the same EC2 instance, 
-- and the ports used.





