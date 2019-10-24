-- MySQL dump 10.17  Distrib 10.3.14-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: SPC
-- ------------------------------------------------------
-- Server version	10.3.14-MariaDB-1:10.3.14+maria~xenial

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `Catalog`
--

DROP TABLE IF EXISTS `Catalog`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Catalog` (
  `catsid` smallint(6) NOT NULL,
  `catpid` smallint(6) NOT NULL,
  `catcost` decimal(10,3) DEFAULT NULL,
  `catqnt` int(11) DEFAULT NULL,
  `catdate` date DEFAULT NULL,
  `catchk` tinyint(1) DEFAULT NULL,
  KEY `CATALOG_PARTSCATALOG` (`catpid`),
  KEY `CATALOG_SUPPLIERSCATALOG` (`catsid`),
  CONSTRAINT `CATALOG_PARTSCATALOG` FOREIGN KEY (`catpid`) REFERENCES `Parts` (`pid`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `CATALOG_SUPPLIERSCATALOG` FOREIGN KEY (`catsid`) REFERENCES `Suppliers` (`sid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `Catalog`
--

LOCK TABLES `Catalog` WRITE;
/*!40000 ALTER TABLE `Catalog` DISABLE KEYS */;
INSERT INTO `Catalog` VALUES (1081,991,36.100,300,'2014-12-20',1),(1081,992,42.300,400,'2014-12-20',1),(1081,993,15.300,200,'2014-03-03',0),(1081,994,20.500,100,'2014-03-03',0),(1081,995,20.500,NULL,NULL,0),(1081,996,124.230,NULL,NULL,0),(1081,997,124.230,NULL,NULL,0),(1081,998,11.700,400,'2014-09-10',1),(1081,999,75.200,NULL,NULL,0),(1082,991,16.500,200,'2014-09-10',1),(1082,997,0.550,100,'2014-09-10',1),(1082,998,7.950,200,'2014-03-03',0),(1083,998,12.500,NULL,NULL,0),(1083,999,1.000,NULL,NULL,0),(1084,994,57.300,100,'2014-12-20',1),(1084,995,22.200,NULL,NULL,0),(1084,998,48.600,200,'2014-12-20',1);
/*!40000 ALTER TABLE `Catalog` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `Parts`
--

DROP TABLE IF EXISTS `Parts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Parts` (
  `pid` smallint(6) NOT NULL,
  `pname` varchar(255) DEFAULT NULL,
  `pcolor` varchar(255) DEFAULT NULL,
  `pweight` decimal(10,3) DEFAULT NULL,
  `punit` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`pid`),
  UNIQUE KEY `SYS_IDX_10375` (`pid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `Parts`
--

LOCK TABLES `Parts` WRITE;
/*!40000 ALTER TABLE `Parts` DISABLE KEYS */;
INSERT INTO `Parts` VALUES (991,'Left Handed Bacon Stretcher Cover','Red',15.500,'lb'),(992,'Smoke Shifter End','Black',3.750,'lb'),(993,'Acme Widget Washer','Red',142.880,'kg'),(994,'Acme Widget Washer','Silver',142.880,'kg'),(995,'I Brake for Crop Circles Sticker',NULL,NULL,NULL),(996,'Anti-Gravity Turbine Generator','Cyan',NULL,NULL),(997,'Anti-Gravity Turbine Generator','Magenta',NULL,NULL),(998,'Fire Hydrant Cap','Red',7.200,'lb'),(999,'7 Segment Display','Green',2.100,'gr');
/*!40000 ALTER TABLE `Parts` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `Suppliers`
--

DROP TABLE IF EXISTS `Suppliers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Suppliers` (
  `sid` smallint(6) NOT NULL,
  `sname` varchar(255) DEFAULT NULL,
  `saddress` varchar(255) DEFAULT NULL,
  `scountry` varchar(255) DEFAULT NULL,
  `scity` varchar(255) DEFAULT NULL,
  `sstatus` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`sid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `Suppliers`
--

LOCK TABLES `Suppliers` WRITE;
/*!40000 ALTER TABLE `Suppliers` DISABLE KEYS */;
INSERT INTO `Suppliers` VALUES (1081,'Acme Widget Suppliers','1 Grub St., Potemkin Village, IL 61801','USA','ILLINOIS',10),(1082,'Big Red Tool and Die','4 My Way, Bermuda Shorts, OR 90305','USA','OREGON',20),(1083,'Perfunctory Parts','99 Short Pier, Terra Del Fuego, TX 41299','SPAIN','MADRID',30),(1084,'Alien Aircaft Inc.','2 Groom Lake, Rachel, NV 51902','UK','NOTTINGHAM',10);
/*!40000 ALTER TABLE `Suppliers` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'SPC'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-09-05  8:14:11
