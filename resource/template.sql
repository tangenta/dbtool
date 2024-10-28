CREATE TABLE `test`.`t` (
  `col1` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  /*{{ rownum }}*/
  `col2` bigint(20) unsigned NOT NULL,
  /*{{ rand.range(1, 9223372036854775808) }}*/
  `col3` bigint(20) unsigned NOT NULL  ,
  /*{{ rand.range(1, 9223372036854775808) }}*/
  `col4` bigint(20) unsigned NOT NULL  ,
  /*{{ rand.range(1, 9223372036854775808) }}*/
  `col5` bigint(20) unsigned NOT NULL  ,
  /*{{ rownum }}*/
  `col6` tinyint(3) unsigned NOT NULL  ,
  /*{{ rand.range(1, 10) }}*/
  `col7` varchar(10) NOT NULL  ,
  /*{{ rand.regex('[a-z0-9]+', '', 5) }}*/
  `col8` decimal(36,18) NOT NULL DEFAULT '0.000000000000000000'  ,
  /*{{ rand.range(1, 100000) }}*/
  `col9` tinyint(4) NOT NULL DEFAULT '0'  ,
  /*{{ rand.range(0, 20) }}*/
  `col10` tinyint(4) NOT NULL  ,
  /*{{ rand.range(0, 5) }}*/
  `col11` bigint(20) unsigned NOT NULL DEFAULT '0'  ,
  /*{{ rand.range(1610535068, 1676882755) }}*/
  `col12` bigint(20) unsigned NOT NULL DEFAULT '0'  ,
  /*{{ rand.range(1610535068, 1676882755) }}*/
  `col13` bigint(20) unsigned NOT NULL DEFAULT '0'  ,
  /*{{ rand.range(1, 10000) }}*/
  PRIMARY KEY (`col1`)
);
