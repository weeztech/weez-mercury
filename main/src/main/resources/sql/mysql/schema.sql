#language=MySQL

#创建数据库

DROP DATABASE IF EXISTS `weez-mercury`;

CREATE DATABASE `weez-mercury`
  DEFAULT CHARACTER SET utf8;

USE `weez-mercury`;

DELIMITER $$

CREATE PROCEDURE sys_create_weez_db_users()
  BEGIN

#创建读写用户
    IF exists(SELECT *
              FROM mysql.user
              WHERE user = 'weez' AND host = 'localhost')
    THEN DROP USER 'weez'@'localhost';
    END IF;
    GRANT ALL ON `weez-mercury`.*
    TO 'weez'@'localhost'
    IDENTIFIED BY 'weez';

#创建只读用户

    IF exists(SELECT *
              FROM mysql.user
              WHERE user = 'weez-ro' AND host = 'localhost')
    THEN DROP USER 'weez-ro'@'localhost';
    END IF;

    GRANT SELECT, SHOW VIEW ON `weez-mercury`.*
    TO 'weez-ro'@'localhost'
    IDENTIFIED BY 'weez';
  END

$$ DELIMITER ;

#创建用户
CALL sys_create_weez_db_users();