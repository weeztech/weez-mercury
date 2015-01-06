/** 创建数据库 **/
DROP DATABASE IF EXISTS `weez-mercury`;
CREATE DATABASE `weez-mercury`
  DEFAULT CHARACTER SET utf8;
USE `weez-mercury`;

/** 创建可读写用户 **/
DROP USER 'weez'@'localhost';
GRANT ALL ON `weez-mercury`.*
TO 'weez'@'localhost'
IDENTIFIED BY 'weez';

/** 创建只读用户 **/
DROP USER 'weez－ro'@'localhost';
GRANT SELECT, SHOW VIEW ON `weez-mercury`.*
TO 'weez－ro'@'localhost'
IDENTIFIED BY 'weez';

/** 员工 **/
CREATE TABLE biz_staffs (
  id       BIGINT PRIMARY KEY NOT NULL,
  code     VARCHAR(254)       NOT NULL,
  name     VARCHAR(254)       NOT NULL,
  password VARBINARY(254)     NOT NULL
);

INSERT INTO biz_staffs (id, code, name, password)
VALUES (1, 'test', 'test', 0x098F6BCD4621D373CADE4E832627B4F6);


/** 商品模型 **/
CREATE TABLE biz_product_models (
  id          BIGINT PRIMARY KEY NOT NULL,
  code        VARCHAR(254)       NOT NULL,
  title       VARCHAR(254)       NOT NULL,
  price       DOUBLE             NOT NULL,
  description VARCHAR(254)       NOT NULL
);


/** 商品 **/
CREATE TABLE biz_products (
  id          BIGINT PRIMARY KEY NOT NULL,
  code        VARCHAR(254)       NOT NULL,
  name        VARCHAR(254)       NOT NULL,
  description VARCHAR(254)       NOT NULL,
  model_id    BIGINT             NOT NULL,
  FOREIGN KEY (model_id) REFERENCES biz_product_models (id)
);
CREATE INDEX model_fk ON biz_products (model_id);

/** 商品价格 **/
CREATE TABLE biz_product_prices (
  product_id BIGINT         NOT NULL,
  start_time DATETIME       NOT NULL,
  end_time   DATETIME       NOT NULL,
  price      DECIMAL(18, 2) NOT NULL,
  PRIMARY KEY (product_id, start_time)
);

/** 助理 **/
CREATE TABLE biz_assistants (
  id          BIGINT PRIMARY KEY NOT NULL,
  price       DOUBLE             NOT NULL,
  description VARCHAR(254)       NOT NULL,
  FOREIGN KEY (id) REFERENCES biz_staffs (id)
);

/** 房间 **/
CREATE TABLE biz_rooms (
  id          BIGINT PRIMARY KEY NOT NULL,
  title       VARCHAR(254)       NOT NULL,
  price       DECIMAL(18, 2)     NOT NULL,
  description VARCHAR(254)       NOT NULL
);
