/** language=MySQL **/

/** 员工 **/
CREATE TABLE biz_staffs (
  id       BIGINT PRIMARY KEY NOT NULL,
  code     VARCHAR(254)       NOT NULL,
  name     VARCHAR(254)       NOT NULL,
  password VARBINARY(254)     NOT NULL
);

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
  model_id    BIGINT             NOT NULL,
  code        VARCHAR(254)       NOT NULL,
  name        VARCHAR(254)       NOT NULL,
  description VARCHAR(254)       NOT NULL,
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
  description VARCHAR(254),
  FOREIGN KEY (id) REFERENCES biz_staffs (id)
);

/** 房间 **/
CREATE TABLE biz_rooms (
  id          BIGINT PRIMARY KEY NOT NULL,
  title       VARCHAR(254)       NOT NULL,
  price       DECIMAL(18, 2)     NOT NULL,
  description VARCHAR(254)
);

CREATE TABLE biz_stock_types (
  id    INT PRIMARY KEY NOT NULL,
  code  VARCHAR(254)    NOT NULL,
  title VARCHAR(254)    NOT NULL
);

INSERT INTO biz_stock_types (id, code, title)
VALUES
  (1, 'store', '仓库'),
  (2, 'customer', '顾客'),
  (3, 'provider', '供应商'),
  (4, 'department', '部门'),
  (5, 'staff', '员工');

#库，物品流转得基数据，
CREATE TABLE biz_stocks (
  id      BIGINT PRIMARY KEY NOT NULL,
  type_id INT                NOT NULL,
  code    VARCHAR(254)       NOT NULL,
  title   VARCHAR(254)       NOT NULL,
  FOREIGN KEY (type_id) REFERENCES biz_stock_types (id)
);

# 流水
CREATE TABLE biz_stock_ios (
  time       DATETIME       NOT NULL COMMENT '时间',
  shangpin_type INT,
  product_id BIGINT         NOT NULL COMMENT '物品ID',
  qty        DECIMAL(18, 2) NOT NULL COMMENT '数量',
  from_id    BIGINT         NOT NULL COMMENT '来源ID',
  to_id      BIGINT         NOT NULL COMMENT '目标ID'
);
