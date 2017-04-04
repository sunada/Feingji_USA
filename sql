CREATE TABLE `fund_history` (
  `ticker` char(6) NOT NULL DEFAULT '',
  `deal_date` date NOT NULL DEFAULT '0000-00-00',
  `price` decimal(9,2) DEFAULT NULL,
  `NAT` decimal(9,2) DEFAULT NULL,
  `discount` decimal(9,4) DEFAULT NULL,
  `ava` decimal(9,4) DEFAULT NULL,
  `fund_std` decimal(9,4) DEFAULT NULL,
  `z` decimal(9,4) DEFAULT NULL,
  PRIMARY KEY (`ticker`,`deal_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8


CREATE TABLE `dividend` (
  `ticker` char(6) NOT NULL DEFAULT '',
  `declare_date` date DEFAULT NULL,
  `payable_date` date NOT NULL DEFAULT '0000-00-00',
  `ex_date` date DEFAULT NULL,
  `amount` decimal(9,5) DEFAULT NULL,
  `income` decimal(9,4) DEFAULT NULL,
  `long_gain` decimal(9,4) DEFAULT NULL,
  `shart_gain` decimal(9,4) DEFAULT NULL,
  `roc` char(1) DEFAULT NULL,
  `cnt` int(2) DEFAULT NULL,
  `bigger_three_year` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `dividend_original` (
  `ticker` CHAR(6) NOT NULL DEFAULT '',
  `declare_date` DATE DEFAULT NULL,
  `payable_date` DATE NOT NULL DEFAULT '0000-00-00',
  `ex_date` DATE DEFAULT NULL,
  `amount` DECIMAL(9,5) DEFAULT NULL
) ENGINE=INNODB DEFAULT CHARSET=utf8

CREATE TABLE `fund_dividend` (
  `deal_date` date NOT NULL DEFAULT '0000-00-00',
  `ticker` char(6) NOT NULL DEFAULT '',
  `price` decimal(9,4) DEFAULT NULL,
  `nat` decimal(9,4) DEFAULT NULL,
  `discount` decimal(9,4) DEFAULT NULL,
  `ava` decimal(9,4) DEFAULT NULL,
  `standard` decimal(9,4) DEFAULT NULL,
  `z` decimal(9,4) DEFAULT NULL,
  `amount` decimal(9,4) DEFAULT NULL,
  `cnt` tinyint(1) DEFAULT NULL,
  `3years` tinyint(1) DEFAULT NULL,
  `3year_sum_amount` decimal(9,4) DEFAULT NULL,
  `is_real_dividend_date` tinyint(1) DEFAULT NULL,
  `3years_profit` decimal(9,4) DEFAULT NULL,
  PRIMARY KEY (`deal_date`,`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `fund_history` (
  `ticker` char(6) NOT NULL DEFAULT '',
  `deal_date` date NOT NULL DEFAULT '0000-00-00',
  `price` decimal(9,2) DEFAULT NULL,
  `NAT` decimal(9,2) DEFAULT NULL,
  `discount` decimal(9,4) DEFAULT NULL,
  `ava` decimal(9,4) DEFAULT NULL,
  `fund_std` decimal(9,4) DEFAULT NULL,
  `z` decimal(9,4) DEFAULT NULL,
  PRIMARY KEY (`ticker`,`deal_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8