'''
https://www.joinquant.com/view/community/detail/47f4a99841f652ec3406af62d4c3a191

使用JQDATA获取数据并本地化（Mysql储存）.py
'''


""" 
本示例主要股票信息，日线行情信息与市值数据信息的本地化
日线行情信息进行了分表，将res按股票代码切分为5个res分别存入5张表中
首先 创建Mysql数据
"""

--创建数据库 jqdata
CREATE SCHEMA `jqdata` COMMENT CHARACTER SET utf8mb4 ;

--创建用户 jqdata
CREATE USER 'jqdata'@'%' IDENTIFIED BY 'jqdata';

grant all privileges on jqdata.* to 'jqdata'@'%' IDENTIFIED BY 'jqdata'

flush privileges;

--股票信息表
CREATE TABLE `jqdata`.`t_all_securities` (
  `security` VARCHAR(20) NOT NULL COMMENT '股票代码',
  `display_name` VARCHAR(20) NULL COMMENT '中文名称',
  `name` VARCHAR(50) NULL COMMENT '缩写简称',
  `start_date` DATE NULL COMMENT '上市日期',
  `end_date` DATE NULL COMMENT '退市日期，如果没有退市则为2200-01-01',
  PRIMARY KEY (`security`))
COMMENT = '所有股票信息';


--日线行情数据：
--分表规则：股票代码模5取余x
--表名 t_kline_day_x
--索引 股票代码，日期
CREATE TABLE `jqdata`.`t_kline_day_0` (
  `security` VARCHAR(20) NOT NULL COMMENT '股票代码',
  `kday` DATE NOT NULL COMMENT '日期',
  `open` DECIMAL(10,2) NULL COMMENT '时间段开始时价格',
  `close` DECIMAL(10,2) NULL COMMENT '时间段结束时价格',
  `low` DECIMAL(10,2) NULL COMMENT '最低价',
  `high` DECIMAL(10,2) NULL COMMENT '最高价',
  `volume` BIGINT NULL COMMENT '成交的股票数量',
  `money` DECIMAL(20,2) NULL COMMENT '成交的金额',
  `factor` DECIMAL(15,8) NULL COMMENT '前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如: close/factor',
  `high_limit` DECIMAL(10,2) NULL COMMENT '涨停价',
  `low_limit` DECIMAL(10,2) NULL COMMENT '跌停价',
  `avg` DECIMAL(10,2) NULL COMMENT '这段时间的平均价, 等于money/volume',
  `pre_close` DECIMAL(10,2) NULL COMMENT '前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟这是前一分钟的结束价格',
  `paused` TINYINT NULL COMMENT '布尔值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0',
  PRIMARY KEY (`security`, `kday`))
COMMENT = '日线行情数据——股票代码模5余0';

CREATE TABLE `jqdata`.`t_kline_day_1` (
  `security` VARCHAR(20) NOT NULL COMMENT '股票代码',
  `kday` DATE NOT NULL COMMENT '日期',
  `open` DECIMAL(10,2) NULL COMMENT '时间段开始时价格',
  `close` DECIMAL(10,2) NULL COMMENT '时间段结束时价格',
  `low` DECIMAL(10,2) NULL COMMENT '最低价',
  `high` DECIMAL(10,2) NULL COMMENT '最高价',
  `volume` BIGINT NULL COMMENT '成交的股票数量',
  `money` DECIMAL(20,2) NULL COMMENT '成交的金额',
  `factor` DECIMAL(15,8) NULL COMMENT '前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如: close/factor',
  `high_limit` DECIMAL(10,2) NULL COMMENT '涨停价',
  `low_limit` DECIMAL(10,2) NULL COMMENT '跌停价',
  `avg` DECIMAL(10,2) NULL COMMENT '这段时间的平均价, 等于money/volume',
  `pre_close` DECIMAL(10,2) NULL COMMENT '前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟这是前一分钟的结束价格',
  `paused` TINYINT NULL COMMENT '布尔值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0',
  PRIMARY KEY (`security`, `kday`))
COMMENT = '日线行情数据——股票代码模5余1';

CREATE TABLE `jqdata`.`t_kline_day_2` (
  `security` VARCHAR(20) NOT NULL COMMENT '股票代码',
  `kday` DATE NOT NULL COMMENT '日期',
  `open` DECIMAL(10,2) NULL COMMENT '时间段开始时价格',
  `close` DECIMAL(10,2) NULL COMMENT '时间段结束时价格',
  `low` DECIMAL(10,2) NULL COMMENT '最低价',
  `high` DECIMAL(10,2) NULL COMMENT '最高价',
  `volume` BIGINT NULL COMMENT '成交的股票数量',
  `money` DECIMAL(20,2) NULL COMMENT '成交的金额',
  `factor` DECIMAL(15,8) NULL COMMENT '前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如: close/factor',
  `high_limit` DECIMAL(10,2) NULL COMMENT '涨停价',
  `low_limit` DECIMAL(10,2) NULL COMMENT '跌停价',
  `avg` DECIMAL(10,2) NULL COMMENT '这段时间的平均价, 等于money/volume',
  `pre_close` DECIMAL(10,2) NULL COMMENT '前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟这是前一分钟的结束价格',
  `paused` TINYINT NULL COMMENT '布尔值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0',
  PRIMARY KEY (`security`, `kday`))
COMMENT = '日线行情数据——股票代码模5余2';

CREATE TABLE `jqdata`.`t_kline_day_3` (
  `security` VARCHAR(20) NOT NULL COMMENT '股票代码',
  `kday` DATE NOT NULL COMMENT '日期',
  `open` DECIMAL(10,2) NULL COMMENT '时间段开始时价格',
  `close` DECIMAL(10,2) NULL COMMENT '时间段结束时价格',
  `low` DECIMAL(10,2) NULL COMMENT '最低价',
  `high` DECIMAL(10,2) NULL COMMENT '最高价',
  `volume` BIGINT NULL COMMENT '成交的股票数量',
  `money` DECIMAL(20,2) NULL COMMENT '成交的金额',
  `factor` DECIMAL(15,8) NULL COMMENT '前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如: close/factor',
  `high_limit` DECIMAL(10,2) NULL COMMENT '涨停价',
  `low_limit` DECIMAL(10,2) NULL COMMENT '跌停价',
  `avg` DECIMAL(10,2) NULL COMMENT '这段时间的平均价, 等于money/volume',
  `pre_close` DECIMAL(10,2) NULL COMMENT '前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟这是前一分钟的结束价格',
  `paused` TINYINT NULL COMMENT '布尔值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0',
  PRIMARY KEY (`security`, `kday`))
COMMENT = '日线行情数据——股票代码模5余3';

CREATE TABLE `jqdata`.`t_kline_day_4` (
  `security` VARCHAR(20) NOT NULL COMMENT '股票代码',
  `kday` DATE NOT NULL COMMENT '日期',
  `open` DECIMAL(10,2) NULL COMMENT '时间段开始时价格',
  `close` DECIMAL(10,2) NULL COMMENT '时间段结束时价格',
  `low` DECIMAL(10,2) NULL COMMENT '最低价',
  `high` DECIMAL(10,2) NULL COMMENT '最高价',
  `volume` BIGINT NULL COMMENT '成交的股票数量',
  `money` DECIMAL(20,2) NULL COMMENT '成交的金额',
  `factor` DECIMAL(15,8) NULL COMMENT '前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如: close/factor',
  `high_limit` DECIMAL(10,2) NULL COMMENT '涨停价',
  `low_limit` DECIMAL(10,2) NULL COMMENT '跌停价',
  `avg` DECIMAL(10,2) NULL COMMENT '这段时间的平均价, 等于money/volume',
  `pre_close` DECIMAL(10,2) NULL COMMENT '前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟这是前一分钟的结束价格',
  `paused` TINYINT NULL COMMENT '布尔值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0',
  PRIMARY KEY (`security`, `kday`))
COMMENT = '日线行情数据——股票代码模5余4';


CREATE TABLE `jqdata`.`t_valuation_0` (
  `code` VARCHAR(20) NOT NULL COMMENT '股票代码  带后缀.XSHE/.XSHG',
  `day` DATE NOT NULL COMMENT '取数据的日期',
  `capitalization` DECIMAL(20,4) NULL COMMENT '总股本(万股)     公司已发行的普通股股份总数(包含A股，B股和H股的总股本)',
  `circulating_cap` DECIMAL(20,4) NULL COMMENT '流通股本(万股)     公司已发行的境内上市流通、以人民币兑换的股份总数(A股市场的流通股本)',
  `market_cap` DECIMAL(20,10) NULL COMMENT '总市值(亿元)     A股收盘价*已发行股票总股本（A股+B股+H股）',
  `circulating_market_cap` DECIMAL(20,10) NULL COMMENT '流通市值(亿元)     流通市值指在某特定时间内当时可交易的流通股股数乘以当时股价得出的流通股票总价值。     A股市场的收盘价*A股市场的流通股数',
  `turnover_ratio` DECIMAL(10,4) NULL COMMENT '换手率(%)     指在一定时间内市场中股票转手买卖的频率，是反映股票流通性强弱的指标之一。     换手率=[指定交易日成交量(手)100/截至该日股票的自由流通股本(股)]100%',
  `pe_ratio` DECIMAL(15,4) NULL COMMENT '市盈率(PE, TTM)     每股市价为每股收益的倍数，反映投资人对每元净利润所愿支付的价格，用来估计股票的投资报酬和风险     市盈率（PE，TTM）=（股票在指定交易日期的收盘价 * 当日人民币外汇挂牌价 * 截止当日公司总股本）/归属于母公司股东的净利润TTM。',
  `pe_ratio_lyr` DECIMAL(15,4) NULL COMMENT '以上一年度每股盈利计算的静态市盈率. 股价/最近年度报告EPS     市盈率（PE）=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的净利润。',
  `pb_ratio` DECIMAL(15,4) NULL COMMENT '市净率(PB)     每股股价与每股净资产的比率     市净率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的权益。',
  `ps_ratio` DECIMAL(15,4) NULL COMMENT '市销率(PS, TTM)     市销率为股票价格与每股销售收入之比，市销率越小，通常被认为投资价值越高。     市销率TTM=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/营业总收入TTM',
  `pcf_ratio` DECIMAL(15,4) NULL COMMENT '市现率(PCF, 现金净流量TTM)     每股市价为每股现金净流量的倍数     市现率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/现金及现金等价物净增加额TTM',
  PRIMARY KEY (`code`, `day`))
COMMENT = '市值数据-股票代码模5余0';

CREATE TABLE `jqdata`.`t_valuation_1` (
  `code` VARCHAR(20) NOT NULL COMMENT '股票代码  带后缀.XSHE/.XSHG',
  `day` DATE NOT NULL COMMENT '取数据的日期',
  `capitalization` DECIMAL(20,4) NULL COMMENT '总股本(万股)     公司已发行的普通股股份总数(包含A股，B股和H股的总股本)',
  `circulating_cap` DECIMAL(20,4) NULL COMMENT '流通股本(万股)     公司已发行的境内上市流通、以人民币兑换的股份总数(A股市场的流通股本)',
  `market_cap` DECIMAL(20,10) NULL COMMENT '总市值(亿元)     A股收盘价*已发行股票总股本（A股+B股+H股）',
  `circulating_market_cap` DECIMAL(20,10) NULL COMMENT '流通市值(亿元)     流通市值指在某特定时间内当时可交易的流通股股数乘以当时股价得出的流通股票总价值。     A股市场的收盘价*A股市场的流通股数',
  `turnover_ratio` DECIMAL(10,4) NULL COMMENT '换手率(%)     指在一定时间内市场中股票转手买卖的频率，是反映股票流通性强弱的指标之一。     换手率=[指定交易日成交量(手)100/截至该日股票的自由流通股本(股)]100%',
  `pe_ratio` DECIMAL(15,4) NULL COMMENT '市盈率(PE, TTM)     每股市价为每股收益的倍数，反映投资人对每元净利润所愿支付的价格，用来估计股票的投资报酬和风险     市盈率（PE，TTM）=（股票在指定交易日期的收盘价 * 当日人民币外汇挂牌价 * 截止当日公司总股本）/归属于母公司股东的净利润TTM。',
  `pe_ratio_lyr` DECIMAL(15,4) NULL COMMENT '以上一年度每股盈利计算的静态市盈率. 股价/最近年度报告EPS     市盈率（PE）=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的净利润。',
  `pb_ratio` DECIMAL(15,4) NULL COMMENT '市净率(PB)     每股股价与每股净资产的比率     市净率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的权益。',
  `ps_ratio` DECIMAL(15,4) NULL COMMENT '市销率(PS, TTM)     市销率为股票价格与每股销售收入之比，市销率越小，通常被认为投资价值越高。     市销率TTM=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/营业总收入TTM',
  `pcf_ratio` DECIMAL(15,4) NULL COMMENT '市现率(PCF, 现金净流量TTM)     每股市价为每股现金净流量的倍数     市现率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/现金及现金等价物净增加额TTM',
  PRIMARY KEY (`code`, `day`))
COMMENT = '市值数据-股票代码模5余1';

CREATE TABLE `jqdata`.`t_valuation_2` (
  `code` VARCHAR(20) NOT NULL COMMENT '股票代码  带后缀.XSHE/.XSHG',
  `day` DATE NOT NULL COMMENT '取数据的日期',
  `capitalization` DECIMAL(20,4) NULL COMMENT '总股本(万股)     公司已发行的普通股股份总数(包含A股，B股和H股的总股本)',
  `circulating_cap` DECIMAL(20,4) NULL COMMENT '流通股本(万股)     公司已发行的境内上市流通、以人民币兑换的股份总数(A股市场的流通股本)',
  `market_cap` DECIMAL(20,10) NULL COMMENT '总市值(亿元)     A股收盘价*已发行股票总股本（A股+B股+H股）',
  `circulating_market_cap` DECIMAL(20,10) NULL COMMENT '流通市值(亿元)     流通市值指在某特定时间内当时可交易的流通股股数乘以当时股价得出的流通股票总价值。     A股市场的收盘价*A股市场的流通股数',
  `turnover_ratio` DECIMAL(10,4) NULL COMMENT '换手率(%)     指在一定时间内市场中股票转手买卖的频率，是反映股票流通性强弱的指标之一。     换手率=[指定交易日成交量(手)100/截至该日股票的自由流通股本(股)]100%',
  `pe_ratio` DECIMAL(15,4) NULL COMMENT '市盈率(PE, TTM)     每股市价为每股收益的倍数，反映投资人对每元净利润所愿支付的价格，用来估计股票的投资报酬和风险     市盈率（PE，TTM）=（股票在指定交易日期的收盘价 * 当日人民币外汇挂牌价 * 截止当日公司总股本）/归属于母公司股东的净利润TTM。',
  `pe_ratio_lyr` DECIMAL(15,4) NULL COMMENT '以上一年度每股盈利计算的静态市盈率. 股价/最近年度报告EPS     市盈率（PE）=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的净利润。',
  `pb_ratio` DECIMAL(15,4) NULL COMMENT '市净率(PB)     每股股价与每股净资产的比率     市净率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的权益。',
  `ps_ratio` DECIMAL(15,4) NULL COMMENT '市销率(PS, TTM)     市销率为股票价格与每股销售收入之比，市销率越小，通常被认为投资价值越高。     市销率TTM=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/营业总收入TTM',
  `pcf_ratio` DECIMAL(15,4) NULL COMMENT '市现率(PCF, 现金净流量TTM)     每股市价为每股现金净流量的倍数     市现率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/现金及现金等价物净增加额TTM',
  PRIMARY KEY (`code`, `day`))
COMMENT = '市值数据-股票代码模5余2';

CREATE TABLE `jqdata`.`t_valuation_3` (
  `code` VARCHAR(20) NOT NULL COMMENT '股票代码  带后缀.XSHE/.XSHG',
  `day` DATE NOT NULL COMMENT '取数据的日期',
  `capitalization` DECIMAL(20,4) NULL COMMENT '总股本(万股)     公司已发行的普通股股份总数(包含A股，B股和H股的总股本)',
  `circulating_cap` DECIMAL(20,4) NULL COMMENT '流通股本(万股)     公司已发行的境内上市流通、以人民币兑换的股份总数(A股市场的流通股本)',
  `market_cap` DECIMAL(20,10) NULL COMMENT '总市值(亿元)     A股收盘价*已发行股票总股本（A股+B股+H股）',
  `circulating_market_cap` DECIMAL(20,10) NULL COMMENT '流通市值(亿元)     流通市值指在某特定时间内当时可交易的流通股股数乘以当时股价得出的流通股票总价值。     A股市场的收盘价*A股市场的流通股数',
  `turnover_ratio` DECIMAL(10,4) NULL COMMENT '换手率(%)     指在一定时间内市场中股票转手买卖的频率，是反映股票流通性强弱的指标之一。     换手率=[指定交易日成交量(手)100/截至该日股票的自由流通股本(股)]100%',
  `pe_ratio` DECIMAL(15,4) NULL COMMENT '市盈率(PE, TTM)     每股市价为每股收益的倍数，反映投资人对每元净利润所愿支付的价格，用来估计股票的投资报酬和风险     市盈率（PE，TTM）=（股票在指定交易日期的收盘价 * 当日人民币外汇挂牌价 * 截止当日公司总股本）/归属于母公司股东的净利润TTM。',
  `pe_ratio_lyr` DECIMAL(15,4) NULL COMMENT '以上一年度每股盈利计算的静态市盈率. 股价/最近年度报告EPS     市盈率（PE）=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的净利润。',
  `pb_ratio` DECIMAL(15,4) NULL COMMENT '市净率(PB)     每股股价与每股净资产的比率     市净率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的权益。',
  `ps_ratio` DECIMAL(15,4) NULL COMMENT '市销率(PS, TTM)     市销率为股票价格与每股销售收入之比，市销率越小，通常被认为投资价值越高。     市销率TTM=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/营业总收入TTM',
  `pcf_ratio` DECIMAL(15,4) NULL COMMENT '市现率(PCF, 现金净流量TTM)     每股市价为每股现金净流量的倍数     市现率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/现金及现金等价物净增加额TTM',
  PRIMARY KEY (`code`, `day`))
COMMENT = '市值数据-股票代码模5余3';

CREATE TABLE `jqdata`.`t_valuation_4` (
  `code` VARCHAR(20) NOT NULL COMMENT '股票代码  带后缀.XSHE/.XSHG',
  `day` DATE NOT NULL COMMENT '取数据的日期',
  `capitalization` DECIMAL(20,4) NULL COMMENT '总股本(万股)     公司已发行的普通股股份总数(包含A股，B股和H股的总股本)',
  `circulating_cap` DECIMAL(20,4) NULL COMMENT '流通股本(万股)     公司已发行的境内上市流通、以人民币兑换的股份总数(A股市场的流通股本)',
  `market_cap` DECIMAL(20,10) NULL COMMENT '总市值(亿元)     A股收盘价*已发行股票总股本（A股+B股+H股）',
  `circulating_market_cap` DECIMAL(20,10) NULL COMMENT '流通市值(亿元)     流通市值指在某特定时间内当时可交易的流通股股数乘以当时股价得出的流通股票总价值。     A股市场的收盘价*A股市场的流通股数',
  `turnover_ratio` DECIMAL(10,4) NULL COMMENT '换手率(%)     指在一定时间内市场中股票转手买卖的频率，是反映股票流通性强弱的指标之一。     换手率=[指定交易日成交量(手)100/截至该日股票的自由流通股本(股)]100%',
  `pe_ratio` DECIMAL(15,4) NULL COMMENT '市盈率(PE, TTM)     每股市价为每股收益的倍数，反映投资人对每元净利润所愿支付的价格，用来估计股票的投资报酬和风险     市盈率（PE，TTM）=（股票在指定交易日期的收盘价 * 当日人民币外汇挂牌价 * 截止当日公司总股本）/归属于母公司股东的净利润TTM。',
  `pe_ratio_lyr` DECIMAL(15,4) NULL COMMENT '以上一年度每股盈利计算的静态市盈率. 股价/最近年度报告EPS     市盈率（PE）=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的净利润。',
  `pb_ratio` DECIMAL(15,4) NULL COMMENT '市净率(PB)     每股股价与每股净资产的比率     市净率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的权益。',
  `ps_ratio` DECIMAL(15,4) NULL COMMENT '市销率(PS, TTM)     市销率为股票价格与每股销售收入之比，市销率越小，通常被认为投资价值越高。     市销率TTM=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/营业总收入TTM',
  `pcf_ratio` DECIMAL(15,4) NULL COMMENT '市现率(PCF, 现金净流量TTM)     每股市价为每股现金净流量的倍数     市现率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/现金及现金等价物净增加额TTM',
  PRIMARY KEY (`code`, `day`))
COMMENT = '市值数据-股票代码模5余4';

# 获取股票信息
# -*- coding: UTF-8 -*-
import sys
import pandas as pd
import jqdatasdk as jq
import mysql.connector
import pymysql
from sqlalchemy import create_engine

def get_all_securities():
    '''获取全部股票信息'''
    res = jq.get_all_securities(types=['stock'], date=None)
    '''删除type字段'''
    res=res.drop(['type'], axis=1)

    '''清表'''
    mdbconn = mysql.connector.connect(user='jqdata', password='jqdata', database='jqdata', use_unicode=True)
    cursor = mdbconn.cursor()
    cursor.execute('truncate table t_all_securities')
    mdbconn.commit()
    cursor.close()
    print('truncate table t_all_securities success')
    '''DataFrame入库'''
    pymysql.install_as_MySQLdb()
    mysqlconnect = create_engine('mysql+mysqldb://jqdata:jqdata@localhost:3306/jqdata?charset=utf8')
    res.to_sql(name='t_all_securities',con=mysqlconnect,schema='jqdata',if_exists='append',index=True,index_label='security',chunksize=1000)
    print('all securities saved in t_all_securities success')
    return

def main():
    jq.auth('***', '***')
    get_all_securities()
    jq.logout()


if __name__ == "__main__":
    sys.exit(main())


# 获取日线行情信息
# -*- coding: UTF-8 -*-
#如不指定下载数据的时间段,则自动判断下载全量或补充增量,
#如指定下载数据时间段,则删除期间数据,重新下载全部股票的数据

import sys
import datetime
import pandas as pd
import jqdatasdk as jq
import mysql.connector
import pymysql
from sqlalchemy import create_engine


def get_one_price(security,startday,endday):
    '''获取单只股票的指定时间段的后复权日线数据'''
    res = jq.get_price(security, start_date=startday, end_date=endday, 
                      frequency='daily', fields=['open','close','high','low','volume','money','factor','high_limit','low_limit','avg','pre_close','paused'], 
                      skip_paused=False, fq='post', 
                      count=None)
    '''增加股票代码列'''
    res['security']=security
    print(res)

    '''表路由,计算表名'''
    tmod=int(security[:6]) % 5

    tablename='t_kline_day_'+str(tmod)
    print(tablename)


    '''清理老数据'''
    sql="delete from " + tablename + " where security = '" + security + "' and kday >='" + startday + "' and kday <='" + endday + "'"
    print(sql)    
    mdbconn = mysql.connector.connect(user='jqdata', password='jqdata', database='jqdata', use_unicode=True)
    cursor = mdbconn.cursor()
    cursor.execute(sql)
    mdbconn.commit()
    cursor.close()


    '''DataFrame入库'''
    pymysql.install_as_MySQLdb()
    mysqlconnect = create_engine('mysql+mysqldb://jqdata:jqdata@localhost:3306/jqdata?charset=utf8')
    res.to_sql(name=tablename,con=mysqlconnect,schema='jqdata',if_exists='append',index=True,index_label='kday',chunksize=1000)
    print("all " + security + "data saved in " + tablename + " success")
    return

def get_all_price(b='0',e='0'):
    '''遍历全部股票,获取日线数据'''
    '''从本地数据库里获取全部股票信息,代码,上市日期,退市日期'''
    mdbconn = mysql.connector.connect(user='jqdata', password='jqdata', database='jqdata', use_unicode=True)
    sql="select security , start_date, end_date from t_all_securities"


    securities=pd.read_sql(sql, mdbconn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)

    cursor = mdbconn.cursor()
    for i in range(0,len(securities)):
        security=securities.iloc[i]['security']
        #没有入参,表示自动运行,判断已经存在的数据,加载至今的,没有数据,加载全部数据.
        if b=='0':
            startday=securities.iloc[i]['start_date'].strftime('%Y-%m-%d')
            endday=securities.iloc[i]['end_date'].strftime('%Y-%m-%d')

            tmod=int(security[:6]) % 5

            tablename='t_kline_day_'+str(tmod)

            sql="select ifnull(max(kday),'0000-00-00') as kday from " + tablename + " where security = '" + security + "'"

            cursor.execute(sql)
            kday = cursor.fetchone()[0]

            today=datetime.datetime.now().strftime('%Y-%m-%d')

            #计算起始日期
            if kday == '0000-00-00':
                pass
            elif kday != '0000-00-00' and kday < today:
                delta=datetime.timedelta(days=1)
                kday=datetime.datetime.strptime(kday, '%Y-%m-%d')
                startday=(kday+delta).strftime('%Y-%m-%d')

            #计算结束日期
            if today < endday :
                endday=today

        else:
            #按指定日期运行
            startday=b
            endday=e


        get_one_price(security, startday, endday)

    cursor.close()
    return

def main(b='0',e='0'):
    jq.auth('***', '***')


    get_all_price(b,e)

    jq.logout()


if __name__ == "__main__":
    #sys.exit(main())
    sys.exit(main('2019-07-01','2019-07-02'))


# 获取市值数据信息
# -*- coding: UTF-8 -*-
#获取市值数据,入参是日期,每日获取全部数据

import sys
import datetime
import pandas as pd
import jqdatasdk as jq
from jqdatasdk import *
import mysql.connector
import pymysql
import sqlalchemy
from sqlalchemy import create_engine

def get_oneday_valuation(day):
    '''
    获取某一天的所有市值数据
    由于返回数量限制,分股票代码 0 3 6 开头三次取数据.
    如何分表呢,将res按股票代码切分为5个res,分别存入5张表中

    '''
    for x in ['0','3','6']:

        #get_fundamentals(query(valuation),date)
        qry = query(
            valuation
        ).filter(
            valuation.code.ilike (x+'__________')
        )
        res = get_fundamentals(qry, day)
        res=res.drop(['id'], axis=1)

        #增加模列
        res['mod']=res['code'].map(lambda x:int(x[:6])%5)

        res0=res.loc[res['mod']==0].drop(['mod'],axis=1)
        res1=res.loc[res['mod']==1].drop(['mod'],axis=1)
        res2=res.loc[res['mod']==2].drop(['mod'],axis=1)
        res3=res.loc[res['mod']==3].drop(['mod'],axis=1)
        res4=res.loc[res['mod']==4].drop(['mod'],axis=1)

        #'''清理老数据'''
        sql0="delete from t_valuation_0 where day = '" + day + "' and code like '"+x+"%'"
        sql1="delete from t_valuation_1 where day = '" + day + "' and code like '"+x+"%'"
        sql2="delete from t_valuation_2 where day = '" + day + "' and code like '"+x+"%'"
        sql3="delete from t_valuation_3 where day = '" + day + "' and code like '"+x+"%'"
        sql4="delete from t_valuation_4 where day = '" + day + "' and code like '"+x+"%'"
        mdbconn = mysql.connector.connect(user='jqdata', password='jqdata', database='jqdata', use_unicode=True)
        cursor = mdbconn.cursor()
        cursor.execute(sql0)
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.execute(sql3)
        cursor.execute(sql4)
        mdbconn.commit()
        cursor.close()


        '''DataFrame入库'''
        pymysql.install_as_MySQLdb()
        mysqlconnect = create_engine('mysql+mysqldb://jqdata:jqdata@localhost:3306/jqdata?charset=utf8')
        res0.to_sql(name='t_valuation_0',con=mysqlconnect,schema='jqdata',if_exists='append',index=False,chunksize=1000)
        res1.to_sql(name='t_valuation_1',con=mysqlconnect,schema='jqdata',if_exists='append',index=False,chunksize=1000)
        res2.to_sql(name='t_valuation_2',con=mysqlconnect,schema='jqdata',if_exists='append',index=False,chunksize=1000)
        res3.to_sql(name='t_valuation_3',con=mysqlconnect,schema='jqdata',if_exists='append',index=False,chunksize=1000)
        res4.to_sql(name='t_valuation_4',con=mysqlconnect,schema='jqdata',if_exists='append',index=False,chunksize=1000)
        print("all "+ day + "valuation data saved in t_valuation_[0,4] success")

    return

jq.auth('***', '***')
#如果传入非交易日,将会获取到之前的最近一个交易日数据,但是删除数据库历史数据删除传入日期,导致后续插入数据失败.
#使用交易日表 按日期循环取历史
get_oneday_valuation('2019-07-05')
jq.logout()





