create table o_retailers_trade_user
(
user_id int (9),
item_id int (9),
behavior_type int (1),
user_geohash varchar (14),
item_category int (5),
time varchar (13)
);

select * from o_retailers_trade_user limit 5;

-- 增加新列date_time、dates
alter table o_retailers_trade_user add column date_time datetime null;
alter table o_retailers_trade_user add column dates char(10) null;
-- dates 字段数据来自于date_time字段
update o_retailers_trade_user set date_time=str_to_date(time, '%Y-%m-%d %H');
update o_retailers_trade_user set dates=date(date_time);
-- %H可以表示0-23；⽽%h表示0-12

-- 去重预处理(使用临时表)
create table temp_trade like o_retailers_trade_user;
insert into temp_trade select distinct * from o_retailers_trade_user;

/*
 需求：uv、pv、浏览深度（按⽇）统计 
 pv：统计behavior_type=1的记录数，需要按⽇统计（分组）
 uv: 统计distinct user_id 的数量，需要按⽇统计（分组）
 浏览深度：pv/uv
*/

select
	dates,
	count( distinct user_id ) 'uv',
	count( if (behavior_type = 1, user_id, null) ) 'pv',
	count( if (behavior_type = 1, user_id, null) )/ count( distinct user_id ) 'pv/uv' 
from
	temp_trade 
group by
	dates;

-- 留存率 (按日) 统计
-- 活跃用户留存率 （某天100个用户活跃，过了3天后剩下50个人活跃）
-- 								活跃用户    过了一天           过了两天
-- 2019-12-28        100          90（90%）          80（80%）
-- 1）  获取到类似于上面的一个结果集
-- 2）  基础数据中所有的日都应该进行如上的计算
-- 
-- 计算到
-- user_id xxx用户 2019-12-28 （在数据集中找到2019-12-28日之后的数据）
--             dates       dates_1
-- xxx用户  2019-12-28  2019-12-29（有29日这条记录，那么该用户就是活跃的）
-- xxx用户  2019-12-28  2019-12-30
-- xxx用户  2019-12-28  (dates_1 - dates)相差几天 ---> 1天就应该在过了1天的统计中+1，或者进行count计数
-- 																										2天就应该在过了2天的统计中+1，或者进行count计数
-- 某日   活跃用户数    1日后留存率    2...7  15  30

-- 自关联的方式
select user_id, dates from temp_trade group by user_id, dates;

create view user_remain_view as
select
	a.dates, count(distinct b.user_id) as user_count,
	count(if (datediff(b.dates, a.dates)=1, b.user_id, null)) as remain1, -- 1日留存数计算
	count(if (datediff(b.dates, a.dates)=2, b.user_id, null)) as remain2, -- 2日留存数计算
	count(if (datediff(b.dates, a.dates)=3, b.user_id, null)) as remain3, -- 3日留存数计算
	count(if (datediff(b.dates, a.dates)=4, b.user_id, null)) as remain4, -- 4日留存数计算
	count(if (datediff(b.dates, a.dates)=5, b.user_id, null)) as remain5, -- 5日留存数计算
	count(if (datediff(b.dates, a.dates)=6, b.user_id, null)) as remain6, -- 6日留存数计算
	count(if (datediff(b.dates, a.dates)=7, b.user_id, null)) as remain7, -- 7日留存数计算
	count(if (datediff(b.dates, a.dates)=15, b.user_id, null)) as remain15, -- 15日留存数计算
	count(if (datediff(b.dates, a.dates)=30, b.user_id, null)) as remain30 -- 30日留存数计算
	
	
from
	( select user_id, dates from temp_trade group by user_id, dates ) a
	left join
	( select user_id, dates from temp_trade group by user_id, dates ) b 
on a.user_id = b.user_id
where b.dates>=a.dates
group by a.dates;

select dates, user_count,-- 一日留存率 cast转换函数  decimal
concat(cast((remain1/user_count)*100 as decimal(10,2)), '%') as 'day_1',
concat(cast((remain2/user_count)*100 as decimal(10,2)), '%') as 'day_2',
concat(cast((remain3/user_count)*100 as decimal(10,2)), '%') as 'day_3',
concat(cast((remain4/user_count)*100 as decimal(10,2)), '%') as 'day_4',
concat(cast((remain5/user_count)*100 as decimal(10,2)), '%') as 'day_5',
concat(cast((remain6/user_count)*100 as decimal(10,2)), '%') as 'day_6',
concat(cast((remain7/user_count)*100 as decimal(10,2)), '%') as 'day_7',
concat(cast((remain15/user_count)*100 as decimal(10,2)), '%') as 'day_15',
concat(cast((remain30/user_count)*100 as decimal(10,2)), '%') as 'day_30'
from user_remain_view;

-- R 指标分析，根据每个用户最近一次购买时间，给出相应的分数
drop view if exists user_recency_view;
create view user_recency_view as
-- 获取每个用户的最近购买时间
select
	user_id,
	max( dates ) as recent_buy_time
from
	temp_trade 
where
	behavior_type = 2 
group by
	user_id;

-- 计算每个用户最近购买时间距离2019-12-18相差几天，根据相差的天数给予一定的分数
create view r_level as 
select user_id, recent_buy_time, datediff('2019-12-18', recent_buy_time), 
-- <=2 5分， <=4 4分， <=6 3分， <=8 2分， 其他 1分
(
case
when datediff('2019-12-18', recent_buy_time)<=2 then 5
when datediff('2019-12-18', recent_buy_time)<=4 then 4
when datediff('2019-12-18', recent_buy_time)<=6 then 3
when datediff('2019-12-18', recent_buy_time)<=8 then 2
else 1 end
) as r_value
from user_recency_view;

-- F 指标计算，求出每一个用户消费次数（购买次数），拿到具体消费次数之后对消费情况评分
-- 求出每个用户消费次数（购买次数）
create view user_buy_fre_view as
select
	user_id,
	count( user_id ) as buy_frequency 
from
	temp_trade 
where
	behavior_type = 2 
group by
	user_id;

-- 评分， 购买次数 <=2 1分， <=4 2分， <=6 3分， <=8 4分， 其他 5分
create view f_level as 
select user_id, buy_frequency,
(
case
when buy_frequency <=2 then 1
when buy_frequency <=4 then 2
when buy_frequency <=6 then 3
when buy_frequency <=8 then 4
else 5 end
) as f_value
from user_buy_fre_view;

-- r均值
select avg(r_value) as 'r_avg' from r_level; -- 2.7939
-- f均值
select avg(f_value) as 'f_avg' from f_level; -- 2.2606


-- 重要价值客户：指最近一次消费较近且消费频率较高的客户；
-- 重要唤回客户：指最近一次消费较远且消费频率较高的客户；
-- 重要深耕客户：指最近一次消费较近且消费频率较低的客户；
-- 重要挽留客户：指最近一次消费较远且消费频率较低的客户；

-- 拿到每个人的r值和f值（两表关联 r_level f_level）, 与均值对比

select
	r.user_id,
	r.r_value,
	f.f_value, 
(
case 
when r.r_value>2.7939 and f.f_value>2.2606 then '重要高价值客户'
when r.r_value<2.7939 and f.f_value>2.2606 then '重要唤回客户'
when r.r_value<2.7939 and f.f_value<2.2606 then '重要深耕客户'
when r.r_value>2.7939 and f.f_value<2.2606 then '重要挽留客户'
end
) '客户分层'
from
	r_level r,
	f_level f 
where
	r.user_id = f.user_id;

-- 商品的点击量 收藏量 加购量 购买次数 购买转化
select
	item_id,
	sum(case when behavior_type = 1 then 1 else 0 end) as 'pv', -- 点击量计算
	sum(case when behavior_type = 4 then 1 else 0 end) as 'fav', -- 收藏量计算
	sum(case when behavior_type = 3 then 1 else 0 end) as 'cart', -- 加购量计算
	sum(case when behavior_type = 2 then 1 else 0 end) as 'bv', -- 购买次数计算
	count(distinct case when behavior_type=2 then user_id else null end) / count(distinct user_id) as 'buy_rate'
from
	temp_trade 
group by
	item_id;


-- 对应品类的点击量 收藏量 加购量 购买次数 购买转化
select
	item_category,
	sum(case when behavior_type = 1 then 1 else 0 end) as 'pv', -- 点击量计算
	sum(case when behavior_type = 4 then 1 else 0 end) as 'fav', -- 收藏量计算
	sum(case when behavior_type = 3 then 1 else 0 end) as 'cart', -- 加购量计算
	sum(case when behavior_type = 2 then 1 else 0 end) as 'bv', -- 购买次数计算
	count(distinct case when behavior_type=2 then user_id else null end) / count(distinct user_id) as 'buy_rate'
from
	temp_trade 
group by
	item_category;
	
-- 行为路径分析
-- 核心：拼接行为路径
-- user_id item_id behavior_type 1-3-4-1-2 (把多个行为并列摆放，才能使用concat函数进行拼接)
-- bxy      a           1
--                      3
-- 										  4
-- 										  1
-- 										  2（偏移分析函数  lag over  lead over   -- 窗口函数， 分组（用户+商品）， 排序（升序））

-- 用户行为拼接准备
drop view path_base_view;
create view path_base_view as
select a.* from 
(
select
	user_id,
	item_id,
	behavior_type,
	lag( behavior_type, 4 ) over ( partition by user_id, item_id order by date_time ) lag_4,
  lag( behavior_type, 3 ) over ( partition by user_id, item_id order by date_time ) lag_3, 
  lag( behavior_type, 2 ) over ( partition by user_id, item_id order by date_time ) lag_2, 
  lag( behavior_type, 1 ) over ( partition by user_id, item_id order by date_time ) lag_1,
	rank() over(partition by user_id, item_id order by date_time desc) rank_number
from
	temp_trade
) a where a.rank_number=1 and a.behavior_type=2;

-- 拼接行为路径
select concat(ifnull(lag_4, '空'), '-', ifnull(lag_3, '空'), '-', ifnull(lag_2, '空'), '-', ifnull(lag_1, '空'), '-', behavior_type) from path_base_view;
-- 针对行为路径进行统计
select concat(ifnull(lag_4, '空'), '-', ifnull(lag_3, '空'), '-', ifnull(lag_2, '空'), '-', ifnull(lag_1, '空'), '-', behavior_type), count(distinct user_id) from path_base_view group by concat(ifnull(lag_4, '空'), '-', ifnull(lag_3, '空'), '-', ifnull(lag_2, '空'), '-', ifnull(lag_1, '空'), '-', behavior_type);

