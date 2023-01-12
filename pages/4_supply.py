import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Supply",
    layout= "wide",
    page_icon="💰",
)
st.title("💰Supply")
st.write("""
Due to the large amount of data, the search may take some time. Please wait...
""")
st.sidebar.success("💰Supply")

def querying_pagination(query_string):
    sdk = ShroomDK('8c37dc3a-fcf4-42a1-a860-337fa9931a2a')
    result_list = []
    for i in range(1,11): 
        data=sdk.query(query_string,page_size=100000,page_number=i)
        if data.run_stats.record_count == 0:  
            break
        else:
            result_list.append(data.records)
  
    result_df=pd.DataFrame()
    for idx, each_list in enumerate(result_list):
        if idx == 0:
            result_df=pd.json_normalize(each_list)
        else:
            result_df=pd.concat([result_df, pd.json_normalize(each_list)])

    return result_df
#total supply and circulating supply
df_query="""
with table1 as(select 
receiver,
sum(amount)/pow(10,4) as "volume(receive)"
from terra.core.ez_transfers
WHERE CURRENCY='uluna'
group by 1
),table2 as(select 
sender,
sum(amount)/pow(10,4) as "volume(sent)"
from terra.core.ez_transfers
WHERE CURRENCY='uluna'
group by 1
  ), total_supply as(select 
sum("volume(receive)") as tot_supply
from table1 a left join table2 b on a.receiver=b.sender
where "volume(sent)" is null
  ), circulating_supply as(select
date_trunc('day',BLOCK_TIMESTAMP) as date,
sum(case when from_currency='uluna' then from_amount/pow(10,6) else null end) - 
sum(case when to_currency='uluna' then from_amount/pow(10,6) else null end) as "vol",
sum("vol") over (order by date) as cir_supply
from terra.core.ez_swaps
group by 1
  )
select 
tot_supply,
cir_supply,
cir_supply * 100 / tot_supply as ratio_supply
from total_supply join circulating_supply
where date = current_date-1
"""
df = querying_pagination(df_query)

#daily supply and circulating supply
df1_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ), table1 as(select 
receiver,
sum(amount)/pow(10,4) as "volume(receive)"
from terra.core.ez_transfers
WHERE CURRENCY='uluna'
group by 1
),table2 as(select 
sender,
sum(amount)/pow(10,4) as "volume(sent)"
from terra.core.ez_transfers
WHERE CURRENCY='uluna'
group by 1
  ), total_supply as(select
sum("volume(receive)") as tot_supply
from table1 a left join table2 b on a.receiver=b.sender
where "volume(sent)" is null
  ), circulating_supply as(select
BLOCK_TIMESTAMP::date as date,
sum(case when from_currency='uluna' then from_amount/pow(10,6) else null end) - 
sum(case when to_currency='uluna' then from_amount/pow(10,6) else null end) as "vol",
sum("vol") over (order by date) as cir_supply
from terra.core.ez_swaps
group by 1
  )
select 
b.date,
price,
cir_supply,
cir_supply * 100 / tot_supply as ratio_supply
from total_supply a join circulating_supply b join luna_price c on b.date=c.date
where b.date > '2022-8-11'
"""
df1 = querying_pagination(df1_query)

#daily staking
df2_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ), main as(select
BLOCK_TIMESTAMP::date as date,
action as "Action",
count(TX_ID) as "action count",
count(DISTINCT DELEGATOR_ADDRESS) as "user count",
sum(amount) as "action volume"
from terra.core.ez_staking
where TX_SUCCEEDED = TRUE
group by 1,2)
select
a.date,
"Action",
price,
"action count",
"user count",
"action volume",
sum("action count") over (order by a.date) as "cumulative count of action",
sum("user count") over (order by a.date) as "cumulative count of user",
sum("action volume") over (order by a.date) as "cumulative action volume"
from main a join luna_price b on a.date=b.date
group by 1,2,3,4,5,6
"""
df2 = querying_pagination(df2_query)

#total staking
df3_query="""
select
action,
count(TX_ID) as action_count,
count(DISTINCT DELEGATOR_ADDRESS) as user_count,
sum(amount) as action_volume
from terra.core.ez_staking
where TX_SUCCEEDED = TRUE
group by 1
"""
df3 = querying_pagination(df3_query)

#user categorize by count of stake
df4_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ), main as(select
DISTINCT DELEGATOR_ADDRESS as user,
action,
count(TX_ID) as "action count",
sum(amount) as "action volume"
from terra.core.ez_staking
where TX_SUCCEEDED = TRUE
group by 1,2)
select
case 
when "action count" = 1 then 'just one time'
when "action count" between 2 and 5 then '2 - 5 time'
when "action count" between 6 and 10 then '6 - 10 time'
when "action count" between 11 and 20 then '11 - 20 time'
when "action count" between 21 and 50 then '21 - 50 time'
when "action count" between 51 and 100 then '51 - 100 time'
when "action count" between 101 and 200 then '101 - 200 time'
when "action count" between 201 and 500 then '201 - 500 time'
when "action count" between 501 and 1000 then '501 - 1000 time'
else 'more than 1000 time'
end as action_count,
Action,
count(user) as count_users
from main
group by 1,2
"""
df4 = querying_pagination(df4_query)

#user categorize by volume of stake
df5_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ), main as(select
DISTINCT DELEGATOR_ADDRESS as user,
action,
count(TX_ID) as "action count",
sum(amount) as "action volume"
from terra.core.ez_staking
where TX_SUCCEEDED = TRUE
group by 1,2)
select
case 
when "action volume" < 100 then 'less than 100 USD'
when "action volume" between 100 and 499.99 then '100 - 500 USD'
when "action volume" between 500 and 999.99 then '500 - 1 K USD'
when "action volume" between 10000 and 2499.99 then '1 K - 2.5 K USD'
when "action volume" between 2500 and 4999.99 then '2.5 K - 5 K USD'
when "action volume" between 5000 and 9999.99 then '5 K - 10 K USD'
when "action volume" between 10000 and 19999.99 then '10 K - 20 K USD'
when "action volume" between 20000 and 49999.99 then '20 K - 50 K USD'
when "action volume" between 50000 and 999999.99 then '50 K - 100 K USD'
else 'more than 100 K USD'
end as action_volume,
action,
count(user) as count_users
from main
group by 1,2
"""
df5 = querying_pagination(df5_query)

#top 10 by count
df6_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  )
select
DISTINCT DELEGATOR_ADDRESS as user,
action,
count(TX_ID) as action_count
from terra.core.ez_staking
where TX_SUCCEEDED = TRUE
group by 1,2
order by 3 desc
limit 10
"""
df6 = querying_pagination(df6_query)

#top 10 by volume
df7_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  )
select
DISTINCT DELEGATOR_ADDRESS as user,
action,
sum(amount) as volume
from terra.core.ez_staking
where TX_SUCCEEDED = TRUE
group by 1,2
order by 3 desc
limit 10
"""
df7 = querying_pagination(df7_query)

#daily reward
df8_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ),main as (select 
BLOCK_TIMESTAMP::date as date,
RECEIVER,
TX_ID,
sum(AMOUNT)/pow(10,6) as "staking reward"
from terra.core.ez_transfers
where MESSAGE_VALUE['@type'] ='/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward'
and CURRENCY='uluna'
and TX_SUCCEEDED = TRUE
group by 1,2,3)
select 
a.date,
count(DISTINCT tx_id) as count_reward,
count(DISTINCT RECEIVER) as count_user,
sum("staking reward") as reward_luna,
sum("staking reward"*price) as reward_usd
from main a join luna_price b on a.date=b.date
group by 1
"""
df8 = querying_pagination(df8_query)

#total reward
df9_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ),main as (select 
date_trunc('day',block_timestamp) as date,
RECEIVER,
TX_ID,
sum(AMOUNT)/pow(10,6) as "staking reward (Luna)"
from terra.core.ez_transfers
where MESSAGE_VALUE['@type'] ='/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward'
and CURRENCY='uluna'
and TX_SUCCEEDED = TRUE
group by 1,2,3)
select 
count(DISTINCT tx_id) as count_reward,
count(DISTINCT RECEIVER) as count_user,
sum("staking reward (Luna)") as reward_luna,
reward_luna/count_user as reward_luna_per_user,
sum("staking reward (Luna)"*price) as reward_usd,
reward_usd/count_user as reward_usd_per_user
from main a join luna_price b on a.date=b.date
"""
df9 = querying_pagination(df9_query)

#user categorize by count of reward transaction
df10_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ),main as (select 
date_trunc('day',block_timestamp) as date,
RECEIVER,
TX_ID,
sum(AMOUNT)/pow(10,6) as "staking reward (Luna)"
from terra.core.ez_transfers
where MESSAGE_VALUE['@type'] ='/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward'
and CURRENCY='uluna'
and TX_SUCCEEDED = TRUE
group by 1,2,3
  ), main1 as(select 
DISTINCT RECEIVER as user,
count(DISTINCT tx_id) as "count of rewards distributed",
sum("staking reward (Luna)"*price) as "total staking reward (USD)"
from main a join luna_price b on a.date=b.date
group by 1)
select
case 
when "count of rewards distributed" = 1 then 'just once rewarded'
when "count of rewards distributed" between 2 and 5 then '2 - 5 rewarded'
when "count of rewards distributed" between 6 and 10 then '6 - 10 rewarded'
when "count of rewards distributed" between 11 and 20 then '11 - 20 rewarded'
when "count of rewards distributed" between 21 and 50 then '21 - 50 rewarded'
when "count of rewards distributed" between 51 and 100 then '51 - 100 rewarded'
when "count of rewards distributed" between 101 and 200 then '101 - 200 rewarded'
when "count of rewards distributed" between 201 and 500 then '201 - 500 rewarded'
when "count of rewards distributed" between 501 and 1000 then '501 - 1000 rewarded'
else 'more than 1000 rewarded'
end as reward_count,
count(user) as count_users
from main1
group by 1
"""
df10 = querying_pagination(df10_query)

#user categorize by volume of reward transaction
df11_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ),main as (select 
date_trunc('day',block_timestamp) as date,
RECEIVER,
TX_ID,
sum(AMOUNT)/pow(10,6) as "staking reward (Luna)"
from terra.core.ez_transfers
where MESSAGE_VALUE['@type'] ='/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward'
and CURRENCY='uluna'
and TX_SUCCEEDED = TRUE
group by 1,2,3
  ), main1 as(select 
DISTINCT RECEIVER as user,
count(DISTINCT tx_id) as "count of rewards distributed",
sum("staking reward (Luna)"*price) as "total staking reward (USD)"
from main a join luna_price b on a.date=b.date
group by 1)
select
case 
when "total staking reward (USD)" < 100 then 'less than 100 USD'
when "total staking reward (USD)" between 100 and 499.99 then '100 - 500 USD'
when "total staking reward (USD)" between 500 and 999.99 then '500 - 1 K USD'
when "total staking reward (USD)" between 10000 and 2499.99 then '1 K - 2.5 K USD'
when "total staking reward (USD)" between 2500 and 4999.99 then '2.5 K - 5 K USD'
when "total staking reward (USD)" between 5000 and 9999.99 then '5 K - 10 K USD'
when "total staking reward (USD)" between 10000 and 19999.99 then '10 K - 20 K USD'
when "total staking reward (USD)" between 20000 and 49999.99 then '20 K - 50 K USD'
when "total staking reward (USD)" between 50000 and 999999.99 then '50 K - 100 K USD'
else 'more than 100 K USD'
end as staking_volume,
count(user) as count_users
from main1
group by 1
"""
df11 = querying_pagination(df11_query)

#top 10 user by count of reward transaction
df12_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ),main as (select 
date_trunc('day',block_timestamp) as date,
RECEIVER,
TX_ID,
sum(AMOUNT)/pow(10,6) as "staking reward (Luna)"
from terra.core.ez_transfers
where MESSAGE_VALUE['@type'] ='/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward'
and CURRENCY='uluna'
and TX_SUCCEEDED = TRUE
group by 1,2,3
  )
select 
DISTINCT RECEIVER as user,
count(DISTINCT tx_id) as count_reward
from main a join luna_price b on a.date=b.date
group by 1
order by 2 desc 
limit 10
"""
df12 = querying_pagination(df12_query)

#top 10 user by volume of reward transaction
df13_query="""
with luna_price as(select
date_trunc('day',RECORDED_HOUR) as date,
avg(CLOSE) as price 
from crosschain.core.fact_hourly_prices
where ID = 'terra-luna-2'
group by 1
  ),main as (select 
date_trunc('day',block_timestamp) as date,
RECEIVER,
TX_ID,
sum(AMOUNT)/pow(10,6) as "staking reward (Luna)"
from terra.core.ez_transfers
where MESSAGE_VALUE['@type'] ='/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward'
and CURRENCY='uluna'
and TX_SUCCEEDED = TRUE
group by 1,2,3
  )
select 
DISTINCT RECEIVER as user,
sum("staking reward (Luna)"*price) as vol_reward
from main a join luna_price b on a.date=b.date
group by 1
order by 2 desc 
limit 10
"""
df13 = querying_pagination(df13_query)

#rich list
df14_query="""
with send as(select 
sender,
sum(amount) as "vol(send)"
from terra.core.ez_transfers
where CURRENCY='uluna'
group by 1
  ),receive as(select 
receiver,
sum(amount) as "vol(receive)"
from terra.core.ez_transfers
where CURRENCY='uluna'
group by 1
)
select  
sender as user, 
("vol(receive)"-"vol(send)")/pow(10,6) as "balance"
from send a join receive b on a.sender=b.receiver 
order by 2 desc
limit 100
"""
df14 = querying_pagination(df14_query)

#vest
df15_query="""
with table1 as(select 
address
from terra.core.dim_address_labels
where LABEL_TYPE='defi'
),table2 as(select 
BLOCK_TIMESTAMP::date as date1,
sum(AMOUNT)/1e6 as amount_in
from terra.core.ez_transfers
where CURRENCY='uluna'
and RECEIVER in (select address from table1) 
group by 1
  ),table3 as(select 
BLOCK_TIMESTAMP::date as date2,
sum(AMOUNT)/1e6 as amount_out
from terra.core.ez_transfers
where CURRENCY='uluna'
and sender in (select address from table1)
group by 1
),table4 as (select 
*
from table2 a join table3 b on a.date1=b.date2
  ),table5 as(select 
date1,
floor((sum(amount_in) over (order by date1))/1000) as vol_in
from table4
  ),table6 as(select 
date2,
floor((sum(amount_out) over (order by date2))/1000) as vol_out
from table4
  )
select
avg(datediff('day', date1, date2)) as ves
from table5 a join table6 b on a.vol_in=b.vol_out
and b.vol_out!=0
"""
df15 = querying_pagination(df15_query)

#vest
df151_query="""
with table1 as(select 
address
from terra.core.dim_address_labels
where LABEL_TYPE='defi'
),table2 as(select 
BLOCK_TIMESTAMP::date as date1,
sum(AMOUNT)/1e6 as amount_in
from terra.core.ez_transfers
where CURRENCY='uluna'
and RECEIVER in (select address from table1) 
group by 1
  ),table3 as(select 
BLOCK_TIMESTAMP::date as date2,
sum(AMOUNT)/1e6 as amount_out
from terra.core.ez_transfers
where CURRENCY='uluna'
and sender in (select address from table1)
group by 1
),table4 as (select 
*
from table2 a join table3 b on a.date1=b.date2
  )
select 
date1,
floor((sum(amount_in) over (order by date1))/1000) as vol_in,
floor((sum(amount_out) over (order by date2))/1000) as vol_out
from table4
"""
df151 = querying_pagination(df151_query)

#daily bridge
df16_query="""
select 
BLOCK_TIMESTAMP::date as date,
case 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'osmo' then 'osmo' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'kuji' then 'kujira'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'cosm' then 'cosmos'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'evmo' then 'evmos'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'stri' then 'STRI'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'axel' then 'axelar' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'grav' then 'GRAV' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'secr' then 'secret' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'terr' then 'terra'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'juno' then 'juno'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 3) = 'cre' then 'CRE'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 3) = 'sif' then 'SIF'
else null 
end as blockchain,
count(TX_ID) as bridge_count,
count(DISTINCT MESSAGE_VALUE['sender']) as bridger_count,
sum(AMOUNT/1e6) as bridge_volume
from terra.core.ez_transfers
where MESSAGE_TYPE='/ibc.applications.transfer.v1.MsgTransfer' 
and CURRENCY='uluna'
and blockchain is not null
group by 1,2
"""
df16 = querying_pagination(df16_query)

#tot bridge
df17_query="""
select 
case 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'osmo' then 'osmo' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'kuji' then 'kujira'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'cosm' then 'cosmos'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'evmo' then 'evmos'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'stri' then 'STRI'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'axel' then 'axelar' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'grav' then 'GRAV' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'secr' then 'secret' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'terr' then 'terra'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'juno' then 'juno'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 3) = 'cre' then 'CRE'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 3) = 'sif' then 'SIF'
else null 
end as blockchain,
count(TX_ID) as bridge_count,
count(DISTINCT MESSAGE_VALUE['sender']) as bridger_count,
sum(AMOUNT/1e6) as bridge_volume
from terra.core.ez_transfers
where MESSAGE_TYPE='/ibc.applications.transfer.v1.MsgTransfer' 
and CURRENCY='uluna'
and blockchain is not null
group by 1
"""
df17 = querying_pagination(df17_query)

#cat count bridge
df18_query="""
with main as(select 
DISTINCT MESSAGE_VALUE['sender'] as user,
count(TX_ID) as "bridge count",
sum(AMOUNT/1e6) as "bridge volume"
from terra.core.ez_transfers
where MESSAGE_TYPE='/ibc.applications.transfer.v1.MsgTransfer' 
and CURRENCY='uluna'
group by 1)
select
case 
when "bridge count" = 1 then 'bridge just one time'
when "bridge count" between 2 and 5 then 'bridge 2 - 5 time'
when "bridge count" between 6 and 10 then 'bridge 6 - 10 time'
when "bridge count" between 11 and 20 then 'bridge 11 - 20 time'
when "bridge count" between 21 and 50 then 'bridge 21 - 50 time'
when "bridge count" between 51 and 100 then 'bridge 51 - 100 time'
when "bridge count" between 101 and 200 then 'bridge 101 - 200 time'
when "bridge count" between 201 and 500 then 'bridge 201 - 500 time'
when "bridge count" between 501 and 1000 then 'bridge 501 - 1000 time'
else 'bridge more than 1000 time'
end as bridge_count,
count(user) as count_users
from main
group by 1
"""
df18 = querying_pagination(df18_query)

#cat vol bridge
df19_query="""
with main as(select 
DISTINCT MESSAGE_VALUE['sender'] as user,
count(TX_ID) as "bridge count",
sum(AMOUNT/1e6) as "bridge volume"
from terra.core.ez_transfers
where MESSAGE_TYPE='/ibc.applications.transfer.v1.MsgTransfer' 
and CURRENCY='uluna'
group by 1)
select
case 
when "bridge volume" < 100 then 'bridge less than 100 USD'
when "bridge volume" between 100 and 499.99 then 'bridge 100 - 500 USD'
when "bridge volume" between 500 and 999.99 then 'bridge 500 - 1 K USD'
when "bridge volume" between 10000 and 2499.99 then 'bridge 1 K - 2.5 K USD'
when "bridge volume" between 2500 and 4999.99 then 'bridge 2.5 K - 5 K USD'
when "bridge volume" between 5000 and 9999.99 then 'bridge 5 K - 10 K USD'
when "bridge volume" between 10000 and 19999.99 then 'bridge 10 K - 20 K USD'
when "bridge volume" between 20000 and 49999.99 then 'bridge 20 K - 50 K USD'
when "bridge volume" between 50000 and 999999.99 then 'bridge 50 K - 100 K USD'
else 'bridge more than 100 K USD'
end as bridge_volume,
count(user) as count_users
from main
group by 1
"""
df19 = querying_pagination(df19_query)

#top count
df20_query="""
select 
DISTINCT MESSAGE_VALUE['sender'] as user,
case 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'osmo' then 'osmo' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'kuji' then 'kujira'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'cosm' then 'cosmos'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'evmo' then 'evmos'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'stri' then 'STRI'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'axel' then 'axelar' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'grav' then 'GRAV' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'secr' then 'secret' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'terr' then 'terra'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'juno' then 'juno'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 3) = 'cre' then 'CRE'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 3) = 'sif' then 'SIF'
else null 
end as blockchain,
count(TX_ID) as bridge_count,
sum(AMOUNT/1e6) as "bridge volume"
from terra.core.ez_transfers
where MESSAGE_TYPE='/ibc.applications.transfer.v1.MsgTransfer' 
and CURRENCY='uluna'
and blockchain is not null
group by 1,2
order by 3 desc 
limit 10
"""
df20 = querying_pagination(df20_query)

#top vol
df21_query="""
select 
DISTINCT MESSAGE_VALUE['sender'] as user,
case 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'osmo' then 'osmo' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'kuji' then 'kujira'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'cosm' then 'cosmos'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'evmo' then 'evmos'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'stri' then 'STRI'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'axel' then 'axelar' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'grav' then 'GRAV' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'secr' then 'secret' 
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'terr' then 'terra'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 4) = 'juno' then 'juno'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 3) = 'cre' then 'CRE'
when SUBSTR(MESSAGE_VALUE['receiver'], 0, 3) = 'sif' then 'SIF'
else null 
end as blockchain,
count(TX_ID) as "bridge count",
sum(AMOUNT/1e6) as bridge_volume
from terra.core.ez_transfers
where MESSAGE_TYPE='/ibc.applications.transfer.v1.MsgTransfer' 
and CURRENCY='uluna'
and blockchain is not null
group by 1,2
order by 4 desc 
limit 10
"""
df21 = querying_pagination(df21_query)

st.write("""
 # Total supply and total and daily circulating supply and his rate vs Luna price #
 
 """
)
cc1, cc2 , cc3= st.columns([1, 1,1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df["tot_supply"][0]),
    label="Total supply",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df["cir_supply"][0]),
    label="Circulation supply",
)
with cc3:
  st.metric(
    value="{0:,.0f}".format(df["ratio_supply"][0]),
    label="Circulation supply rate from total supply",
)
cc1, cc2 = st.columns([1, 1])
with cc1:
 st.caption('Daily circulation supply')
 st.line_chart(df1, x='date', y='cir_supply')
with cc2:
 st.caption('Daily circulation supply rate vs price')
 st.line_chart(df1, x='date', y=['price','ratio_supply'])

st.write("""
 # Total and daily staking activity #
 
 """
)
cc1, cc2 , cc3= st.columns([1, 1,1])
with cc1:
  st.caption('Total number of stake at each action')
  st.bar_chart(df3, x='action', y = 'action_count', width = 400, height = 400)

with cc2:
  st.caption('Total number of staker at each action')
  st.bar_chart(df3, x='action', y = 'user_count', width = 400, height = 400)

with cc3:
  st.caption('Total volume of stake at each action')
  st.bar_chart(df3, x='action', y = 'action_volume', width = 400, height = 400)

st.subheader('Daily number of stake at each action')
fig = px.bar(df2, x='date', y='action count',color='action', title='Daily number of stake at each action')
fig.update_layout(legend_title=None, legend_y=0.5)
st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.subheader('Daily number of staker at each action')
fig = px.bar(df2, x='date', y='user count',color='action', title='Daily number of staker at each action')
fig.update_layout(legend_title=None, legend_y=0.5)
st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.subheader('Daily volume of stake at each action')
fig = px.bar(df2, x='date', y='action volume',color='action', title='Daily volume of stake at each action')
fig.update_layout(legend_title=None, legend_y=0.5)
st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.write("""
 # staker categorize by count and volume of staking #
 In this charts you can see total user at each action (for action count and volume).
 """
)
cc1, cc2= st.columns([1, 1])
with cc1:
  fig = px.bar(df4, x='action_count', y='count_users',color='action', title='user categorize by count of stake at each action')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

with cc2:
  fig = px.pie(df4, values='count_users', names='action_count', title='user categorize by count of stake')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

cc1, cc2= st.columns([1, 1])
with cc1:
  fig = px.bar(df5, x='action_volume', y='count_users',color='action', title='user categorize by volume of stake at each action')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

with cc2:
  fig = px.pie(df5, values='count_users', names='action_volume', title='user categorize by volume of stake')
  fig.update_layout(legend_title=None, legend_y=0.5)
  fig.update_traces(textinfo='percent+label', textposition='inside')
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.write("""
 # Top 10 user by count and volume of staking #
 In this charts you can see top 10 users by count and volume of staking.
 """
)
cc1, cc2= st.columns([1, 1])
with cc1:
 fig = px.bar(df6, x='user', y='action_count',color='action', title='Top 10 user by count of stake at each action')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')
with cc2:
 fig = px.bar(df7, x='user', y='volume',color='action', title='Top 10 user by volume of stake at each action')
 fig.update_layout(legend_title=None, legend_y=0.5)
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.write("""
 # Staking reward #
 
 """
)
cc1, cc2= st.columns([1, 1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df9["count_reward"][0]),
    label="Total count of reward distributed",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df9["count_user"][0]),
    label="Total count of user that get reward",
)
cc1, cc2= st.columns([1, 1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df9["reward_luna"][0]),
    label="Total volume of Luna that rewarded",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df9["reward_usd"][0]),
    label="Total volume of USD that rewarded",
)
cc1, cc2= st.columns([1, 1])
with cc1:
  st.metric(
    value="{0:,.0f}".format(df9["reward_luna_per_user"][0]),
    label="Total volume of Luna that rewarded per each user",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df9["reward_usd_per_user"][0]),
    label="Total volume of USD that rewarded per each user",
)

st.subheader('Daily count of reward distributed')
st.caption('Daily count of reward distributed')
st.bar_chart(df8, x='date', y = 'count_reward', width = 400, height = 400)

st.subheader('Daily number of user that get reward')
st.caption('Daily number of user that get reward')
st.bar_chart(df8, x='date', y = 'count_user', width = 400, height = 400)

st.subheader('Daily volume of Luna that rewarded')
st.caption('Daily volume of Luna that rewarded')
st.bar_chart(df8, x='date', y = 'reward_luna', width = 400, height = 400)

st.subheader('Daily volume of USD that rewarded')
st.caption('Daily volume of USD that rewarded')
st.bar_chart(df8, x='date', y = 'reward_usd', width = 400, height = 400)

st.write("""
 # User categorize by count and volume of reward #
 In this charts you can see total user at each staking reward distribution (for reward count and volume).
 """
)
cc1, cc2= st.columns([1, 1])
with cc1:
 st.subheader('User categorize by count of rewarded')
 st.caption('User categorize by count of rewarded')
 st.bar_chart(df10, x='reward_count', y = 'count_users', width = 400, height = 400)
with cc2:
 fig = px.pie(df10, values='count_users', names='reward_count', title='User categorize by count of rewarded')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

cc1, cc2= st.columns([1, 1])
with cc1:
 st.subheader('User categorize by volume of rewarded')
 st.caption('User categorize by volume of rewarded')
 st.bar_chart(df11, x='staking_volume', y = 'count_users', width = 400, height = 400)
with cc2:
 fig = px.pie(df11, values='count_users', names='staking_volume', title='User categorize by volume of rewarded')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.write("""
 # Top 10 user by count and volume of reward #
 In this charts you can see top 10 user at geting staking reward distribution (for reward count and volume).
 """
)
cc1, cc2= st.columns([1, 1])
with cc1:
 st.caption('Top 10 user by count of rewarded')
 st.bar_chart(df12, x='user', y = 'count_reward', width = 400, height = 400)
with cc2:
 st.caption('Top 10 user by volume of rewarded')
 st.bar_chart(df13, x='user', y = 'vol_reward', width = 400, height = 400)

st.write("""
 # Richlist (Top 100) by wallet balance #
 
 """
)
st.subheader('Rich list')
st.table(df14)

st.write("""
 # Vesting schedule #
 
 """
)


st.metric(
  value="{0:,.0f}".format(df15["ves"][0]),
  label="Vesting Schedule",
)
st.caption('volume in vs volume out')
st.line_chart(df151, x='date1', y=['vol_in','vol_out'])

st.write("""
 # Bridge activity #
 
 """
)
cc1, cc2= st.columns([1, 1])
with cc1:
  fig = px.bar(df16, x='date', y='bridge_count',color='blockchain', title='Daily number of bridge transaction at each blockchain')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

with cc2:
  st.subheader('Total number of bridge transaction at each blockchain')
  st.caption('Total number of bridge transaction at each blockchain')
  st.bar_chart(df17, x='blockchain', y = 'bridge_count', width = 400, height = 400)

cc1, cc2= st.columns([1, 1])
with cc1:
  fig = px.bar(df16, x='date', y='bridger_count',color='blockchain', title='Daily number of bridger at each blockchain')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

with cc2:
  st.subheader('Total number of bridger at each blockchain')
  st.caption('Total number of bridger at each blockchain')
  st.bar_chart(df17, x='blockchain', y = 'bridger_count', width = 400, height = 400)

cc1, cc2= st.columns([1, 1])
with cc1:
  fig = px.bar(df16, x='date', y='bridge_volume',color='blockchain', title='Daily volume of bridge transaction at each blockchain')
  fig.update_layout(legend_title=None, legend_y=0.5)
  st.plotly_chart(fig, use_container_width=True, theme='streamlit')

with cc2:
  st.subheader('Total volume of bridge transaction at each blockchain')
  st.caption('Total volume of bridge transaction at each blockchain')
  st.bar_chart(df17, x='blockchain', y = 'bridge_volume', width = 400, height = 400)

st.write("""
 # Bridger categorize by count and volume of bridge #
 In this charts you can see total user in bridging activity (count and volume).
 """
)
cc1, cc2= st.columns([1, 1])
with cc1:
 st.subheader('User categorize by count of bridge')
 st.caption('User categorize by count of bridge')
 st.bar_chart(df18, x='bridge_count', y = 'count_users', width = 400, height = 400)
with cc2:
 fig = px.pie(df18, values='count_users', names='bridge_count', title='User categorize by count of bridge')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

cc1, cc2= st.columns([1, 1])
with cc1:
 st.subheader('User categorize by volume of bridge')
 st.caption('User categorize by volume of bridge')
 st.bar_chart(df19, x='bridge_volume', y = 'count_users', width = 400, height = 400)
with cc2:
 fig = px.pie(df19, values='count_users', names='bridge_volume', title='User categorize by volume of bridge')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.write("""
 # Top 10 bridger by count and volume of bridge #

 """
)
cc1, cc2= st.columns([1, 1])
with cc1:
 st.caption('Top 10 user by count of bridge')
 st.bar_chart(df20, x='user', y = 'bridge_count', width = 400, height = 400)
with cc2:
 st.caption('Top 10 user by volume of bridge')
 st.bar_chart(df21, x='user', y = 'bridge_volume', width = 400, height = 400)
