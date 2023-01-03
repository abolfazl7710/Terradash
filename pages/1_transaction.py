import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Transactions",
    layout= "wide",
    page_icon="💰",
)
st.title("💰Transactions")
st.sidebar.success("💰Transactions")

@st.cache(ttl=600)
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
#total transaction
df_query="""
with table1 as(select 
count(TX_id) as tx_count,
sum(fee) as tot_fee,
count(case when TX_SUCCEEDED = True then TX_id end) as success_txn_count,
(success_txn_count / tx_count)*100 as success_rate,
(100 - success_rate) as fail_rate,
count(case when TX_SUCCEEDED = True then TX_id end) / (7*24*60) as tpm,
count(case when TX_SUCCEEDED = True then TX_id end) / (7*24*60*60) as tps
from terra.core.fact_transactions
)
select 
tx_count as tot_txn,
success_rate,
fail_rate,
tot_fee,
avg(tot_fee)/tx_count as avgfee_count,
avg(tpm) as tpm,
avg(tps) as tps
from table1
group by 1,2,3,4
"""

df = querying_pagination(df_query)

#daily transaction
df1_query="""
with table1 as(select 
date_trunc('week',block_timestamp) as date,
count(TX_id) as tx_count,
sum(fee) as tot_fee,
count(case when TX_SUCCEEDED = True then TX_id end) as success_txn_count,
(success_txn_count / tx_count)*100 as success_rate,
(100 - success_rate) as fail_rate,
count(case when TX_SUCCEEDED = True then TX_id end) / (7*24*60) as tpm,
count(case when TX_SUCCEEDED = True then TX_id end) / (7*24*60*60) as tps
from terra.core.fact_transactions
group by 1), main as
(select 
date::date as date,
tx_count as tot_txn,
success_rate,
fail_rate,
tot_fee,
avg(tot_fee)/tx_count as avgfee_count,
avg(tpm) as tpm,
avg(tps) as tps
from table1
group by 1,2,3,4,5
  ),avg as(SELECT
avg(success_rate) as avg_success,
100-avg_success as avg_fail,
avg(tpm) as avg_tpm,
avg(tps) as avg_tps
from main
  )
SELECT
*
from main , avg
"""
df1 = querying_pagination(df1_query)

#block timestamp
df2_query="""
with main as (
select
date_trunc('week',block_timestamp) as date,
lag(block_timestamp,1) over ( order by block_timestamp) as "block time",
datediff(second,"block time",block_timestamp) as "block time diff"
from near.core.fact_blocks
)
select 
date::date as date,
avg("block time diff") as avg_btime
from main
where "block time diff" > 0
group by 1
"""
df2 = querying_pagination(df2_query)

#avg block timestamp
df3_query="""
with main as (
select
block_timestamp::date as date,
lag(block_timestamp,1) over ( order by block_timestamp) as "block time",
datediff(second,"block time",block_timestamp) as "block time diff"
from near.core.fact_blocks
)
select 
avg("block time diff") as avg_btime
from main
where "block time diff" > 0
"""
df3 = querying_pagination(df3_query)

st.write("""
 # Overal activity #
 In this part the weekly Number of transactions per week , average transaction fee per transaction , total transaction fee , transaction per secound , transaction per minute and average block time difference , ... analyzed.

 """
)
cc1, cc2 , cc3= st.columns([1, 1,1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df["tot_txn"][0]),
    label="Total number of transaction",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df["tot_fee"][0]),
    label="Total transaction fee",
)
with cc3:
  st.metric(
    value="{0:,.0f}".format(df3["avg_btime"][0]),
    label="Average time between two block",
)

cc1, cc2 = st.columns([1, 1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df["success_rate"][0]),
    label="Average transaction success rate",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df["fail_rate"][0]),
    label="Average transaction fail rate",
)

cc1, cc2= st.columns([1, 1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df["tpm"][0]),
    label="Average transaction count per minute",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df["tps"][0]),
    label="Average transaction count per secound",
)
st.subheader('Total number of transaction per week')
st.caption('Total number of transaction per week')
st.bar_chart(df1, x='date', y = 'tot_txn', width = 400, height = 400)

st.subheader('Total transaction fee and average transaction fee per transaction per week ')
st.caption('Total transaction fee per week')
st.bar_chart(df1, x='date', y = 'tot_fee', width = 400, height = 400)
st.caption('Average transaction fee per transaction per week')
st.bar_chart(df1, x='date', y = 'avgfee_count', width = 400, height = 400)

st.subheader('Success rate vs fail rate')
st.write("""
 transaction success rate is calculated by dividing the total number of successful (approved) transactions by the total number of attempted transactions over a given time period.
 Here you can see transaction success and fail rate in weekly time frame:
 """
)
st.caption('Success rate')
st.line_chart(df1, x='date', y = ['success_rate','avg_success'], width = 400, height = 400)
st.caption('Fail rate')
st.line_chart(df1, x='date', y = ['fail_rate','avg_fail'], width = 400, height = 400)

st.subheader('Transaction per minute vs transaction per secound')
st.write("""
 Transactions per second (TPS) and Transactions per minute (TPM) refers to how many transactions the network can process in a second / minute, followed by how rapidly the network can confirm a trade or an exchange. The average transaction speed is significant because it indicates the network's current capacity to process transactions. 
 Here you can see TPM and TPS in weekly time frame:
 """
)
st.caption('Transaction per minute')
st.line_chart(df1, x='date', y = ['tpm','avg_tpm'], width = 400, height = 400)
st.caption('Transaction per secound')
st.line_chart(df1, x='date', y = ['tps','avg_tps'], width = 400, height = 400)

st.subheader('Average block time per week')
st.write("""
 Block time is the length of time it takes to create a new block in a cryptocurrency blockchain.it is verified by miners, who compete against each other to verify the transactions and solve the hash, which creates another block. under the proof-of-work consensus mechanism, cryptocurrency is rewarded for solving a block's hash and creating a new block.
 Here you can see average block time in weekly time frame:
 """
)
st.caption('Average block time per week')
st.line_chart(df2, x='date', y = 'avg_btime', width = 400, height = 400)