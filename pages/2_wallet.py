import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Transactions",
    layout= "wide",
    page_icon="💵",
)
st.title("💵 Wallet")
st.sidebar.success("💵 Wallet")

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
#daily new wallet
df_query="""
with main as(select
min(date_trunc('week',BLOCK_TIMESTAMP)) as min_date,
TX_SENDER
FROM terra.core.fact_transactions
group by 2
)
select
min_date::date as date,
count(TX_SENDER) as count_new,
sum (count_new) over (order by date) as cum_new
from main
group by 1
"""

df = querying_pagination(df_query)

#total new wallet
df1_query="""
with main as(select
min(date_trunc('week',BLOCK_TIMESTAMP)) as min_date,
TX_SENDER
FROM terra.core.fact_transactions
group by 2
)
select
count(TX_SENDER) as count_new
from main
"""

df1 = querying_pagination(df1_query)

#daily active wallet
df2_query="""
select
date_trunc('week',BLOCK_TIMESTAMP) as date,
count(TX_SENDER) as count,
sum (count) over (order by date) as cum_wall
FROM terra.core.fact_transactions
group by 1
"""

df2 = querying_pagination(df2_query)

#total active wallet
df3_query="""
select
count(TX_SENDER) as count
FROM terra.core.fact_transactions
"""

df3 = querying_pagination(df3_query)

st.write("""
 # New wallet #
 New Accounts are new wallets being created on the Terra blockchain.
 Here you can see weekly total new wallet created:
 """
)
st.metric(
 value="{0:,.0f}".format(df1["count_new"][0]),
 label="Total number of new wallet",
)
cc1, cc2= st.columns([1, 1])

with cc1:
 st.caption('Weekly number of new wallet')
 st.bar_chart(df, x='date', y = 'count_new', width = 400, height = 400)
with cc2:
 st.caption('cumulative number of new wallet')
 st.line_chart(df, x='date', y = 'cum_new', width = 400, height = 400)

st.write("""
 # Active wallet #
 The Daily Number of Active Accounts is a measure of how many wallets on Terra are making transactions on chain. Over the last week.
 Here you can see weekly total active wallet created:
 """
)
st.metric(
 value="{0:,.0f}".format(df3["count"][0]),
 label="Total number of active wallet",
)
cc1, cc2= st.columns([1, 1])

with cc1:
 st.caption('Weekly number of active wallet')
 st.bar_chart(df2, x='date', y = 'count', width = 400, height = 400)
with cc2:
 st.caption('cumulative number of active wallet')
 st.line_chart(df2, x='date', y = 'cum_wall', width = 400, height = 400)
