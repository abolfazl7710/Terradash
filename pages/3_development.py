import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Development",
    layout= "wide",
    page_icon="📈",
)
st.title("📈 Development")
st.sidebar.success("📈 Development")

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
#daily new contract
df_query="""
with main as (select 
distinct tx:body:messages[0]:contract as contract,
min(block_timestamp) as day1
from terra.core.fact_transactions 
where TX_SUCCEEDED='TRUE'
group by 1
)
select
date_trunc('week',day1) as date,
count(distinct contract) as new_contract,
sum(new_contract) over (order by date) as cum_contract
from main
group by 1
"""

df = querying_pagination(df_query)

#total new contract
df1_query="""
with main as (select 
distinct tx:body:messages[0]:contract as contract,
min(block_timestamp) as day1
from terra.core.fact_transactions 
where TX_SUCCEEDED='TRUE'
group by 1
)
select
count(distinct contract) as new_contract
from main
"""

df1 = querying_pagination(df1_query)

#daily active contract
df2_query="""
select 
date_trunc('week',block_timestamp) as date,
count(distinct tx:body:messages[0]:contract) as count_active,
sum(count_active) over (order by date) as cum_active
from terra.core.fact_transactions 
where TX_SUCCEEDED='TRUE'
group by 1
"""

df2 = querying_pagination(df2_query)

#total active contract
df3_query="""
select 
count(distinct tx:body:messages[0]:contract) as count_active
from terra.core.fact_transactions 
where TX_SUCCEEDED='TRUE'
"""

df3 = querying_pagination(df3_query)

st.write("""
 # New contract #
 Contracts on NEAR are simply programs stored on a blockchain that run when predetermined conditions are met. The Daily Number of New Contracts is a valuable metric for understanding the health and growth of an ecosystem.
 Here you can see weekly total new contract created:
 """
)
st.metric(
 value="{0:,.0f}".format(df1["new_contract"][0]),
 label="Total number of new wallet",
)
cc1, cc2= st.columns([1, 1])

with cc1:
 st.caption('Weekly number of new contract')
 st.bar_chart(df, x='date', y = 'new_contract', width = 400, height = 400)
with cc2:
 st.caption('cumulative number of new contract')
 st.line_chart(df, x='date', y = 'cum_contract', width = 400, height = 400)

st.write("""
 # Active contract #
 The Daily Number of Active contract is a measure of how many contract that execute in a week on Terra chain.
 Here you can see weekly total active contract created:
 """
)
st.metric(
 value="{0:,.0f}".format(df3["count_active"][0]),
 label="Total number of active contract",
)
cc1, cc2= st.columns([1, 1])

with cc1:
 st.caption('Weekly number of active contract')
 st.bar_chart(df2, x='date', y = 'count_active', width = 400, height = 400)
with cc2:
 st.caption('cumulative number of active contract')
 st.line_chart(df2, x='date', y = 'cum_active', width = 400, height = 400)