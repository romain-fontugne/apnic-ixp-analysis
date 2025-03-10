# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Automated IXP report

# %%
COUNTRY_CODES = ["NZ", "AU", "JP", "KR", "CN", "TH", "GU", "IN", "PK", "PH", "ID", "SG", "MY"]

# imports for output
from collections import defaultdict
from IPython.display import display, Markdown
# 
import plotly.graph_objects as go
from plotly.offline import init_notebook_mode, iplot
from plotly.graph_objs import *
import plotly.express as px
from itertools import cycle
import pandas as pd

import radix
import socket

#init_notebook_mode(connected=True) 

# Setup access to IYP
from neo4j import GraphDatabase

# Using IYP local instance
URI = "neo4j://localhost:7687"
# Using IYP public instance
# URI = "neo4j://iyp-bolt.iijlab.net:7687"
AUTH = ('neo4j', 'password')
db = GraphDatabase.driver(URI, auth=AUTH)

query = """
MATCH (:Country {country_code:$country_code})-[p:POPULATION]-(:Estimate) RETURN p.reference_time_fetch
"""
res, _, _ = db.execute_query(query, country_code=COUNTRY_CODES[0])
if len(res):
    print(f"Data collected on {res[0][0]}")


def group_by_nb_ix(query):
    country_df = {}
    as_nb_ix_list = []

    for country_code in COUNTRY_CODES:
        res, _, keys = db.execute_query(query, country_code=country_code)
        df = pd.DataFrame(res, columns=keys)

        # Keep data for later analysis
        country_df[country_code] = df

        # Compute percentage of AS per number of IXP 
        gb = df.groupby('nb_ix').count()/len(df)
        gb['country'] = country_code

        as_nb_ix_list.append(gb)

    df = pd.concat(as_nb_ix_list)
    df['nb_ix'] = df.index

    return df


# Find ASes registered in the country and the IXPs they are member of
query_all = """
MATCH (ases:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
WHERE (ases)-[:ORIGINATE]-(:Prefix)
OPTIONAL MATCH (ases)-[:MEMBER_OF]-(ix:IXP)
OPTIONAL MATCH (ases)-[:RANK {reference_name:'caida.asrank'}]-(ix:IXP)
OPTIONAL MATCH (ix)-[:COUNTRY]-(cc:Country)
RETURN $country_code AS country, ases.asn AS asn, count(DISTINCT ix) AS nb_ix, collect(DISTINCT cc.country_code), collect(DISTINCT ix.name)
"""

query_pop = """
MATCH (ases:AS)-[:COUNTRY {reference_org:'NRO'}]-(selected_country:Country {country_code:$country_code})
MATCH (ases)-[p:POPULATION]-(selected_country)
WHERE (ases)-[:ORIGINATE]-(:Prefix) AND p.percent > 1
OPTIONAL MATCH (ases)-[:MEMBER_OF]-(ix:IXP)
OPTIONAL MATCH (ases)-[:RANK {reference_name:'caida.asrank'}]-(ix:IXP)
OPTIONAL MATCH (ix)-[:COUNTRY]-(cc:Country)
RETURN $country_code AS country, ases.asn AS asn, count(DISTINCT ix) AS nb_ix, collect(DISTINCT cc.country_code), collect(DISTINCT ix.name)
"""

query_top = """
MATCH (ases:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
MATCH (ases)-[ihr_rank:RANK {reference_org:'IHR', weightscheme:'as'}]-(:Ranking)
WHERE (ases)-[:ORIGINATE]-(:Prefix) AND ihr_rank.hege > 0.01
OPTIONAL MATCH (ases)-[:MEMBER_OF]-(ix:IXP)
OPTIONAL MATCH (ases)-[:RANK {reference_name:'caida.asrank'}]-(ix:IXP)
OPTIONAL MATCH (ix)-[:COUNTRY]-(cc:Country)
RETURN $country_code AS country, ases.asn AS asn, count(DISTINCT ix) AS nb_ix, collect(DISTINCT cc.country_code), collect(DISTINCT ix.name)
"""

df = group_by_nb_ix(query_all)
fig = px.bar(df, x='country', y="asn", color='nb_ix',
             color_continuous_scale=[(0.00, "red"),   (0.01, "red"), (0.01, "green"),
                                     (0.66, "green"), (0.66, "blue"),  (1.00, "blue")],
             title='Distribution of all ASes at IXPs', text='nb_ix')
fig.show()


df = group_by_nb_ix(query_top)
fig = px.bar(df, x='country', y="asn", color='nb_ix',
             color_continuous_scale=[(0.00, "red"),   (0.01, "red"), (0.01, "green"),
                                     (0.66, "green"), (0.66, "blue"),  (1.00, "blue")],
             title='Distribution of  at IXPs', text='nb_ix')
fig.show()
