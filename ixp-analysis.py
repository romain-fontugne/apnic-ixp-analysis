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
# # IXP and ASN Connectivity Analysis
# Determine the proportion of active ASNs in the region that are connected to at
# least one IXP and those connected to multiple IXPs. This will provide insights
# into the level of engagement and redundancy in the local peering environment.

# %%
COUNTRY_CODES = ["NZ", "AU", "JP", "KR", "CN", "TH", "GU", "IN", "PK", "PH", "ID", "SG", "MY", "MM", "BD"]

# imports for output
from collections import defaultdict
from IPython.display import display, Markdown, HTML
# 
import plotly.graph_objects as go
from plotly.offline import init_notebook_mode, iplot
from plotly.graph_objs import *
import plotly.express as px
from itertools import cycle
import pandas as pd
import numpy as np

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

query_dt = """
MATCH (:Country {country_code:$country_code})-[p:POPULATION]-(:Estimate) RETURN p.reference_time_fetch
"""
res, _, _ = db.execute_query(query_dt, country_code=COUNTRY_CODES[0])
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


def as_no_ixp(query) -> pd.DataFrame:
    country_df = {}
    as_nb_ix_list = []

    for country_code in COUNTRY_CODES:
        res, _, keys = db.execute_query(query, country_code=country_code)
        df = pd.DataFrame(res, columns=keys)
        df = df[df['nb_ix'] == 0]

        # Keep data for later analysis
        df['country'] = country_code
        country_df[country_code] = df
        as_nb_ix_list.append(df)

    df = pd.concat(as_nb_ix_list)

    return df


# Find ASes registered in the country and the IXPs they are member of
query_all = """
MATCH (ases:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
WHERE (ases)-[:ORIGINATE]-(:Prefix)
OPTIONAL MATCH (ases)-[:MEMBER_OF]-(ix:IXP)
OPTIONAL MATCH (ases)-[:RANK {reference_name:'caida.asrank'}]-(ix:IXP)
OPTIONAL MATCH (ix)-[:COUNTRY]-(cc:Country)
OPTIONAL MATCH (ases)-[:NAME {reference_org:'RIPE NCC'}]-(as_name:Name)
RETURN $country_code AS country, ases.asn AS asn, as_name.name AS as_name, count(DISTINCT ix) AS nb_ix,
collect(DISTINCT cc.country_code) AS ix_country, collect(DISTINCT ix.name) AS ix_name
"""
df = group_by_nb_ix(query_all)
fig = px.bar(df, x='country', y="asn", color='nb_ix',
             color_continuous_scale=[(0.00, "red"),   (0.01, "red"), (0.01, "green"),
                                     (0.66, "green"), (0.66, "blue"),  (1.00, "blue")],
             title='Distribution of all ASes at IXPs', text='nb_ix')
fig.show()

query_top = """
MATCH (ases:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
MATCH (ases)-[ihr_rank:RANK {reference_org:'IHR', weightscheme:'as'}]-(:Ranking)
WHERE (ases)-[:ORIGINATE]-(:Prefix) AND ihr_rank.hege > 0.01
OPTIONAL MATCH (ases)-[:MEMBER_OF]-(ix:IXP)
OPTIONAL MATCH (ases)-[:RANK {reference_name:'caida.asrank'}]-(ix:IXP)
OPTIONAL MATCH (ix)-[:COUNTRY]-(cc:Country)
OPTIONAL MATCH (ases)-[:NAME {reference_org:'RIPE NCC'}]-(as_name:Name)
RETURN $country_code AS country, ases.asn AS asn, as_name.name AS as_name, count(DISTINCT ix) AS nb_ix,
collect(DISTINCT cc.country_code) AS ix_country, collect(DISTINCT ix.name) AS ix_name
"""
df = group_by_nb_ix(query_top)
fig = px.bar(df, x='country', y="asn", color='nb_ix',
             color_continuous_scale=[(0.00, "red"),   (0.01, "red"), (0.01, "green"),
                                     (0.66, "green"), (0.66, "blue"),  (1.00, "blue")],
             title='Distribution of transit networks at IXPs (transit for more that 1% ASes)', text='nb_ix')
fig.show()

query_pop = """
MATCH (ases:AS)-[:COUNTRY {reference_org:'NRO'}]-(selected_country:Country {country_code:$country_code})
MATCH (ases)-[p:POPULATION]-(selected_country)
WHERE (ases)-[:ORIGINATE]-(:Prefix) AND p.percent > 1
OPTIONAL MATCH (ases)-[:MEMBER_OF]-(ix:IXP)
OPTIONAL MATCH (ases)-[:RANK {reference_name:'caida.asrank'}]-(ix:IXP)
OPTIONAL MATCH (ix)-[:COUNTRY]-(cc:Country)
OPTIONAL MATCH (ases)-[:NAME {reference_org:'RIPE NCC'}]-(as_name:Name)
RETURN $country_code AS country, ases.asn AS asn, as_name.name AS as_name, count(DISTINCT ix) AS nb_ix,
collect(DISTINCT cc.country_code) AS ix_country, collect(DISTINCT ix.name) AS ix_name
"""
df = group_by_nb_ix(query_pop)
fig = px.bar(df, x='country', y="asn", color='nb_ix',
             color_continuous_scale=[(0.00, "red"),   (0.01, "red"), (0.01, "green"),
                                     (0.66, "green"), (0.66, "blue"),  (1.00, "blue")],
             title='Distribution of eyeball networks at IXPs (host more than 1% population)', text='nb_ix')
fig.show()

# %% [markdown]
# ## Eyeball networks not at IXPs

# %%
with open('output/no_ixp.html', 'w') as fp:
    as_no_ixp(query_pop).to_html(fp)

display(HTML(as_no_ixp(query_pop).to_html()))
# %%

# %% [markdown]
# # IXP Distribution and Peering Ecosystem Health
# Evaluate the distribution of ASNs across multiple IXPs to understand if there's
# a concentration of peers in few IXPs or if peers are evenly distributed across
# available IXPs. This includes analysing unique ASNs that only connect to a
# single IXP versus those that peer at multiple locations.

# %%
query_ix_mem = """
MATCH (members:AS)-[:COUNTRY {reference_org:'NRO'}]-(mem_country:Country {country_code:$country_code})
MATCH (ix)-[:MEMBER_OF]-(members)
OPTIONAL MATCH (ix:IXP)-[:COUNTRY]-(ix_country:Country)
RETURN ix_country.country_code + ' - ' + ix.name AS ix_name, members.asn AS member_asn, mem_country.country_code AS member_country ORDER BY ix_name
"""

MIN_NB_AS = 10

for country_code in COUNTRY_CODES:
    ixs = defaultdict(set)
    res, _, keys = db.execute_query(query_ix_mem, country_code=country_code)
    for ix_name, member_asn, member_country in res:
        ixs[ix_name.lower()].add(member_asn)

    # remove unpopular international IXPs
    to_remove = []
    for ix, members in ixs.items():
        if len(members) < MIN_NB_AS and not ix.startswith(country_code):
            to_remove.append(ix)

    for ix in to_remove:
        ixs.pop(ix)

    # Plot in a matrix
    nb_members_matrix = []
    for members0 in ixs.values():
        row = []
        for members1 in ixs.values():
            row.append(len(members0.intersection(members1)) / len(members0))

        nb_members_matrix.append(row)

    fig = px.imshow(nb_members_matrix, x=list(ixs.keys()), y=list(ixs.keys()),
                    color_continuous_scale='autumn', title=country_code)
    fig.show()
