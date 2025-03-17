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
COUNTRY_CODES = ["AF", "AS", "AU", "BD", "BT", "IO", "BN", "KH", "CN", "CX",
                 "CC", "CK", "TL", "FJ", "PF", "TF", "GU", "HK", "IN", "ID",
                 "JP", "KI", "KP", "KR", "LA", "MO", "MY", "MV", "MH", "FM",
                 "MN", "MM", "NR", "NP", "NC", "NZ", "NU", "NF", " MP", "PK",
                 "PW", "PG", "PH", "PN", "WS", "SG", "SB", "LK", "TW", "TH",
                 "TK", "TO", "TV", "VU", "VN", "WF"]

# ASes representing more than EYEBALL_MIN_PERC percent of the population are
# considered as eyeball networks
EYEBALL_MIN_PERC = 1

# ASes with a country hegemony value higher than HEGE_MIN are considered as
# transit networks
HEGE_MIN = 0.01

import os
from collections import defaultdict
from IPython.display import display, HTML
from plotly.graph_objs import *
import plotly.express as px
import pandas as pd
from scipy.cluster.hierarchy import linkage, fcluster


#init_notebook_mode(connected=True) 

# Setup access to IYP
from neo4j import GraphDatabase

# Prepare output folders
os.makedirs('output', exist_ok=True)
os.makedirs('output/ixp_distribution', exist_ok=True)
os.makedirs('output/as_peering_count', exist_ok=True)
os.makedirs('output/ixp_stats', exist_ok=True)
os.makedirs('output/violin', exist_ok=True)
os.makedirs('output/box', exist_ok=True)

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
        res, _, keys = db.execute_query(query, country_code=country_code, 
                                        eyeball_min_perc=EYEBALL_MIN_PERC, hege_min=HEGE_MIN)
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
        res, _, keys = db.execute_query(query, country_code=country_code,
                                        eyeball_min_perc=EYEBALL_MIN_PERC, hege_min=HEGE_MIN)
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
fig.write_html('output/as_peering_count/all.html')

query_top = """
MATCH (ases:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
MATCH (ases)-[ihr_rank:RANK {reference_org:'IHR', weightscheme:'as'}]-(:Ranking)
WHERE (ases)-[:ORIGINATE]-(:Prefix) AND ihr_rank.hege > $hege_min
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
fig.write_html('output/as_peering_count/transit.html')

query_pop = """
MATCH (ases:AS)-[:COUNTRY {reference_org:'NRO'}]-(selected_country:Country {country_code:$country_code})
MATCH (ases)-[p:POPULATION]-(selected_country)
WHERE (ases)-[:ORIGINATE]-(:Prefix) AND p.percent > $eyeball_min_perc
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
fig.write_html('output/as_peering_count/eyeball.html')

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
MIN_NB_AS = 0.05
NORMALIZE = False

# Using every dataset that provide IXP membership (may cretate some error for
# IXPs not uniquely identified by CAIDA ix dataset)
query_as_membership = """
MATCH (:Country {country_code:$country_code})-[:COUNTRY {reference_org:'NRO'}]-(member:AS)
WHERE (member)-[:ORIGINATE]-(:Prefix)
OPTIONAL MATCH (member)-[:CATEGORIZED {reference_name:'bgptools.as_names'}]-(tag:Tag)
OPTIONAL MATCH (member)-[:MEMBER_OF]-(ix_dom:IXP)-[:COUNTRY]-(:Country {country_code:$country_code})
OPTIONAL MATCH (member)-[:MEMBER_OF]-(ix_intl:IXP)-[:COUNTRY]-(other_country:Country)
WHERE other_country.country_code <> $country_code
RETURN  member.asn AS asn, coalesce(tag.label, 'Other') AS label, count(DISTINCT lower(ix_dom.name)) AS nb_dom_ix, count(DISTINCT lower(ix_intl.name)) AS nb_intl_ix
"""

for country_code in COUNTRY_CODES:
    res, _, keys = db.execute_query(query_as_membership, country_code=country_code)
    df = pd.DataFrame(res, columns=keys)

    fig = px.box(df, x='label', y='nb_dom_ix')
    fig.write_html(f'output/box/{country_code}_dom.html')
    fig = px.box(df, x='label', y='nb_intl_ix')
    fig.write_html(f'output/box/{country_code}_intl.html')


def heatmap_ixps(query, fname_suffix):
    for country_code in COUNTRY_CODES:
        ixs = defaultdict(set)
        countries = defaultdict(set)
        asns = set()
        membership_per_dataset = defaultdict(set)
        res, _, _ = db.execute_query(query, country_code=country_code,
                                     hege_min=HEGE_MIN, eyeball_min_perc=EYEBALL_MIN_PERC)
        for ix_name, member_asn, ix_country, data_source in res:
            ixs[ix_name.lower()].add(member_asn)
            asns.add(member_asn)
            countries[ix_country].add(member_asn)

            # Keep track of data sources
            membership_per_dataset[data_source].add(f'{ix_name}, {member_asn}')

        # remove unpopular international IXPs
        to_remove = []
        threshold_ixp = max(int(len(asns)*MIN_NB_AS), 5)
        for ix, members in ixs.items():
            if len(members) < threshold_ixp and not ix.endswith(country_code.lower()):
                to_remove.append(ix)
            elif ix.endswith(country_code.lower()) and len(members) < 5:
                to_remove.append(ix)


        for ix in to_remove:
            ixs.pop(ix)

        # Build the matrix
        nb_members_matrix = []
        for members0 in ixs.values():
            row = []
            for members1 in ixs.values():
                row.append(len(members0.intersection(members1)))
                if NORMALIZE:
                    row[-1] /= len(members0)

            nb_members_matrix.append(row)

        if len(nb_members_matrix) > 2:
            # Sort the matrix / Cluster the data
            threshold_cluster = 0.2
            Z = linkage(nb_members_matrix, 'ward')
            clusters = list(fcluster(Z, threshold_cluster, criterion='distance'))

            # clusterer = AgglomerativeClustering(n_clusters=len(nb_members_matrix), metric="precomputed", linkage="average")
            # clusters = list(clusterer.fit_predict(nb_members_matrix))
            print(clusters)

            labels = list(ixs.keys())
            sorted_labels = []
            sorted_matrix = []
            for i in range(max(clusters)):
                idx = clusters.index(i+1)
                sorted_labels.append(labels[idx])

                row = nb_members_matrix[idx]
                sorted_row = [row[clusters.index(j+1)] for j in range(max(clusters))]
                sorted_matrix.append(sorted_row)

            title = f'{country_code}:  {len(asns)} {country_code} ASNs peer at IXPs (intl. IXP with less than {threshold_ixp} ASes not shown)'

            for data_source, mem in membership_per_dataset.items():
                title += f'<br>   {len(mem)} membership reported by {data_source}'
            title += '<br>   Top countries where these ASNs peer (nb. unique ASNs):'
            top_countries = sorted(countries.items(), key=lambda x: len(x[1]), reverse=True)
            for cc, asns in top_countries[:5]:
                title += f'<br>        {len(asns)} peers in {cc}'

            fig = px.imshow(sorted_matrix, x=sorted_labels, y=sorted_labels,
                            color_continuous_scale='sunsetdark', title=title, text_auto=True)
            fig.write_html(f'output/ixp_distribution/{country_code}_{fname_suffix}.html')
        else:
            print(f'WARNING: no data for {country_code}')


query_ix_mem_all = """
MATCH (members:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
WHERE (members)-[:ORIGINATE]-(:Prefix)
MATCH (ix)-[mo:MEMBER_OF]-(members)
OPTIONAL MATCH (ix:IXP)-[:COUNTRY]-(ix_country:Country)
OPTIONAL MATCH (ix:IXP)-[:MANAGED_BY {reference_org:'PeeringDB'}]-(ix_org:Organization)
RETURN  ix.name + ' - ' + upper(coalesce(ix_country.country_code, 'zz')) AS ix_name,
members.asn AS member_asn, ix_country.country_code AS ix_country,
mo.reference_org AS data_source
ORDER BY ix_name, ix_org.name
"""

heatmap_ixps(query_ix_mem_all, 'all')

if True:
    query_ix_mem_transit = """
    MATCH (members:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
    WHERE (members)-[:ORIGINATE]-(:Prefix)
    MATCH (members)-[ihr_rank:RANK {reference_org:'IHR', weightscheme:'as'}]-(:Ranking)
    WHERE ihr_rank.hege > $hege_min
    MATCH (ix)-[mo:MEMBER_OF]-(members)
    OPTIONAL MATCH (ix:IXP)-[:COUNTRY]-(ix_country:Country)
    OPTIONAL MATCH (ix:IXP)-[:MANAGED_BY {reference_org:'PeeringDB'}]-(ix_org:Organization)
    RETURN  ix.name + ' - ' + upper(coalesce(ix_country.country_code, 'zz')) AS ix_name,
    members.asn AS member_asn, ix_country.country_code AS ix_country,
    mo.reference_org AS data_source
    ORDER BY ix_name, ix_org.name
    """

#    query_ix_mem_transit = """
#    MATCH (members:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
#    MATCH (members)-[ihr_rank:RANK {reference_org:'IHR', weightscheme:'as'}]-(:Ranking)
#    WHERE ihr_rank.hege > $hege_min
#    MATCH (ix)-[mo:MEMBER_OF]-(members)
#    OPTIONAL MATCH (ix:IXP)-[:COUNTRY]-(ix_country:Country)
#    OPTIONAL MATCH (ix:IXP)-[:MANAGED_BY {reference_org:'PeeringDB'}]-(ix_org:Organization)
#    RETURN  ix.name + ' - ' + upper(coalesce(ix_country.country_code, 'zz')) AS ix_name,
#    members.asn AS member_asn, ix_country.country_code AS ix_country,
#    mo.reference_org AS data_source
#    ORDER BY ix_name, ix_org.name
#    """

    heatmap_ixps(query_ix_mem_transit, 'transit')

    query_ix_mem_eyeball = """
    MATCH (members:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
    WHERE (members)-[:ORIGINATE]-(:Prefix)
    MATCH (ix)-[mo:MEMBER_OF]-(members)
    MATCH (members)-[p:POPULATION]-(selected_country)
    WHERE  p.percent > $eyeball_min_perc
    OPTIONAL MATCH (ix:IXP)-[:COUNTRY]-(ix_country:Country)
    OPTIONAL MATCH (ix:IXP)-[:MANAGED_BY {reference_org:'PeeringDB'}]-(ix_org:Organization)
    RETURN  ix.name + ' - ' + upper(coalesce(ix_country.country_code, 'zz')) AS ix_name,
    members.asn AS member_asn, ix_country.country_code AS ix_country,
    mo.reference_org AS data_source
    ORDER BY ix_name, ix_org.name
    """

#    MATCH (members:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
#    MATCH (members)-[p:POPULATION]-(selected_country)
#    WHERE  p.percent > $eyeball_min_perc
#    MATCH (ix)-[mo:MEMBER_OF]-(members)
#    OPTIONAL MATCH (ix:IXP)-[:COUNTRY]-(ix_country:Country)
#    OPTIONAL MATCH (ix:IXP)-[:MANAGED_BY {reference_org:'PeeringDB'}]-(ix_org:Organization)
#    RETURN  ix.name + ' - ' + upper(coalesce(ix_country.country_code, 'zz')) AS ix_name,
#    members.asn AS member_asn, ix_country.country_code AS ix_country,
#    mo.reference_org AS data_source
#    ORDER BY ix_name, ix_org.name
#    """

    heatmap_ixps(query_ix_mem_eyeball, 'eyeball')

    query_ix_mem_content = """
    MATCH (members:AS)-[:COUNTRY {reference_org:'NRO'}]-(:Country {country_code:$country_code})
    WHERE (members)-[:ORIGINATE]-(:Prefix)
    MATCH (members)-[:CATEGORIZED]-(:Tag {label:'Content'})
    MATCH (ix)-[mo:MEMBER_OF]-(members)
    OPTIONAL MATCH (ix:IXP)-[:COUNTRY]-(ix_country:Country)
    OPTIONAL MATCH (ix:IXP)-[:MANAGED_BY {reference_org:'PeeringDB'}]-(ix_org:Organization)
    RETURN  ix.name + ' - ' + upper(coalesce(ix_country.country_code, 'zz')) AS ix_name,
    members.asn AS member_asn, ix_country.country_code AS ix_country,
    mo.reference_org AS data_source
    ORDER BY ix_name, ix_org.name
    """

    heatmap_ixps(query_ix_mem_content, 'content')

    query_ix_mem_intl = """
    MATCH (members:AS)-[mo:MEMBER_OF]-(ix:IXP)-[:COUNTRY]-(ix_country:Country {country_code:$country_code})
    MATCH (members:AS)-[:COUNTRY {reference_org:'NRO'}]-(as_country:Country)
    WHERE  as_country.country_code <> $country_code AND (members)-[:ORIGINATE]-(:Prefix)
    OPTIONAL MATCH (ix:IXP)-[:MANAGED_BY {reference_org:'PeeringDB'}]-(ix_org:Organization)
    RETURN  ix.name + ' - ' + upper(coalesce(ix_country.country_code, 'zz')) AS ix_name,
    members.asn AS member_asn, ix_country.country_code AS ix_country,
    mo.reference_org AS data_source
    ORDER BY ix_name, ix_org.name
    """

    heatmap_ixps(query_ix_mem_intl, 'intl')
##

query_ix_stats = """
MATCH (ix:IXP)-[:COUNTRY]-(cc:Country {country_code:$country_code})
OPTIONAL MATCH (ix)-[:MEMBER_OF]-(content_as:AS)-[:CATEGORIZED]-(:Tag {label:'Content'})
OPTIONAL MATCH (ix)-[:MEMBER_OF]-(eyeball_as:AS)-[:CATEGORIZED]-(:Tag {label:'Eyeball'})
OPTIONAL MATCH (ix)-[:MEMBER_OF]-(a:AS)
RETURN ix.name AS ix_name, count(DISTINCT a.asn) AS nb_members, count(DISTINCT content_as) AS nb_content,
count(DISTINCT eyeball_as) AS nb_eyeball, collect(DISTINCT content_as.asn) AS content_ases,
collect(DISTINCT eyeball_as.asn) AS eyeball_ases, cc.country_code as country_code
"""

for country_code in COUNTRY_CODES:
    res, _, keys = db.execute_query(query_ix_stats, country_code=country_code)
    df = pd.DataFrame(res, columns=keys)

    fig = px.scatter(df, x='nb_content', y='nb_eyeball', size='nb_members', hover_name='ix_name')
    fig.write_html(f'output/ixp_stats/{country_code}.html')


