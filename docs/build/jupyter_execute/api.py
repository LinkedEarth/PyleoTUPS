#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyleotups import Dataset
ds=Dataset()
df = ds.search_studies(noaa_id=33213)
dfs = ds.get_data(dataTableIDs="45859")
dfs[0].head()


# In[2]:


from pyleotups import Dataset
ds=Dataset()
dsf = ds.search_studies(noaa_id=33213)
df = ds.get_funding()
df.head()


# In[3]:


from pyleotups import Dataset
ds=Dataset()
dsf = ds.search_studies(noaa_id=33213)
df = ds.get_geo()
df.head()


# In[4]:


from pyleotups import Dataset
ds=Dataset()
dsf = ds.search_studies(noaa_id=33213)
bib, df = ds.get_publications()
df.head()


# In[5]:


from pyleotups import Dataset
ds=Dataset()
df = ds.search_studies(noaa_id=33213)
df.head()


# In[6]:


from pyleotups import Dataset
ds=Dataset()
dsf = ds.search_studies(noaa_id=33213)
df = ds.get_tables()
df.head()


# In[7]:


from pyleotups import Dataset
ds=Dataset()
dsf = ds.search_studies(noaa_id=33213)
df_var = ds.get_variables(dataTableIDs="45859")
df_var.head()


# In[8]:


import pyleotups as pt
ds = pt.Dataset()
df_noaa = ds.search_studies(noaa_id=13156)
df_xml = ds.search_studies(xml_id=1840)
df_noaa.head()
df_xml.head()


# In[9]:


# Single phrase
df_singlephrase = ds.search_studies(search_text="younger dryas", limit=20)
df_singlephrase.head()


# In[10]:


# Logical operator (AND)
df_logop = ds.search_studies(search_text="loess AND stratigraphy", limit=20)
df_logop.head()


# In[11]:


# Wildcards: '_' (single char), '%' (multi-char)
df_wc_1 = ds.search_studies(search_text="f_re", limit=20)
df_wc_2 = ds.search_studies(search_text="pol%", limit=20)
df_wc_1.head(), df_wc_2.head()


# In[12]:


# Escaping special characters (use backslashes)
df_specchar = ds.search_studies(search_text=r"noaa\-tree\-19260", limit=20)
df_specchar.head()


# In[13]:


# Multiple investigators (OR by default)
df_multinv_default = ds.search_studies(investigators=["Wahl, E.R.", "Vose, R.S."])
df_multinv_default.head()


# In[14]:


# Multiple investigators (AND by default)
df_multinv_and = ds.search_studies(investigators=["Wahl, E.R.", "Vose, R.S."], investigatorsAndOr = "and")
df_multinv_and.head()


# In[15]:


# Keywords: hierarchy with '>' and multiple via '|'
df_keywords = ds.search_studies(keywords="earth science>paleoclimate>paleocean>biomarkers")
df_keywords.head()


# In[16]:


# Location hierarchy
df_loc = ds.search_studies(locations="Continent>Africa>Eastern Africa>Zambia")
df_loc.head()


# In[17]:


# Species: four-letter codes (uppercase enforced)
df_species = ds.search_studies(species=["ABAL", "PIPO"])
df_species.head()


# In[18]:


# Data types: one or more IDs separated by '|'
df_muldatatypes = ds.search_studies(data_type_id="4|18")
df_muldatatypes.head()


# In[19]:


df_latlong = ds.search_studies(min_lat=68, max_lat=69, min_lon=30, max_lon=40)
df_latlong.head()


# In[20]:


df_elv = ds.search_studies(min_elevation=100, max_elevation=110)
df_elv.head()


# In[21]:


# Explicit BP with method
df_timew = ds.search_studies(earliest_year=12000, time_format="BP", time_method="overAny")
df_timew.head()


# In[22]:


# No time_format/time_method → defaults to CE
df_time_defualt = ds.search_studies(earliest_year=1500, latest_year=0)
df_time_defualt.head()


# In[23]:


df_recon = ds.search_studies(reconstruction=True)
df_recon.head()


# In[24]:


df_recent = ds.search_studies(recent=True, limit=25)
df_recent.head()


# In[25]:


# Limit up to first 10 results
df_limit = ds.search_studies(earliest_year=12000, time_format="BP", time_method="overAny", limit=10)
df_limit.head()

# Skip the first 10 results (i.e., get results 11-20)
df_skip = ds.search_studies(earliest_year=12000, time_format="BP", time_method="overAny", limit=10, skip=10)
df_skip.head()

