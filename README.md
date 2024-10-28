# PyTUPS
PyTUPS: Table Understanding for Paleoclimate Studies in Python

Using the PyTUPS notebook, you would be able to make pytups object, to access the NCEI NOAA respositories by basic arguments like xmlId,NOAAStudyId, dataPublisher, investigators, latitude, longitude, location.

Steps to access:

#### 1. Create a PyTUPS object;
```python
pytups = pyTups()
```

#### 2. Search for studies:
if xmlID or NOAAId is availble, either of these IDs will have preference. Else, search using other parameters

example:

```python
pytups.search_studies(xml_id=16017) #option 1
pytups.search_studies(data_publisher="NOAA", investigators="Khider") #option 2
```

search_studies() gives an output of a dataframe displaying appropriate matches as per arguments passed

#### 3. Select respective url
```python
pytups.select_study_url(0)  # Replace 0 with the desired index of the response dataframe
```

#### 4. Load Studies/Data
Only after the above 3 steps, Call the load data method
```python
df, metadata = pytups.load_data_from_selected_url()
```
The method will return a dataframe and metadata associated with the respective study/datafile


