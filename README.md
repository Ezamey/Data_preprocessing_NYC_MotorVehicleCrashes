# DataPreprocessingScript
![alt text](img/nycut.png)
```
```

## Libraries

The needed libraries are in the requirement.txt. To install it, use the command below in the root file :
```shell
python -m pip install -r requirements.txt
```
## What does he do :

This script preprocess the datas presents in the dataset from [NYC OpenData](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) using [Pandas](https://pandas.pydata.org/) for possibles uses in machine learning algorithms.

## How to use it :

### 1.Classic :
If you want a vague idea of the data you will get after the preprocessing part. Just run the __main.py__ file. In the terminal, the differents steps will be displayed and a __final.csv__ file will be created in the root directory. This __final.csv__ file is the result of the preprocessing  of a __100.000 lines__ dataset.

```python
py main.py
```

If you  want to preprocess the whole dataset(). Run the same command with an argument. This argument will be the name of your futur __.csv__ file. Be aware that  this dataset  is quite heavy _(1,738,137 lines)_

```python
py main.py biggy #will create a biggy.csv file in the root directory
```

### 2. A little deeper :

In  the preprocessing parts, some missing boroughs datas are filled using different methods. If you have time and want some more precise result. You can run the following commands :

```python
py pipelines/borough_finder.py
```
```python
py pipelines/borough_finder.py biggy  #will create a biggywith_borough.csv file in the root directory
```

## _TODO_ :
- Find a way to implements a faster way to use [Geopy](https://geopy.readthedocs.io/en/stable/) for filling missing  localisation values.

#### By and For :

__Christian Melot [@Becode](https://becode.org/fr/)__
![alt text](img/nycut2.png)