# MongoSlurp

![MongoSlurp Logo](images/lickitung.gif)

MongoSlurp is a small and very unfinished program that helps process CSV files before inserting them into MongoDB.

## How to use

```sh
python3 mongo_slurp.py 'MongoDB URI' database collection file_to_import.csv
```

Example: 

```sh
python3 mongo_slurp.py 'mongodb://localhost' test zips data/zips.csv
```
