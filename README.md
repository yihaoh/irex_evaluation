# Evaluation of OurSys

This repo contains the necessary scripts/queries to reproduce the experiment results in the paper. We assume the OS is Ubuntu.

## PostgreSQL Setup
First, install PostgreSQL along with some necessary dependencies.
```
sudo bash system_setup.sh
```

Go into the Postgres and create a superuser for testing:
```
CREATE USER oursys WITH SUPERUSER ENCRYPTED PASSWORD 'oursys';
```

# Bloom Filter Extension
To build and install bloom filter extension, go to `sargsum` and execute the following:

```
sudo make clean
make
sudo make install
```

Now go into the database and execute:
```
CREATE EXTENSION BLMFL;
```

Also for the same database, run all SQL commands in the `helper.sql`.


## Database Setup
Use [tpch-dbgen](https://github.com/databricks/tpch-dbgen) to generate the database instance. Move all `*.tbl` files under `tpch-setup` folder, and run `bash setup.sh` to sanitize the `tbl` files. Under `tpch-setup` folder, execute the following commands (replace `$dbname` with the database you wish to load):

```
mkdir data && mv *.tbl data
psql -d $dbname -af create.sql
psql -d $dbname -af load.sql
psql -d $dbname -af index.sql
```


## TPC-H Queries
All TPC-H queries are instantiated with reasonable parameters (see details in `queries_for_test`). In addition, to test Bloom Filter and Predicate Pushdown alone, we pick two queries and they are under `queries_for_test_special`. Each table has three queries: milestone query, page query and table query. 

## Running Test Scripts
Here is a sample command to run the test for all queries:
```
sudo python query_test.py --scale 1 --pg_sz 200 --db tpch1
```
The result will be under the generated `results` folder. For more command parameters, please check out `query_test.py`.

Here is a sample command to run the test for only Bloom Filter and Predicate Pushdown:
```
sudo python query_test_special.py --scale 1 --pg_sz 50 --db tpch1
```
The result will be under the generated `results_special` folder. For more command parameters, please check out `query_test_special.py`.
