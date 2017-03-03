####A program running on sparkstreaming.
####Consume data from kafka,then produce to kafka,or write to Elasticsearch.
#Usage
vim AppConfig.json
`LIBRARY_PATH="/data/sdb1/python/lib"`  
`SCHEMA_PATH="/data/sdb1/python/schema"`  
`PYTHON_LIB_PATH="/data/sdb1/python/pylib"`
##RUN
`python StartJobShell.py -j jobname`
