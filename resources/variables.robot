*** Variables ***
${SCHEMA_NAME}    YOUR_SCHEMA_NAME
${TABLE_NAME}    YOUR_TABLE_NAME

&{SOURCE_CONFIG}    host=onprem-db2-host    port=50000
...                 database=SOURCE_DB    username=user    password=pass

&{TARGET_CONFIG}    host=aws-rds-host    port=50000
...                 database=TARGET_DB    username=user    password=pass