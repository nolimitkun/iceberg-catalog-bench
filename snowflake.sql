SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();

--{"snowflake-vnet-subnet-id":["/subscriptions/61940d42-dbae-45a5-a08d-ca8717f2f151/resourceGroups/deployment-infra-rg2/providers/Microsoft.Network/virtualNetworks/deployment-vnet2/subnets/gs","/subscriptions/61940d42-dbae-45a5-a08d-ca8717f2f151/resourceGroups/deployment-infra-rg2/providers/Microsoft.Network/virtualNetworks/deployment-vnet2/subnets/xp","/subscriptions/c4163da0-f07a-42ab-a254-9ca2bf882e98/resourceGroups/deployment-infra-rg/providers/Microsoft.Network/virtualNetworks/deployment-vnet/subnets/gs","/subscriptions/c4163da0-f07a-42ab-a254-9ca2bf882e98/resourceGroups/deployment-infra-rg/providers/Microsoft.Network/virtualNetworks/deployment-vnet/subnets/xp","/subscriptions/debd2e8d-3a70-4668-9770-fb68c5a87b60/resourceGroups/deployment-infra-rg3/providers/Microsoft.Network/virtualNetworks/deployment-vnet3/subnets/gs","/subscriptions/debd2e8d-3a70-4668-9770-fb68c5a87b60/resourceGroups/deployment-infra-rg3/providers/Microsoft.Network/virtualNetworks/deployment-vnet3/subnets/xp"]}

--az storage account network-rule add --resource-group "datalake" --account-name "datalakestgmf6bsc" --subnet /subscriptions/debd2e8d-3a70-4668-9770-fb68c5a87b60/resourceGroups/deployment-infra-rg3/providers/Microsoft.Network/virtualNetworks/deployment-vnet3/subnets/xp


ALTER DATABASE iceberg_tutorial_db SET CATALOG = 'SNOWFLAKE';
ALTER DATABASE iceberg_tutorial_db SET EXTERNAL_VOLUME = 'dataset2';

SHOW PARAMETERS IN DATABASE iceberg_tutorial_db;

SHOW PARAMETERS IN schema PUBLIC;


--catalog intergration external

CREATE OR REPLACE CATALOG INTEGRATION demo_open_catalog_ext 
  CATALOG_SOURCE=POLARIS 
  TABLE_FORMAT=ICEBERG 
  REST_CONFIG = (
    CATALOG_URI = 'https://pxlrpte-vs41448open.snowflakecomputing.com/polaris/api/catalog' 
    CATALOG_NAME = 'externalcatalog'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH 
    OAUTH_CLIENT_ID = 'BAS4VTpOSofPOSHXuExJACSN8HE='
    OAUTH_CLIENT_SECRET = 'yoursecret' 
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:snowflake') 
  ) 
  ENABLED=TRUE;



ALTER DATABASE iceberg_tutorial_db SET CATALOG_SYNC = 'demo_open_catalog_ext';




  
-------internal_opencatalog_linked_db------------
CREATE EXTERNAL VOLUME opensnowflake
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'opensnowflake'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://sfoc.blob.core.windows.net/opensnowflake/'
        AZURE_TENANT_ID = '81652c07-f8c1-4cca-8372-9146285e400c'
      )
    );

DESC EXTERNAL VOLUME opensnowflake;

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('opensnowflake');

show external volumes;
drop external volume sfocex;

CREATE OR REPLACE CATALOG INTEGRATION opensnowflake
  CATALOG_SOURCE = POLARIS 
  TABLE_FORMAT = ICEBERG 
  --CATALOG_NAMESPACE = 'open_snowflake'
  REST_CONFIG = (
    CATALOG_URI = 'https://pxlrpte-vs41448open.snowflakecomputing.com/polaris/api/catalog' 
    CATALOG_NAME = 'open_snowflake'
    ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS
  )
    REST_AUTHENTICATION = (
    TYPE = OAUTH 
    OAUTH_CLIENT_ID = 'BAS4VTpOSofPOSHXuExJACSN8HE='
    OAUTH_CLIENT_SECRET = 'yoursecret' 
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:snowflake') 
  ) 
  ENABLED = TRUE;

show catalog integrations;
drop catalog integration opensnowflake;

CREATE DATABASE catalog_linked_db
  LINKED_CATALOG = (
    CATALOG = 'opensnowflake',
    NAMESPACE_MODE = FLATTEN_NESTED_NAMESPACE,
    NAMESPACE_FLATTEN_DELIMITER = '-'
    ALLOWED_NAMESPACES = ('catalog_linked_db')
  ),
  EXTERNAL_VOLUME = 'opensnowflake';

--CREATE SCHEMA catalog_linked_db;
--004506 (0A000): SQL compilation error: Only alphanumeric schema name is supported in a Catalog-Linked Database.

show tables;

CREATE SCHEMA cataloglinkedschema;

CREATE ICEBERG TABLE catalog_linked_table (
  first_name STRING,
  last_name STRING,
  amount INT,
  create_date DATE
)
TARGET_FILE_SIZE = '64MB';

INSERT INTO catalog_linked_table VALUES ('kun', 'xue', 100, '2025-05-06');

show schemas;
use schema catalog_linked_db;

CREATE ICEBERG TABLE catalog_linked_table_partition (
  first_name STRING,
  last_name STRING,
  amount INT,
  create_date DATE
)
partition by (first_name)
TARGET_FILE_SIZE = '64MB';


INSERT INTO catalog_linked_table VALUES ('kun', 'xue', 100, '2025-05-06');

select * from catalog_linked_table;

-------internal_catalog_external_managed_table------------


---create table at opencatalog by spark or other client

create database external_managed_db;
create schema external_managed_schema;

CREATE OR REPLACE ICEBERG TABLE external_managed_table
  EXTERNAL_VOLUME = 'opensnowflake'
  CATALOG = 'opensnowflake'
  CATALOG_NAMESPACE = 'external_managed_namespace'
  CATALOG_TABLE_NAME = 'external_managed_table';

INSERT INTO external_managed_table VALUES ('kun', 'xue', 100, '2025-05-06');
select * from external_managed_table;

-----test create failed managed external table in linked db------
CREATE OR REPLACE ICEBERG TABLE external_managed_table
  EXTERNAL_VOLUME = 'opensnowflake'
  CATALOG = 'opensnowflake'
  CATALOG_NAMESPACE = 'catalog_linked_db'
  CATALOG_TABLE_NAME = 'external_managed_table';

-------internal_catalog_linked_db_vended------------

CREATE OR REPLACE CATALOG INTEGRATION internal_catalog_vended
  CATALOG_SOURCE = POLARIS 
  TABLE_FORMAT = ICEBERG 
  CATALOG_NAMESPACE = 'internal'
  REST_CONFIG = (
    CATALOG_URI = 'https://pxlrpte-vs41448open.snowflakecomputing.com/polaris/api/catalog' 
    CATALOG_NAME = 'internal'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
    REST_AUTHENTICATION = (
    TYPE = OAUTH 
    OAUTH_CLIENT_ID = 'BAS4VTpOSofPOSHXuExJACSN8HE='
    OAUTH_CLIENT_SECRET = 'yoursecret' 
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:snowflake') 
  ) 
  ENABLED = TRUE;

CREATE DATABASE internal_catalog_linked_db_vended
  LINKED_CATALOG = (
    CATALOG = 'internal_catalog_vended'
  ),
  EXTERNAL_VOLUME = 'internalvol';
  

CREATE SCHEMA catalogLinkedNamespace;


CREATE ICEBERG TABLE catalog_linked_table_vended (
  first_name STRING,
  last_name STRING,
  amount INT
)
TARGET_FILE_SIZE = '64MB';
---PARTITION BY (first_name);

INSERT INTO catalog_linked_table VALUES ('kun', 'xue', 100, '2025-05-06');

select * from catalog_linked_table;




-----------
use iceberg_tutorial_db.public;

CREATE OR REPLACE ICEBERG TABLE test_write_table
  EXTERNAL_VOLUME = 'dataset2'
  CATALOG = 'internal_catalog'
  CATALOG_TABLE_NAME = 'test_table';

select * from test_write_table;

insert into test_write_table values (3);







SELECT SYSTEM$CATALOG_LINK_STATUS('my_catalog_linked_db');


drop table catalog_linked_table;
drop schema cataloglinkednamespace;
drop database my_catalog_linked_db;
drop database internal_catalog_linked_db;
drop CATALOG INTEGRATION internal_catalog;


use cataloglinkednamespace;

show tables;

create or replace ICEBERG TABLE INTERNAL_CATALOG_LINKED_DB.CATALOGLINKEDNAMESPACE."sales_events" 
 EXTERNAL_VOLUME = 'INTERNALVOL'
 CATALOG = 'INTERNAL_CATALOG'
 CATALOG_TABLE_NAME = 'sales_events'
 CATALOG_NAMESPACE = 'CATALOGLINKEDNAMESPACE';
 
select * from sales_events limit 10;


create database open_unity;


--------------------
CREATE EXTERNAL VOLUME unitysnowflake
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'unitysnowflake'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://datalakestgmf6bsc.blob.core.windows.net/unitysnowflake/'
        AZURE_TENANT_ID = '81652c07-f8c1-4cca-8372-9146285e400c'
      )
    );
CREATE EXTERNAL VOLUME unitysnowflake2
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'unitysnowflake2'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://datalakestgmf6bsc.dfs.core.windows.net/unitysnowflake/'
        AZURE_TENANT_ID = '81652c07-f8c1-4cca-8372-9146285e400c'
      )
    );
CREATE EXTERNAL VOLUME unityspark
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'unitysnowflake2'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://datalakestgmf6bsc.blob.core.windows.net/unityspark/'
        AZURE_TENANT_ID = '81652c07-f8c1-4cca-8372-9146285e400c'
      )
    );
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('unitysnowflake');
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('unitysnowflake2');
--Provided Azure storage endpoint 'datalakestgmf6bsc.dfs.core.windows.net' is currently not supported.
drop external volume unitysnowflake;




----------Unity Snowflake managed external table RO OK, RW KO--------------

CREATE OR REPLACE CATALOG INTEGRATION unityspark
CATALOG_SOURCE = ICEBERG_REST
TABLE_FORMAT = ICEBERG
--CATALOG_NAMESPACE = 'my_namespace'
REST_CONFIG = (
CATALOG_URI = 'https://adb-733469256132524.4.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest'
CATALOG_NAME = 'unityspark'
)
REST_AUTHENTICATION = (
TYPE = BEARER
BEARER_TOKEN = 'yoursecret'
)
ENABLED = TRUE;

create database unity_snowflake_db;
create schema unity_snowflake_schema;

CREATE OR REPLACE ICEBERG TABLE external_managed_table
  EXTERNAL_VOLUME = 'unityspark'
  CATALOG = 'unityspark'
  CATALOG_NAMESPACE = 'default'
  CATALOG_TABLE_NAME = 'unityspark';

show tables;

select * from external_managed_table;


drop table external_managed_table;

drop table external_managed_table;
INSERT INTO external_managed_table VALUES ('kun', 'xue', 100, '2025-05-06');
select * from external_managed_table;
---------------------------




----------Unity Snowflake catalog linked db KO--------------
show catalog integrations;
drop catalog integration unitysnowflake;

CREATE OR REPLACE CATALOG INTEGRATION unitysnowflake
CATALOG_SOURCE = ICEBERG_REST
TABLE_FORMAT = ICEBERG
--CATALOG_NAMESPACE = 'my_namespace'
REST_CONFIG = (
CATALOG_URI = 'https://adb-733469256132524.4.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest'
CATALOG_NAME = 'unity_snowflake'
ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
)
REST_AUTHENTICATION = (
TYPE = BEARER
BEARER_TOKEN = 'yoursecret'
)
ENABLED = TRUE;

CREATE DATABASE unity_catalog_linked_db
  LINKED_CATALOG = (
    CATALOG = 'unitysnowflake',
    NAMESPACE_MODE = FLATTEN_NESTED_NAMESPACE,
    NAMESPACE_FLATTEN_DELIMITER = '-'
    --ALLOWED_NAMESPACES = ('catalog_linked_db')
  );
--EXTERNAL_VOLUME = 'unitysnowflake2';
use schema "default";

CREATE ICEBERG TABLE catalog_linked_table (
  first_name STRING,
  last_name STRING,
  amount INT,
  create_date DATE
)
TARGET_FILE_SIZE = '64MB';

drop database unity_catalog_linked_db;



use database CATALOG_LINKED_DB;
use catalog_linked_db;


SELECT * FROM catalog_linked_table AT(TIMESTAMP => CAST('2025-09-29 18:36:00' AS TIMESTAMP_LTZ));

SELECT * FROM catalog_linked_table AT(OFFSET => -60*1800);


SELECT *
  FROM TABLE(
    INFORMATION_SCHEMA.ICEBERG_TABLE_FILES(
      TABLE_NAME => 'catalog_linked_table',
      AT => CAST('2025-09-30 15:36:00' AS TIMESTAMP_LTZ)
    )
  );
select * from catalog_linked_table;

INSERT INTO catalog_linked_table VALUES ('lily', 'bai', 200, '2025-08-12');
  
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.ICEBERG_TABLE_SNAPSHOT_REFRESH_HISTORY(
    TABLE_NAME => 'catalog_linked_table'
  ));

--093678 (0A000): SQL Compilation Error: This operation is not supported in a catalog-linked database.
CREATE SNAPSHOT POLICY hourly_snapshot_policy
  SCHEDULE = '60 MINUTE'
  EXPIRE_AFTER_DAYS = 90
  COMMENT = 'Hourly backups expire after 90 days';
  
--093678 (0A000): SQL Compilation Error: This operation is not supported in a catalog-linked database.
CREATE SNAPSHOT SET t1_snapshots FOR TABLE catalog_linked_table;
ALTER SNAPSHOT SET t1_snapshots ADD SNAPSHOT;

ALTER ICEBERG TABLE catalog_linked_table REFRESH;

ALTER ICEBERG TABLE catalog_linked_table ADD COLUMN mail STRING comment 'e-mail' ;
INSERT INTO catalog_linked_table VALUES ('kiki', 'liu', 500, '2025-12-05','kiki.liu@mail.com');

  

select * from catalog_linked_table;

UPDATE catalog_linked_table
  SET  amount = 400
  WHERE first_name = 'kun';

select * from catalog_linked_table_partition;

MERGE INTO catalog_linked_table_partition
  USING catalog_linked_table
  ON catalog_linked_table_partition.first_name = catalog_linked_table.first_name
  WHEN MATCHED THEN
  UPDATE SET catalog_linked_table_partition.amount = catalog_linked_table.amount;
