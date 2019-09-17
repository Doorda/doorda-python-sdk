# Example ETL Script (AWS Glue Job)

Example Glue Script to extract data from DoordaHost into Parquet format on s3.  

1) `sample_glue_script_all.py`  
    Extracts all tables from indicated Catalog into S3  
    
2) `sample_glue_script_single.py`   
    Extracts table indicated in `TABLENAME` parameter into S3  

## Requirements

- pyspark==2.4  # Current version installed on AWS Glue job  
- DoordaHost JDBC driver [here](https://github.com/Doorda/drivers-cli/raw/master/doordahost/jdbc/DoordaHostJDBC_309d.jar)
  
### Usage

1) Click on `Add job` in AWS Glue  

    **Parameters**
    - `Name`: {Provide name for job}
    - `IAM role`: {Select iam role}
        - PUT/GET/DELETE object from bucket
    - `Type`: Spark  
    - `Glue version`: Spark 2.4, Python 3 (Glue version 1.0)  
    - `This job runs`: A new script to be authored by you  
    - `Script file name`: {Provide name for script}
    - `S3 path where the script is stored`: {provide path to store authored script}
    - `Temporary directory`: {provide path to store temporary results}


    ![](assets/glue_part_1.png)


    - `Dependent jars path`: {provide path to bucket on S3 with DoordaHost JDBC driver file}
    - `Maximum capacity`: {Adjust depending on needs}
    - `Max concurrency`: {Adjust depending on needs}

    ![](assets/glue_part_2.png)


    - `Job parameters`:

    | key  | value |
    |---|---|
    |  --USERNAME |  username |
    | --PASSWORD  | password  |
    | --CATALOG  |  catalog_name |
    |  --SCHEMA | schema_name  |
    | --TABLENAME  | table_name  |
    | --FILE_OUTPUT_DIRECTORY  | `s3://<bucket>/<path to store parquet files>/`  |

    **NOTE**: This method of passing account information/credentials is useful for testing,
    but will be expose to anyone with access to this job. It is recommended to
    use AWS Systems Manager (Parameter Store) to store and retrieve these details instead.

    ![](assets/glue_part_3.png)


2) Copy `sample_glue_script_all.py` or `sample_glue_script_single.py` into Script Editor and amend as needed.