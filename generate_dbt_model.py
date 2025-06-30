import argparse
from dbt.config.profile import read_profile
from dbt.config.renderer import ProfileRenderer
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy import inspect
from datetime import datetime
import os
import yaml

def get_connection_url_from_profile(profile_name, target_name):
    """Get SQLAlchemy connection URL from dbt profile"""
    # Read the raw profile data
    raw_profiles = read_profile( os.path.expanduser('~/.dbt'))
    
    if profile_name not in raw_profiles:
        raise ValueError(f"Profile '{profile_name}' not found in profiles.yml")
    
    profile_data = raw_profiles[profile_name]
    
    if target_name not in profile_data['outputs']:
        raise ValueError(f"Target '{target_name}' not found in profile '{profile_name}'")
    
    # Render the profile (handles environment variables)
    renderer = ProfileRenderer()
    credentials = renderer.render_data(profile_data['outputs'][target_name])
    
    # Map dbt profile types to SQLAlchemy dialects
    type_map = {
        'postgres': 'postgresql',
        'snowflake': 'snowflake',
        'redshift': 'redshift',
        'bigquery': 'bigquery',
        'sqlserver': 'mssql',
        'databricks': 'databricks'
    }
    
    dialect = type_map.get(credentials['type'], credentials['type'])
    
    if dialect == 'snowflake':
        account = credentials['account'].replace('.snowflakecomputing.com', '')
        return (
            f"snowflake://{credentials['user']}:{credentials['password']}"
            f"@{account}/{credentials['database']}/{credentials['schema']}"
            f"?warehouse={credentials['warehouse']}&role={credentials.get('role', '')}"
        )
    elif dialect == 'postgresql':
        return URL.create(
            drivername='postgresql',
            username=credentials['user'],
            password=credentials['password'],
            host=credentials['host'],
            port=credentials['port'],
            database=credentials['dbname']
        )
    elif dialect == 'bigquery':
        return f"bigquery://{credentials['project']}/{credentials['dataset']}"
    else:
        # Generic connection string for other databases
        return f"{dialect}://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"

def generate_dbt_model(profile_name, target_name, schema_name, table_name, output_dir, incremental_strategy,distributed_by ):
    try:
        # Get connection URL from dbt profile
        connection_url = get_connection_url_from_profile(profile_name, target_name)
        engine = create_engine(connection_url)
        
        inspector = inspect(engine)
        
        # Get columns information
        columns = inspector.get_columns(table_name, schema=schema_name)
        if incremental_strategy in ("delta_merge", "delta_upsert", "delta","partitions"):
            print('Enter delta field:')
            delta_field = input()
            add_ext_target = f"""load_method = 'pxf',
        extraction_type = 'DELTA',
        model_target = '{table_name}',
        delta_field = '{delta_field}',"""
            connection_string = f"'{schema_name}.{table_name}?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://'~ var('external_db_address')~'/postgres&USER='~ var('external_db_user')~'&PASS='~var('external_db_password')"
            set_connection_string = f"""{{% set connect_string = {connection_string} %}}"""         
        else:
            connection_string=f"""LOCATION ('pxf://{schema_name}.{table_name}?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://{{{{ var('external_db_address') }}}}/postgres&USER={{{{ var('external_db_user') }}}}&PASS={{{{ var('external_db_password') }}}}') ON ALL 
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' ) 
ENCODING 'UTF8'"""
            set_connection_string = f"""
{{% set connect_string %}}
{connection_string}
{{% endset %}}
            """
            add_ext_target = f"""extraction_type = 'FULL',"""

        
        # Generate SELECT clause
        columns_ext =  ",\n".join([
            f"{column['name']} {str(column.get('type', 'unknown'))}"
            for column in columns
        ])
        columns_select =  ",\n".join([
            f"{column['name']} as {column['name']}"
            for column in columns
        ])        
        # Generate the model content for external table
        model_content = f"""-- dbt model generated from table {schema_name}.{table_name}
-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{set_connection_string}


{{{{ 
    config(
        materialized='external_table',
        connect_string=connect_string,
        columns="
{columns_ext}
        ",
        {add_ext_target}
        tags=["{table_name}","generated"]  
    )
}}}}
        """

        # Write to file
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"ext_{table_name}.sql")
        
        with open(output_file, 'w') as f:
            f.write(model_content)
        
        print(f"dbt external table model generated at: {output_file}")

        if distributed_by == 'replicated':
            distribution_logic = 'distributed_replicated = true'
        else:
            distribution_logic = f"distributed_by='{distributed_by}'"
        # Generate the model content
        if incremental_strategy == 'full':
            model_content = f"""-- dbt model generated from table {schema_name}.{table_name}
-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{{{{ 
    config(
        materialized='proplum',
        incremental_strategy='{incremental_strategy}',
        tags=["{table_name}","generated"],
        {distribution_logic}
    )
}}}}

SELECT 
{columns_select}
FROM {{{{ ref('ext_{table_name}') }}}}
            """

        elif incremental_strategy == 'delta_merge':
            print('Enter partition field:')
            partition_field = input()
            print("Enter merge keys (separated by ','):")
            merge_keys = input()
            formatted_merge_keys = ",".join(f"'{key.strip()}'" for key in merge_keys.split(","))
            model_content = f"""-- dbt model generated from table {schema_name}.{table_name}
-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{{% set raw_partition %}}
PARTITION BY RANGE({partition_field}) 
    (
    DEFAULT PARTITION def
    );
{{% endset%}}

{{% set fields_string %}}
{columns_ext}
{{% endset%}}

{{{{ 
    config(
        materialized='proplum',
        incremental_strategy='{incremental_strategy}',
        tags=["{table_name}","generated"],
        {distribution_logic},
        merge_keys=[{formatted_merge_keys}],
        delta_field='{delta_field}',
        raw_partition=raw_partition,
        fields_string=fields_string
    )
}}}}

SELECT 
{columns_select}
FROM {{{{ ref('ext_{table_name}') }}}}
            """

        elif incremental_strategy == 'delta_upsert':
            print("Enter merge keys (separated by ','):")
            merge_keys = input()
            formatted_merge_keys = ",".join(f"'{key.strip()}'" for key in merge_keys.split(","))
            model_content = f"""-- dbt model generated from table {schema_name}.{table_name}
-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{{{{ 
    config(
        materialized='proplum',
        incremental_strategy='{incremental_strategy}',
        tags=["{table_name}","generated"],
        {distribution_logic},
        merge_keys=[{formatted_merge_keys}],
        delta_field='{delta_field}'
    )
}}}}

SELECT 
{columns_select}
FROM {{{{ ref('ext_{table_name}') }}}}
            """

        elif incremental_strategy == 'delta':
            print('Enter partition field:')
            partition_field = input()
            print("Enter partiton start period (YYYY-MM-DD):")
            partition_start = input()
            print("Enter partiton end period (YYYY-MM-DD):")
            partition_end = input()   
            print("Enter partiton period ('1 year', '1 month', '1 day'):")
            partition_period = input()             
            model_content = f"""-- dbt model generated from table {schema_name}.{table_name}
-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{{% set raw_partition %}}
PARTITION BY RANGE({partition_field}) 
    (
        START ('{partition_start}'::timestamp) INCLUSIVE
        END ('{partition_end}'::timestamp) EXCLUSIVE
        EVERY (INTERVAL '{partition_period}'),
        DEFAULT PARTITION extra
    );
{{% endset%}}

{{% set fields_string %}}
{columns_ext}
{{% endset%}}


{{{{ 
    config(
        materialized='proplum',
        incremental_strategy='{incremental_strategy}',
        tags=["{table_name}","generated"],
        {distribution_logic},
        raw_partition=raw_partition,
        fields_string=fields_string,
        delta_field='{delta_field}'
    )
}}}}

SELECT 
{columns_select}
FROM {{{{ ref('ext_{table_name}') }}}}
"""

        elif incremental_strategy == 'partitions':
            print('Enter partition field:')
            partition_field = input()
            print("Enter partiton start period (YYYY-MM-DD):")
            partition_start = input()
            print("Enter partiton end period (YYYY-MM-DD):")
            partition_end = input()   
            print("Enter partiton period ('1 year', '1 month', '1 day'):")
            partition_period = input() 
            print("merge partitions? (true or false):")
            merge_partitions = input()
            if merge_partitions:
                print("Enter merge keys (separated by ','):")
                merge_keys = input()
                formatted_merge_keys = ",".join(f"'{key.strip()}'" for key in merge_keys.split(","))
                merge_formated=f"""merge_partitions=true,
 merge_keys=[{formatted_merge_keys}],"""
            else:
                merge_formated=f"""merge_partitions=false,"""      
            model_content = f"""-- dbt model generated from table {schema_name}.{table_name}
-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{{% set raw_partition %}}
PARTITION BY RANGE({partition_field}) 
    (
        START ('{partition_start}'::timestamp) INCLUSIVE
        END ('{partition_end}'::timestamp) EXCLUSIVE
        EVERY (INTERVAL '{partition_period}'),
        DEFAULT PARTITION extra
    );
{{% endset%}}

{{% set fields_string %}}
{columns_ext}
{{% endset%}}


{{{{ 
    config(
        materialized='proplum',
        incremental_strategy='{incremental_strategy}',
        tags=["{table_name}","generated"],
        {distribution_logic},
        raw_partition=raw_partition,
        fields_string=fields_string,
        {merge_formated}
        delta_field='{delta_field}'
    )
}}}}

SELECT 
{columns_select}
FROM {{{{ ref('ext_{table_name}') }}}}
"""
            
        # Write to file
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{table_name}.sql")
        
        with open(output_file, 'w') as f:
            f.write(model_content)
        
        print(f"dbt model generated at: {output_file}")

        return output_file
        
    except Exception as e:
        print(f"Error generating model: {e}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()

def main():
    parser = argparse.ArgumentParser(description='Generate dbt model from database table using dbt profile')
    parser.add_argument('--profile', default=None, help='dbt profile name')
    parser.add_argument('--target', required=True, help='dbt target name')
    parser.add_argument('--schema', required=True, help='Schema name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--output-dir', default='models', help='Output directory for dbt models')
    parser.add_argument('--incremental-strategy', default='full', help='Method to load data into table')
    parser.add_argument('--distributed-by', default='replicated', help='Field to use for distrubution in db')
    #parser.add_argument('--delta-field',  nargs='?',const=None, default=None, help='Field in table used for delta load')
    
    import sys
    print("Raw arguments:", sys.argv)

    args = parser.parse_args()
    
    generate_dbt_model(
        profile_name=args.profile,
        target_name=args.target,
        schema_name=args.schema,
        table_name=args.table,
        output_dir=args.output_dir,
        incremental_strategy=args.incremental_strategy,
        distributed_by=args.distributed_by
        #delta_field=args.delta_field
    )

if __name__ == '__main__':
    main()