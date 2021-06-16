with model_executions as (

    select * from {{ ref('fct_dbt__model_executions') }}

),

exposures_record as (

    select * from {{ ref('dim_dbt__exposures') }}

),


model_updates as (

    select 
        max(query_completed_at) as latest_update,
        node_id
    from model_executions
    group by node_id

),

exposures_latest as (

    select
        artifact_generated_at as latest_generation,
        node_id,
        name,
        type,
        owner,
        maturity,
        package_name,
        depends_on_nodes,
        depends_on_sources
    from exposures_record
    where artifact_generated_at = (select max(artifact_generated_at) from exposures_record)

),

exposures_updates as (

    select 
        e.latest_generation,
        e.node_id,
        e.name, 
        e.type,
        e.owner,
        e.maturity,
        e.package_name,
        e.depends_on_nodes,
        e.depends_on_sources,
        latest_update as feed_latest_update
    from exposures_latest e
    left join model_updates m
      on m.node_id = e.output_feeds

)

select * from exposures_updates