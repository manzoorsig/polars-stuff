select app.id, app.name,app.description, app.created_at, app.in_trash, 
      proj.id, proj.application_id, proj.name, proj.description, 
      proj.created_at, proj.in_trash , 
      br.id, br.name, br.description,br.in_trash, br.created_at
from `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on app.id = br.organization_id and proj.id = br.project_id
order by app.id;
--31867 rows

select count(*) from 
( 
select app.id, app.name, app.created_at, app.in_trash, proj.id, proj.application_id, proj.name, proj.created_at, proj.in_trash , br.id, br.name, br.in_trash, br.created_at
from `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on app.id = br.organization_id and proj.id = br.project_id
order by app.id
)