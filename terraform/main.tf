# Snowflake platform for the cost showback: warehouses per workload, a role per team, data champion and service users.
# Applied as FINOPS_SVC (key-pair) with role FINOPS_ADMIN, created once by snowflake/bootstrap.sql.

provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = "FINOPS_SVC"
  role              = "FINOPS_ADMIN"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(pathexpand(var.private_key_path))
}

locals {
  # Workload warehouses. BI_WH keeps a 10-minute auto-suspend on purpose: it is the setting a dashboard team
  # often leaves in place, and the showback should surface what it costs.
  warehouses = {
    TRANSFORM_WH = { auto_suspend = 60, comment = "Scheduled ELT" }
    BI_WH        = { auto_suspend = 600, comment = "Dashboards (BI service account)" }
    ADHOC_WH     = { auto_suspend = 300, comment = "Data champions' ad hoc queries" }
  }

  teams = ["FINANCE", "RISK", "MARKETING", "OPERATIONS"]

  champions = { for i in range(1, 9) : format("CHAMPION_%02d", i) => local.teams[floor((i - 1) / 2)] }

  service_users = {
    LOOKER_SVC    = { role = "BI_READER", warehouse = "BI_WH" }
    TRANSFORM_SVC = { role = "TRANSFORMER", warehouse = "TRANSFORM_WH" }
  }
}

resource "snowflake_warehouse" "wh" {
  for_each                            = local.warehouses
  name                                = each.key
  warehouse_size                      = "XSMALL"
  auto_suspend                        = each.value.auto_suspend
  auto_resume                         = true
  initially_suspended                 = true
  statement_timeout_in_seconds        = 600
  statement_queued_timeout_in_seconds = 300
  comment                             = each.value.comment
}

resource "snowflake_database" "analytics" {
  name    = "ANALYTICS"
  comment = "Workload data for the cost showback demo"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.analytics.name
  name     = "MARTS"
}

# Roles: one per team, plus the BI and transform service roles.
resource "snowflake_account_role" "team" {
  for_each = toset(concat([for t in local.teams : "${t}_ANALYST"], ["BI_READER", "TRANSFORMER"]))
  name     = each.key
}

resource "snowflake_grant_account_role" "roles_to_admin" {
  for_each         = snowflake_account_role.team
  role_name        = each.value.name
  parent_role_name = "FINOPS_ADMIN"
}

resource "snowflake_grant_privileges_to_account_role" "db_usage" {
  for_each          = snowflake_account_role.team
  account_role_name = each.value.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.analytics.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  for_each          = snowflake_account_role.team
  account_role_name = each.value.name
  privileges        = each.value.name == "TRANSFORMER" ? ["USAGE", "CREATE TABLE"] : ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.analytics.name}\".\"${snowflake_schema.marts.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "read_future_tables" {
  for_each          = snowflake_account_role.team
  account_role_name = each.value.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.analytics.name}\".\"${snowflake_schema.marts.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "read_all_tables" {
  for_each          = snowflake_account_role.team
  account_role_name = each.value.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.analytics.name}\".\"${snowflake_schema.marts.name}\""
    }
  }
}

# Warehouse access: champions use ADHOC_WH; service roles use their own warehouse.
resource "snowflake_grant_privileges_to_account_role" "wh_usage" {
  for_each = merge(
    { for t in local.teams : "${t}_ANALYST" => "ADHOC_WH" },
    { BI_READER = "BI_WH", TRANSFORMER = "TRANSFORM_WH" },
  )
  account_role_name = snowflake_account_role.team[each.key].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.wh[each.value].name
  }
}

# Users: key-pair service users only, no passwords. The scripted workload (workload.py) signs in as each of them.
resource "snowflake_service_user" "champion" {
  for_each          = local.champions
  name              = each.key
  default_role      = "${each.value}_ANALYST"
  default_warehouse = "ADHOC_WH"
  rsa_public_key    = var.workload_public_key
  comment           = "Data champion (scripted workload), team ${lower(each.value)}"
}

resource "snowflake_service_user" "svc" {
  for_each          = local.service_users
  name              = each.key
  default_role      = each.value.role
  default_warehouse = each.value.warehouse
  rsa_public_key    = var.workload_public_key
}

resource "snowflake_grant_account_role" "champion_role" {
  for_each  = local.champions
  role_name = snowflake_account_role.team["${each.value}_ANALYST"].name
  user_name = snowflake_service_user.champion[each.key].name
}

resource "snowflake_grant_account_role" "svc_role" {
  for_each  = local.service_users
  role_name = snowflake_account_role.team[each.value.role].name
  user_name = snowflake_service_user.svc[each.key].name
}

output "user_team" {
  description = "Feeds config/user_team.csv for the showback"
  value = merge(
    { for u, t in local.champions : u => lower(t) },
    { LOOKER_SVC = "bi", TRANSFORM_SVC = "platform", FINOPS_SVC = "platform" },
  )
}
