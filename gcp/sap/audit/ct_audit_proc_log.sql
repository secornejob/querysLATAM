drop table `sp-ed-opssupplyanalytics-dev.supply_sap.audit_proc_log`;
create table `sp-ed-opssupplyanalytics-dev.supply_sap.audit_proc_log` (
  proc_name string not null,
  proc_type string not null,
  proc_description string not null,
  proc_target_table string,
  proc_datetime_start datetime not null,
  proc_datetime_end datetime not null,
  proc_records int64,
  proc_error_message string,
)