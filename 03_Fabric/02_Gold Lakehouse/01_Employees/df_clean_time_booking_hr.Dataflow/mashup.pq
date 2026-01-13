[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_time_recording_hr_ce = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_time_recording_hr_ce", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Working Hours", Currency.Type}, {"Overtime Hours", Currency.Type}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "Booked in HR", each ([Working Hours] + [Overtime Hours])/8),
  #"Split column by delimiter" = Table.SplitColumn(#"Added custom", "Month", Splitter.SplitTextByDelimiter("-"), {"Month.1", "Month.2"}),
  #"Added custom 1" = Table.AddColumn(#"Split column by delimiter", "Month Date", each [Month.2] & "-" & [Month.1]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"Month Date", type date}, {"Month.1", Int64.Type}, {"Month.2", Int64.Type}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"Refresh Date", "Working Hours", "Overtime Hours", "Month.1", "Month.2", "Year", "File Name"}),
  #"Merged queries" = Table.NestedJoin(#"Removed columns", {"HR ID"}, fact_employee_internal_ce_unique, {"HR ID"}, "fact_employee_internal_ce_unique", JoinKind.LeftOuter),
  #"Expanded fact_employee_internal_ce_unique" = Table.ExpandTableColumn(#"Merged queries", "fact_employee_internal_ce_unique", {"Last Name", "First Name"}, {"Last Name", "First Name"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Expanded fact_employee_internal_ce_unique", {{"Booked in HR", Currency.Type}})
in
  #"Changed column type 2";
shared fact_time_recording_hr_corr_at45 = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_time_recording_hr_corr_at45", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"Booked in SAP", "Booked in HR"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Booked in HR", Currency.Type}, {"Month Date", type date}})
in
  #"Changed column type";
shared fact_time_recording_hr_tele_at45 = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_time_recording_hr_tele_at45", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"Booked in SAP", "Booked in HR"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Booked in HR", Currency.Type}, {"Month Date", type date}})
in
  #"Changed column type";
shared fact_employee_internal_ce_unique = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_employee_internal", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [Company Code] <> "AT45" and [Company Code] <> "DE44"),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"HR ID"})
in
  #"Removed duplicates";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_time_recording_hr_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_time_recording_hr = let
  Source = Table.Combine({fact_time_recording_hr_ce, fact_time_recording_hr_corr_at45, fact_time_recording_hr_tele_at45}),
  #"Added custom" = Table.AddColumn(Source, "Employee Key", each [Company Code] & "-" & Date.ToText([Month Date], [Format="MM", Culture="de-DE"]) & "-" & Date.ToText([Month Date], [Format="yyyy", Culture="de-DE"]) & "-" & [HR ID]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Employee Key", type text}})
in
  #"Changed column type";
shared fact_time_recording_hr_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_time_recording_hr", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
