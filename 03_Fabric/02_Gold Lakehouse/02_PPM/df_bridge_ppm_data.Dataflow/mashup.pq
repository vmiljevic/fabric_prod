[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_employee_internal = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_employee_internal", ItemKind = "Table"]}[Data],
  #"Added custom" = Table.AddColumn(#"Navigation 2", "User Key", each [Company Code] & "-" & Date.ToText([Month Date], [Format="MM", Culture="de-DE"]) & "-" & Date.ToText([Month Date], [Format="yyyy", Culture="de-DE"]) & "-" & [User ID]),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "Employee Key New", each [Company Code] & "-" & [Month] & "-" & [Year] & "-" & [HR ID]),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 1", {"Employee Key"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Employee Key New", "Employee Key"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"User Key", type text}, {"Employee Key", type text}})
in
  #"Changed column type";
shared fact_employee_internals_sumarized = let
  Source = fact_employee_internal,
  #"Removed other columns" = Table.SelectColumns(Source, {"Employee Name", "Employee Key", "Company Code", "Month Date"}),
  #"Added custom" = Table.AddColumn(#"Removed other columns", "Resource Type", each "Internal Resources"),
  #"Removed duplicates" = Table.Distinct(#"Added custom", {"Employee Key"})
in
  #"Removed duplicates";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_employee_all_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_employee_all = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_ppm_time_cards", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Resource Type] <> "Internal Resources")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"Resource Type", "Employee Key", "ExportFor", "Booked By","Month Date"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"Employee Key"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed duplicates", {{"Booked By", "Employee Name"}, {"ExportFor", "Company Code"}}),
  #"Appended query" = Table.Combine({#"Renamed columns", fact_employee_internals_sumarized}),
  #"Changed column type" = Table.TransformColumnTypes(#"Appended query", {{"Resource Type", type text}}),
  #"Uppercased text" = Table.TransformColumns(#"Changed column type", {{"Employee Key", each Text.Upper(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Uppercased text", {"Employee Key"}, fact_ppm_time_cards_sumarized, {"Employee Key"}, "fact_ppm_time_cards_sumarized", JoinKind.LeftOuter),
  #"Expanded fact_ppm_time_cards_sumarized" = Table.ExpandTableColumn(#"Merged queries", "fact_ppm_time_cards_sumarized", {"Booking Category"}, {"Booking Category"}),
  #"Added custom" = Table.AddColumn(#"Expanded fact_ppm_time_cards_sumarized", "Booked in PPM", each if [Booking Category] = null then "No" else "Yes"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"Booked in PPM", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"Booking Category"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed columns", each [Employee Key] <> null)
in
  #"Filtered rows 1";
shared fact_employee_all_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_employee_all", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_ppm_time_cards_sumarized = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_ppm_time_cards", ItemKind = "Table"]}[Data],
  #"Removed other columns 1" = Table.SelectColumns(#"Navigation 2", {"Employee Key", "Booking Category"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed other columns 1", each ([Booking Category] <> "Absence")),
  #"Removed duplicates 1" = Table.Distinct(#"Filtered rows 1", {"Employee Key"}),
  #"Filtered rows" = Table.SelectRows(#"Removed duplicates 1", each ([Booking Category] <> "Absence"))
in
  #"Filtered rows";
