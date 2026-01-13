[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_cost_revenue_actuals_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_cost_revenue_actuals_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_cost_revenue_actuals_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_cost_revenue_actuals_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_cost_revenue_actuals_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_cost_revenue = let
  Source = Table.Combine({fact_cost_revenue_actuals_current, fact_cost_revenue_actuals_archive, fact_cost_revenue_plan_current, fact_cost_revenue_plan_archive, fact_cost_revenue_plan_future, fact_cost_revenue_forecast_current}),
  #"Added custom" = Table.AddColumn(Source, "Month Date", each [Posting Period] & "/" & [Year]),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "Offsetting Account and Name", each [Offsetting Account] & " - " & [Offsetting Account Name]),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Cost Center Number and Name", each [Cost Center] & " - " & [Cost Center Name]),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Partner Object Number and Name", each [Partner Object] & " - " & [Partner Object Name]),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Profit Center Number and Name", each [Profit Center] & " - " & [Profit Center Name]),
  #"Added custom 5" = Table.AddColumn(#"Added custom 4", "Payer Number and Name", each [Payer] & " - " & [Payer Name]),
  #"Added custom 6" = Table.AddColumn(#"Added custom 5", "Cost Element Number and Name", each [Cost Element] & " - " & [Cost Element Name]),
  #"Added custom 7" = Table.AddColumn(#"Added custom 6", "Material Number and Name", each [Material] & " - " & [Material Name]),
  #"Added custom 8" = Table.AddColumn(#"Added custom 7", "Vendor Key", each if (Text.Length(Text.AfterDelimiter([PO Vendor], "/", 1)) = 10) then [Company Code] & "-" & Text.AfterDelimiter([PO Vendor], "/", 1) else if Text.StartsWith([Offsetting Account],"K") = true then [Company Code] & "-" & Text.AfterDelimiter([Offsetting Account], "/", 0) else "#"),
  #"Merged queries" = Table.NestedJoin(#"Added custom 8", {"Cost Element"}, dim_cost_element, {"Cost Element"}, "dim_cost_element", JoinKind.LeftOuter),
  #"Expanded dim_cost_element" = Table.ExpandTableColumn(#"Merged queries", "dim_cost_element", {"Type"}, {"Type"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded dim_cost_element", {{"Type", "Cost Element Type"}}),
  #"Added custom 9" = Table.AddColumn(#"Renamed columns", "Material Key", each if (([Material] <> "#") and ([Material] <> "")) then [Company Code] & "-" & [Material] else ""),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 9", {{"Month Date", type date}, {"Amount", Currency.Type}, {"Quantity", Currency.Type}, {"Company Code", type text}, {"CO Document Number", type text}, {"CO Doc Line Item Txt", type text}, {"Cost Element", type text}, {"Cost Element Name", type text}, {"PO Vendor", type text}, {"Material", type text}, {"Material Name", type text}, {"PO Item Text", type text}, {"Company ID", type text}, {"Doc.number", type text}, {"Customer Cost Center", type text}, {"Offsetting Account", type text}, {"Offsetting Account Name", type text}, {"Origin Object", type text}, {"Origin Object Name", type text}, {"Payer", type text}, {"Payer Name", type text}, {"Reversal ref. number", type text}, {"WBS Element", type text}, {"User Name", type text}, {"Currency", type text}, {"File Name", type text}, {"Refresh Date", type datetime}, {"Year", type text}, {"Row Type", type text}, {"Purchasing Document", type text}, {"Posting Period", type text}, {"Fiscal Year", type text}, {"Partner Company Code", type text}, {"Partner Object", type text}, {"Partner Object Name", type text}, {"Profit Center", type text}, {"Profit Center Name", type text}, {"Cost Center", type text}, {"Cost Center Name", type text}, {"Company Code Name", type text}, {"Person responsible", type text}, {"WBS Element Name", type text}, {"Customer Company Code", type text}, {"Customer Object", type text}, {"Version", type text}, {"Offsetting Account and Name", type text}, {"Cost Center Number and Name", type text}, {"Partner Object Number and Name", type text}, {"Profit Center Number and Name", type text}, {"Payer Number and Name", type text}, {"Cost Element Number and Name", type text}, {"Material Number and Name", type text}, {"Vendor Key", type text}, {"Material Key", type text}})
in
  #"Changed column type";
shared fact_cost_revenue_forecast_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_cost_revenue_forecast_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_cost_revenue_plan_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_cost_revenue_plan_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_cost_revenue_plan_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_cost_revenue_plan_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_cost_revenue_plan_future = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_cost_revenue_plan_future", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_cost_revenue_actuals_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_cost_revenue", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_cost_element = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dim_cost_element_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
