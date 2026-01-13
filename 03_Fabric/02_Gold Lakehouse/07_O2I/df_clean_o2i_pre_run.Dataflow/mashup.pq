[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_o2i_pre_run_cost_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_o2i_pre_run_cost_current", ItemKind = "Table"]}[Data],
  #"Added custom" = Table.AddColumn(#"Navigation 2", "Type", each "Cost"),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Type", type text}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"Payer"}, dim_company, {"Debtor Number"}, "dim_company", JoinKind.LeftOuter),
  #"Expanded dim_company" = Table.ExpandTableColumn(#"Merged queries", "dim_company", {"Company Code"}, {"Company Code"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded dim_company", {{"Company Code", "Export For"}})
in
  #"Renamed columns";
shared fact_o2i_pre_run_cost_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_o2i_pre_run_cost_archive", ItemKind = "Table"]}[Data],
  #"Added custom" = Table.AddColumn(#"Navigation 2", "Type", each "Cost"),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Type", type text}}),
  #"Merged queries" = Table.NestedJoin(#"Changed column type", {"Payer"}, dim_company, {"Debtor Number"}, "dim_company", JoinKind.LeftOuter),
  #"Expanded dim_company" = Table.ExpandTableColumn(#"Merged queries", "dim_company", {"Company Code"}, {"Company Code"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded dim_company", {{"Company Code", "Export For"}})
in
  #"Renamed columns";
shared fact_o2i_pre_run_revenue_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_o2i_pre_run_revenue_current", ItemKind = "Table"]}[Data],
  #"Added custom" = Table.AddColumn(#"Navigation 2", "Type", each "Revenue"),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Type", type text}}),
  #"Added custom 1" = Table.AddColumn(#"Changed column type", "Export For", each [Sales Organization])
in
  #"Added custom 1";
shared fact_o2i_pre_run_revenue_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_o2i_pre_run_revenue_archive", ItemKind = "Table"]}[Data],
  #"Added custom" = Table.AddColumn(#"Navigation 2", "Type", each "Revenue"),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Type", type text}}),
  #"Added custom 1" = Table.AddColumn(#"Changed column type", "Export For", each [Sales Organization])
in
  #"Added custom 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_o2i_pre_run_current_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_o2i_pre_run = let
  Source = Table.Combine({fact_o2i_pre_run_revenue_current, fact_o2i_pre_run_cost_current,fact_o2i_pre_run_revenue_archive,fact_o2i_pre_run_cost_archive}),
  #"Filtered rows" = Table.SelectRows(Source, each ([#"Cal. Year/Month of Load"] <> null)),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"SAP Refresh Time"}),
  #"Added custom" = Table.AddColumn(#"Removed columns", "Price Key", each [Sales Organization] & "-" & [Material]),
  #"Merged queries" = Table.NestedJoin(#"Added custom", {"Price Key"}, dim_material_price, {"Price Key"}, "dim_material_price", JoinKind.LeftOuter),
  #"Expanded dim_material_price" = Table.ExpandTableColumn(#"Merged queries", "dim_material_price", {"Currency", "Price Outside VAT Group", "Price Inside VAT Group","Price Type"}, {"Currency", "Price Outside VAT Group", "Price Inside VAT Group", "Price Type"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded dim_material_price", {"Payer"}, dim_payer, {"Payer"}, "dim_payer", JoinKind.LeftOuter),
  #"Expanded dim_payer" = Table.ExpandTableColumn(#"Merged queries 1", "dim_payer", {"Payer Name", "Country"}, {"Payer Name", "Country"}),
  #"Added custom 1" = Table.AddColumn(#"Expanded dim_payer", "Payer Number and Name", each [Payer] & " - " & [Payer Name]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 1", {{"Quantity", Currency.Type}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type", "Amount excl. VAT", each [Quantity] * [Price Outside VAT Group]),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Amount incl. VAT", each [Quantity] * [Price Inside VAT Group]),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Amount", each if Text.Start([Sales Organization],2) = [Country] then [Quantity] * [Price Inside VAT Group] else [Quantity] * [Price Outside VAT Group]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 4", {{"Amount excl. VAT", Currency.Type}, {"Amount incl. VAT", Currency.Type}, {"Amount", Currency.Type}}),
  #"Added custom 5" = Table.AddColumn(#"Changed column type 1", "SI Status Name", each if [SI Status] = "10" then "10 - Service Active"
        else if [SI Status] = "9" then "9 - Service in preparation, Not yet active"
        else if [SI Status] = "7" then "7 - Service Decommissioned"
        else if [SI Status] = "#" then "# - Not available"
        else "# - Not available"),
  #"Merged queries 2" = Table.NestedJoin(#"Added custom 5", {"Quote ID"}, fact_o2i_pre_run_status, {"Quote ID"}, "fact_o2i_pre_run_status", JoinKind.LeftOuter),
  #"Expanded fact_o2i_pre_run_status_current" = Table.ExpandTableColumn(#"Merged queries 2", "fact_o2i_pre_run_status", {"CAP Error Message"}, {"CAP Error Message"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded fact_o2i_pre_run_status_current", {{"CAP Error Message", "Error Message"}}),
  #"Added custom 6" = Table.AddColumn(#"Renamed columns", "Status", each if [Error Message] <> null then "Not Processed - Error found" else "Processed"),
  #"Added custom 8" = Table.AddColumn(#"Added custom 6", "Service ID Link", each if [Service Instance ID]<>"#" then "https://aztech.service-now.com/nav_to.do?uri=%2Fu_cmdb_ci_si_billing_plan.do?sysparm_query=u_number%3D" & [Service Instance ID] else "#"),
  #"Added custom 9" = Table.AddColumn(#"Added custom 8", "Quote Line ID", each if Text.StartsWith(Text.End([SI Name],10),"QLN") then Text.End([SI Name],10) else ""),
  #"Added custom 10" = Table.AddColumn(#"Added custom 9", "Quote Line ID Link", each if [Quote Line ID]<>"" then "https://aztech.service-now.com/nav_to.do?uri=%2Fu_csm_quote_line_item.do?sysparm_query=order_line_id%3D" & [Quote Line ID] else ""),
  #"Added custom 11" = Table.AddColumn(#"Added custom 10", "Quote ID Link", each if [Quote ID]<>"#" then "https://aztech.service-now.com/nav_to.do?uri=%2Fu_csm_quote.do%3Fsysparm_query=number=" & [Quote ID] else "#"),
  #"Renamed columns 1" = Table.RenameColumns(#"Added custom 11", {{"Service Date", "Month Date"}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Renamed columns 1", {{"SI Status Name", type text}, {"Error Message", type text}, {"Status", type text}, {"Service ID Link", type text}, {"Quote Line ID", type text}, {"Quote Line ID Link", type text}, {"Quote ID Link", type text}, {"Load Date", type date}, {"Month Date", type date}, {"Timestamp Load", type datetime}, {"Payer Number and Name", type text}, {"Price Key", type text}, {"Export For", type text}, {"Cal. Year/Month of Load", type date}}, "de-DE"),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type 3", {{"Cal. Year/Month of Load", "Month of Load"}, {"Price Key", "Material Key"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 2", {{"Refresh Date", type datetime}})
in
  #"Changed column type 2";
shared dim_company = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_company", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_material_price = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_material_price", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_payer = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_payer", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_o2i_pre_run_status_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_o2i_status_current", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Quote ID] <> "#" and [Quote ID] <> "0")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Quote ID"})
in
  #"Removed duplicates";
shared fact_o2i_pre_run_current_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_o2i_pre_run", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_o2i_pre_run_status_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_o2i_status_archive", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Quote ID] <> "#" and [Quote ID] <> "0")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Quote ID"})
in
  #"Removed duplicates";
shared fact_o2i_pre_run_status = let
  Source = Table.Combine({fact_o2i_pre_run_status_archive, fact_o2i_pre_run_status_current})
in
  Source;
