[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_service_spend_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_service_spend_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_service_spend_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_service_spend_current", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Quantity] <> null))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_service_spend_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}, PreviewOnlySteps = {"Kept top rows"}]
shared fact_service_spend = let
  Source = Table.Combine({fact_service_spend_archive, fact_service_spend_current}),
  #"Added custom 7" = Table.AddColumn(Source, "Month Date", each Text.Middle([Service Date],3,2) &"-"& Text.Start([Service Date],2) &"-"& Text.End([Service Date],4)),
  #"Kept top rows" = Table.FirstN(#"Added custom 7", 1000),
    #"Merged Queries1" = Table.NestedJoin(#"Kept top rows", {"Payer"}, _map_company, {"Debtor Number"}, "_map_company", JoinKind.LeftOuter),
  #"Expanded _map_company" = Table.ExpandTableColumn(#"Merged Queries1", "_map_company", {"Company Code"}, {"Company Code"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Expanded _map_company",{{"Company Code", "Data For"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns1",{{"Data For", type text}}),
    #"Added Custom1" = Table.AddColumn(#"Changed Type1", "Material and Sales Organisation", each [Sales Organization]& "-"& [Material]),
    #"Merged Queries" = Table.NestedJoin(#"Added Custom1", {"Data For"}, _map_tax_rates, {"Sales Organisation"}, "_map_tax_rates", JoinKind.LeftOuter),
  #"Expanded _map_tax_rates" = Table.ExpandTableColumn(#"Merged Queries", "_map_tax_rates", {"Tax Rate"}, {"Tax Rate"}),
    #"Added Custom2" = Table.AddColumn(#"Expanded _map_tax_rates", "Tax Rate Corrected", each if [Sales Organization] = [Data For] then 1 else 1+ [Tax Rate]),
    #"Removed Columns1" = Table.RemoveColumns(#"Added Custom2",{"Tax Rate"}),
    #"Renamed Columns2" = Table.RenameColumns(#"Removed Columns1",{{"Tax Rate Corrected", "Tax Rate"}}),
    #"Changed Type2" = Table.TransformColumnTypes(#"Renamed Columns2",{{"Amount", Currency.Type}, {"Tax Rate", Currency.Type},  {"Service Date", type date}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type2",{"Semantic Key CAP"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns","DEACT_","",Replacer.ReplaceText,{"Account Assigned To"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","_DIS","",Replacer.ReplaceText,{"Account Assigned To"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","_PAD01","",Replacer.ReplaceText,{"Account Assigned To"}),
    #"Replaced Value3" = Table.ReplaceValue(#"Replaced Value2","_PAD0","",Replacer.ReplaceText,{"Account Assigned To"}),
    #"Replaced Value4" = Table.ReplaceValue(#"Replaced Value3","_ALLIANZ","",Replacer.ReplaceText,{"Account Assigned To"}),
    #"Merged Queries2" = Table.NestedJoin(#"Replaced Value4", {"Account Assigned To"}, _map_sys_users, {"final_user_name"}, "_map_sys_users", JoinKind.LeftOuter),
  #"Expanded _map_sys_users 1" = Table.ExpandTableColumn(#"Merged Queries2", "_map_sys_users", {"legal_entity", "name"}, {"legal_entity", "name"}),
    #"Renamed Columns3" = Table.RenameColumns(#"Expanded _map_sys_users 1",{{"legal_entity", "User Assigned Legal Entity"}, {"name", "User Assigned Name"}}),
    #"Merged Queries3" = Table.NestedJoin(#"Renamed Columns3", {"Material"}, _map_services_spm, {"Material"}, "_map_services_spm", JoinKind.LeftOuter),
  #"Expanded _map_services_spm" = Table.ExpandTableColumn(#"Merged Queries3", "_map_services_spm", {"Internal IT Cost", "Category"}, {"Internal IT Cost", "Category"}),
    #"Replaced Value5" = Table.ReplaceValue(#"Expanded _map_services_spm",null,"N/A",Replacer.ReplaceValue,{"Internal IT Cost"}),
    #"Replaced Value6" = Table.ReplaceValue(#"Replaced Value5",null,"N/A",Replacer.ReplaceValue,{"Category"}),
    #"Merged Queries4" = Table.NestedJoin(#"Replaced Value6", {"Account Assigned To"}, _map_user_login_data, {"User ID"}, "_map_user_login_data", JoinKind.LeftOuter),
  #"Expanded _map_user_login_data" = Table.ExpandTableColumn(#"Merged Queries4", "_map_user_login_data", {"Ovl Last Login", "Days Without Login"}, {"Ovl Last Login", "Days Without Login"}),
    #"Filtered Rows2" = Table.SelectRows(#"Expanded _map_user_login_data", each ([SAP Refresh Time] <> "SAP Refresh Time")),
    #"Changed Type" = Table.TransformColumnTypes(#"Filtered Rows2", {{"SAP Refresh Time", type datetime}, {"Quantity", type number}}),
  #"Added custom" = Table.AddColumn(#"Changed Type", "Amount incl. Tax", each [Amount] * [Tax Rate]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Amount incl. Tax", Currency.Type}, {"Days Without Login", Int64.Type}}),
  #"Added custom 1" = Table.AddColumn(#"Changed column type", "Last Login", each if [Days Without Login] = null then "Unknown" else
        if [Days Without Login] <= 30 then "Between 0 and 30 Days" else
        if [Days Without Login] <= 90 then "Between 30 and 90 Days" else
        if [Days Without Login] <= 180 then "Between 90 and 180 Days" else
        if [Days Without Login] <= 360 then "Between 180 and 360 Days" else
        "More than 360 Days"),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "Quote ID Link", each if [Quote ID]<>"#" then "https://aztech.service-now.com/nav_to.do?uri=%2Fu_csm_quote.do%3Fsysparm_query=number=" & [Quote ID] else "#"),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Service Instance ID Link", each if [Service Instance ID]<>"#" then "https://aztech.service-now.com/nav_to.do?uri=%2Fu_cmdb_ci_si_billing_plan.do?sysparm_query=u_number%3D" & [Service Instance ID] else "#"),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "User Assigned", each [Account Assigned To] & "-" & [User Assigned Name]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 4", {{"User Assigned", type text}, {"Service Instance ID Link", type text}, {"Quote ID Link", type text}, {"Last Login", type text}, {"Material and Sales Organisation", type text}, {"Refresh Date", type datetime}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type 1", {"SAP Refresh Time"}),
  #"Added custom 5" = Table.AddColumn(#"Removed columns 1", "Material Key", each [Sales Organization] & "-" & [Material]),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Added custom 5", {{"Material Key", type text}}),
  #"Added custom 6" = Table.AddColumn(#"Changed column type 2", "Service Instance ID and Name", each [Service Instance ID] & " - " & [SI Name]),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Added custom 6", {{"Service Instance ID and Name", type text}, {"Month Date", type date}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Changed column type 3", {"Service Date"})
in
   #"Removed columns 2";
shared _map_company = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_company", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared _map_tax_rates = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_tax_rates", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Tax Rate", type number}})
in
  #"Changed column type";
shared _map_sys_users = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_ppm_sys_users", ItemKind = "Table"]}[Data],
  #"Removed Duplicates" = Table.Distinct(#"Navigation 2", {"legal_entity", "user_name"}),
  #"Filtered Rows" = Table.SelectRows(#"Removed Duplicates", each [user_name] <> ""),
  #"Added Custom" = Table.AddColumn(#"Filtered Rows", "user_name_cleaned", each [user_name]),
  #"Replaced Value" = Table.ReplaceValue(#"Added Custom", "DEACT_", "", Replacer.ReplaceText, {"user_name_cleaned"}),
  #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value", "_DIS", "", Replacer.ReplaceText, {"user_name_cleaned"}),
  #"Added Custom1" = Table.AddColumn(#"Replaced Value1", "Custom", each if [u_local_logon_name] <> "" then [u_local_logon_name] else [user_name_cleaned]),
  #"Renamed Columns" = Table.RenameColumns(#"Added Custom1", {{"Custom", "final_user_name"}}),
  #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns", {"user_name_cleaned"}),
  #"Replaced Value2" = Table.ReplaceValue(#"Removed Columns", "_PAD01", "", Replacer.ReplaceText, {"final_user_name"}),
  #"Replaced Value3" = Table.ReplaceValue(#"Replaced Value2", "_PAD0", "", Replacer.ReplaceText, {"final_user_name"}),
  #"Removed Duplicates1" = Table.Distinct(#"Replaced Value3", {"sys_id"}),
  #"Removed Duplicates2" = Table.Distinct(#"Removed Duplicates1", {"final_user_name"}),
  #"Lowercased Text" = Table.TransformColumns(#"Removed Duplicates2",{{"email", Text.Lower, type text}})
in
  #"Lowercased Text";
shared _map_user_login_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_user_login_data", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared _map_services_spm = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_service_mapping", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_service_spend_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_service_spend", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
