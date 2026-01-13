[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_incoming_invoices_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_incoming_invoices_current", ItemKind = "Table"]}[Data],
  #"Filtered rows 2" = Table.SelectRows(#"Navigation 2", each ([Company Code] <> null and [Company Code] <> "")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows 2", {{"Document Date", type date}}),
   #"Added Custom12" = Table.AddColumn(#"Changed column type", "Month Date", each Date.StartOfMonth([Document Date])),
    #"Changed Type" = Table.TransformColumnTypes(#"Added Custom12",{ {"Month Date", type date}}, "de-DE"),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{{"Amount in loc.curr.2", "Amount in EUR"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns",{{"Amount in EUR", Currency.Type}, {"Amount in local currency", Currency.Type}, {"Posting Date", type date}, {"Document Date", type date}, {"Net due date", type date}}),
    #"Filtered Rows" = Table.SelectRows(#"Changed Type1", each ([Company Code] <> "")),
    #"Added Custom" = Table.AddColumn(#"Filtered Rows", "Current Date", each DateTime.LocalNow()),
    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom",{{"Current Date", type date}}),
    #"Added Custom3" = Table.AddColumn(#"Changed Type2", "Status", each if [Net due date] < [Current Date] then "Open - Overdue" else "Open"),
    #"Added Custom4" = Table.AddColumn(#"Added Custom3", "Overdue Days", each if [Net due date] < [Current Date] then [Current Date] - [Net due date] else 0),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom4",{{"Overdue Days", Int64.Type}}),
  #"Added custom 6" = Table.AddColumn(#"Changed Type3", "Vendor Key", each [Company Code] & "-" & [Account]),
    #"Merged Queries" = Table.NestedJoin(#"Added custom 6", {"Vendor Key"}, dim_vendors, {"Vendor Key"}, "dim_vendors", JoinKind.LeftOuter),
  #"Expanded dim_vendors" = Table.ExpandTableColumn(#"Merged Queries", "dim_vendors", {"Vendor Name"}, {"Vendor Name"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_vendors", {"Document Type"}, map_document_type, {"Code"}, "map_document_type", JoinKind.LeftOuter),
  #"Expanded map_document_type" = Table.ExpandTableColumn(#"Merged Queries1", "map_document_type", {"English Name"}, {"Document Type Name"}),
    #"Merged Queries2" = Table.NestedJoin(#"Expanded map_document_type", {"G/L Account"}, map_gl_accounts, {"GL Account"}, "map_gl_accounts", JoinKind.LeftOuter),
  #"Expanded map_gl_accounts" = Table.ExpandTableColumn(#"Merged Queries2", "map_gl_accounts", {"GL Account Name"}, {"GL Account Name"}),
  #"Added custom 1" = Table.AddColumn(#"Expanded map_gl_accounts", "Overdue Days Range", each if [Overdue Days] > 0 and [Overdue Days] < 7 then "Between 0 and 7 days" else
        if [Overdue Days] >= 7 and [Overdue Days] < 15 then "Between 7 and 15 days" else
        if [Overdue Days] >= 15 and [Overdue Days] < 30 then "Between 15 and 30 days" else
        if [Overdue Days] >= 30 then "More than 30 days" 
        else "Not Due"),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "GL Account Number and Name", each [#"G/L Account"] & " - " & [GL Account Name]),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Document Type Code and Name ", each [Document Type] & " - " & [Document Type Name]),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Vendor Number and Name", each [Account] & " - " & [Vendor Name]),
  #"Added custom 5" =  Table.AddColumn(#"Added custom 4", "Processing",
        each if [File Name] <> null and Text.Contains([File Name], "Parked")
            then "Parked"
            else "Processing"
    ),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 5", {{"Processing", type text}, {"Vendor Number and Name", type text}, {"Document Type Code and Name ", type text}, {"GL Account Number and Name", type text}, {"Overdue Days Range", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"Cleared/open items symbol"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed columns", {{"Status", type text}, {"Refresh Date", type datetime}})
in
  #"Changed column type 2";
shared map_gl_accounts = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_gl_accounts", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared map_document_type = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "map_document_type", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_vendors = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_vendor", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_incoming_invoices_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_incoming_invoices_archive", ItemKind = "Table"]}[Data],
  #"Filtered rows 2" = Table.SelectRows(#"Navigation 2", each ([Company Code] <> null and [Company Code] <> "")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows 2", {{"Document Date", type date}}),
   #"Added Custom12" = Table.AddColumn(#"Changed column type", "Month Date", each Date.StartOfMonth([Document Date])),
    #"Changed Type" = Table.TransformColumnTypes(#"Added Custom12",{ {"Month Date", type date}}, "de-DE"),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{{"Amount in loc.curr.2", "Amount in EUR"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns",{{"Amount in EUR", Currency.Type}, {"Amount in local currency", Currency.Type}, {"Posting Date", type date}, {"Document Date", type date}, {"Net due date", type date}}),
    #"Filtered Rows" = Table.SelectRows(#"Changed Type1", each ([Company Code] <> "")),
    #"Added Custom" = Table.AddColumn(#"Filtered Rows", "Current Date", each DateTime.LocalNow()),
    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom",{{"Current Date", type date}}),
    #"Added Custom3" = Table.AddColumn(#"Changed Type2", "Status", each if [Net due date] < [Current Date] then "Open - Overdue" else "Open"),
    #"Added Custom4" = Table.AddColumn(#"Added Custom3", "Overdue Days", each if [Net due date] < [Current Date] then [Current Date] - [Net due date] else 0),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom4",{{"Overdue Days", Int64.Type}}),
  #"Added custom 6" = Table.AddColumn(#"Changed Type3", "Vendor Key", each [Company Code] & "-" & [Account]),
    #"Merged Queries" = Table.NestedJoin(#"Added custom 6", {"Vendor Key"}, dim_vendors, {"Vendor Key"}, "dim_vendors", JoinKind.LeftOuter),
  #"Expanded dim_vendors" = Table.ExpandTableColumn(#"Merged Queries", "dim_vendors", {"Vendor Name"}, {"Vendor Name"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_vendors", {"Document Type"}, map_document_type, {"Code"}, "map_document_type", JoinKind.LeftOuter),
  #"Expanded map_document_type" = Table.ExpandTableColumn(#"Merged Queries1", "map_document_type", {"English Name"}, {"Document Type Name"}),
    #"Merged Queries2" = Table.NestedJoin(#"Expanded map_document_type", {"G/L Account"}, map_gl_accounts, {"GL Account"}, "map_gl_accounts", JoinKind.LeftOuter),
  #"Expanded map_gl_accounts" = Table.ExpandTableColumn(#"Merged Queries2", "map_gl_accounts", {"GL Account Name"}, {"GL Account Name"}),
  #"Added custom 1" = Table.AddColumn(#"Expanded map_gl_accounts", "Overdue Days Range", each if [Overdue Days] > 0 and [Overdue Days] < 7 then "Between 0 and 7 days" else
        if [Overdue Days] >= 7 and [Overdue Days] < 15 then "Between 7 and 15 days" else
        if [Overdue Days] >= 15 and [Overdue Days] < 30 then "Between 15 and 30 days" else
        if [Overdue Days] >= 30 then "More than 30 days" 
        else "Not Due"),
  #"Added custom 2" = Table.AddColumn(#"Added custom 1", "GL Account Number and Name", each [#"G/L Account"] & " - " & [GL Account Name]),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Document Type Code and Name ", each [Document Type] & " - " & [Document Type Name]),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Vendor Number and Name", each [Account] & " - " & [Vendor Name]),
  #"Added custom 5" =  Table.AddColumn(#"Added custom 4", "Processing",
        each if [File Name] <> null and Text.Contains([File Name], "Parked")
            then "Parked"
            else "Processing"
    ),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 5", {{"Processing", type text}, {"Vendor Number and Name", type text}, {"Document Type Code and Name ", type text}, {"GL Account Number and Name", type text}, {"Overdue Days Range", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type 1", {"Cleared/open items symbol"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed columns", {{"Status", type text}, {"Refresh Date", type datetime}})
in
  #"Changed column type 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_incoming_invoices_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_incoming_invoices = let
  Source = Table.Combine({fact_incoming_invoices_current, fact_incoming_invoices_archive}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Vendor Key", type text}})
in
  #"Changed column type";
shared fact_incoming_invoices_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_incoming_invoices", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
