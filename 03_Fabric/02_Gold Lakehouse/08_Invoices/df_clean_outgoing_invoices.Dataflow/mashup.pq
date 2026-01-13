[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_outgoing_invoices_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_outgoing_invoices_current", ItemKind = "Table"]}[Data],
      #"Added Custom" = Table.AddColumn(#"Navigation 2", "Posting Date New", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then #date(2022, 12, 1) else [Posting Date]),
    #"Added Custom3" = Table.AddColumn(#"Added Custom", "Net due date new", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then #date(2022, 12, 1) else [Net due date]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom3", "Fiscal Year New", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then "2022" else [Fiscal Year]),
    #"Added Custom5" = Table.AddColumn(#"Added Custom4", "Year/month new", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then "2022/12" else [#"Year/month"]),
    #"Added Custom6" = Table.AddColumn(#"Added Custom5", "Posting period new", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then "12" else [Posting Period]),
    #"Removed Columns1" = Table.RemoveColumns(#"Added Custom6",{"Posting Date", "Net due date", "Fiscal Year", "Year/month", "Posting Period"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns1",{{"Posting Date New", "Posting Date"}, {"Net due date new", "Net due date"}, {"Fiscal Year New", "Fiscal Year"}, {"Year/month new", "Year/month"}, {"Posting period new", "Posting period"}}),
    #"Changed Type22" = Table.TransformColumnTypes(#"Renamed Columns",{{"Clearing date", type date}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Changed Type22",{ {"Document Date", type date}, {"Posting Date", type date}, {"Amount in local currency", Currency.Type}}),
    #"Renamed Columns1" = Table.RenameColumns(#"Changed Type1",{{"Customer", "Debtor Number"}}),
    #"Filtered Rows" = Table.SelectRows(#"Renamed Columns1", each ([Posting Key] <> "")),
    #"Changed Type2" = Table.TransformColumnTypes(#"Filtered Rows",{{"Net due date", type date}}),
    #"Added Custom22" = Table.AddColumn(#"Changed Type2", "Overdue Days", each if [Clearing date] is null then DateTime.Date(DateTime.LocalNow())-[Net due date] else 0),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom22",{{"Overdue Days", Int64.Type}}),
    #"Added Custom12" = Table.AddColumn(#"Changed Type3", "Month Date", each Date.StartOfMonth([Net due date])),
    #"Changed Type4" = Table.TransformColumnTypes(#"Added Custom12",{{"Month Date", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed Type4", {{"Debtor Number", "Payer Number"}}),
    #"Merged Queries" = Table.NestedJoin(#"Renamed columns 1", {"Payer Number"}, dim_payer, {"Payer"}, "dim_payer", JoinKind.LeftOuter),
  #"Expanded dim_payer" = Table.ExpandTableColumn(#"Merged Queries", "dim_payer", {"Payer Name"}, {"Payer Name"}),
    #"Replaced Value" = Table.ReplaceValue(#"Expanded dim_payer",null,"NA Debtor Name",Replacer.ReplaceValue,{"Payer Name"}),
    #"Added Custom23" = Table.AddColumn(#"Replaced Value", "Payer Number and Name", each [Payer Number] & " - " & [Payer Name]),
    #"Changed Type" = Table.TransformColumnTypes(#"Added Custom23",{{"Amount in loc.curr.2", Currency.Type}}),
    #"Renamed Columns2" = Table.RenameColumns(#"Changed Type",{{"Amount in loc.curr.2", "Amount in EUR"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed Columns2", {{"Refresh Date", type datetime}, {"Payer Number and Name", type text}, {"Posting period", type text}, {"Year/month", type text}, {"Fiscal Year", type text}}),
  #"Added custom 1" = Table.AddColumn(#"Changed column type", "Status", each if [Clearing date] = null then
            if [Overdue Days] > 30 then "Open - Overdue" else "Open"
        else
            "Paid"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"Status", type text}})
in
  #"Changed column type 1";
shared dim_payer = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_payer", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_outgoing_invoices_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_outgoing_invoices_archive", ItemKind = "Table"]}[Data],
      #"Added Custom" = Table.AddColumn(#"Navigation 2", "Posting Date New", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then #date(2022, 12, 1) else [Posting Date]),
    #"Added Custom3" = Table.AddColumn(#"Added Custom", "Net due date new", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then #date(2022, 12, 1) else [Net due date]),
    #"Added Custom4" = Table.AddColumn(#"Added Custom3", "Fiscal Year New", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then "2022" else [Fiscal Year]),
    #"Added Custom5" = Table.AddColumn(#"Added Custom4", "Year/month new", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then "2022/12" else [#"Year/month"]),
    #"Added Custom6" = Table.AddColumn(#"Added Custom5", "Posting period new", each if Text.StartsWith([Assignment],"AzTec 22-") and [Posting Date] = #date(2023, 1, 1) then "12" else [Posting Period]),
    #"Removed Columns1" = Table.RemoveColumns(#"Added Custom6",{"Posting Date", "Net due date", "Fiscal Year", "Year/month", "Posting Period"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns1",{{"Posting Date New", "Posting Date"}, {"Net due date new", "Net due date"}, {"Fiscal Year New", "Fiscal Year"}, {"Year/month new", "Year/month"}, {"Posting period new", "Posting period"}}),
    #"Changed Type22" = Table.TransformColumnTypes(#"Renamed Columns",{{"Clearing date", type date}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Changed Type22",{ {"Document Date", type date}, {"Posting Date", type date}, {"Amount in local currency", Currency.Type}}),
    #"Renamed Columns1" = Table.RenameColumns(#"Changed Type1",{{"Customer", "Debtor Number"}}),
    #"Filtered Rows" = Table.SelectRows(#"Renamed Columns1", each ([Posting Key] <> "")),
    #"Changed Type2" = Table.TransformColumnTypes(#"Filtered Rows",{{"Net due date", type date}}),
    #"Added Custom22" = Table.AddColumn(#"Changed Type2", "Overdue Days", each if [Clearing date] is null then DateTime.Date(DateTime.LocalNow())-[Net due date] else 0),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom22",{{"Overdue Days", Int64.Type}}),
    #"Added Custom12" = Table.AddColumn(#"Changed Type3", "Month Date", each Date.StartOfMonth([Net due date])),
    #"Changed Type4" = Table.TransformColumnTypes(#"Added Custom12",{{"Month Date", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed Type4", {{"Debtor Number", "Payer Number"}}),
    #"Merged Queries" = Table.NestedJoin(#"Renamed columns 1", {"Payer Number"}, dim_payer, {"Payer"}, "dim_payer", JoinKind.LeftOuter),
  #"Expanded dim_payer" = Table.ExpandTableColumn(#"Merged Queries", "dim_payer", {"Payer Name"}, {"Payer Name"}),
    #"Replaced Value" = Table.ReplaceValue(#"Expanded dim_payer",null,"NA Debtor Name",Replacer.ReplaceValue,{"Payer Name"}),
    #"Added Custom23" = Table.AddColumn(#"Replaced Value", "Payer Number and Name", each [Payer Number] & " - " & [Payer Name]),
    #"Changed Type" = Table.TransformColumnTypes(#"Added Custom23",{{"Amount in loc.curr.2", Currency.Type}}),
    #"Renamed Columns2" = Table.RenameColumns(#"Changed Type",{{"Amount in loc.curr.2", "Amount in EUR"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed Columns2", {{"Refresh Date", type datetime}, {"Payer Number and Name", type text}, {"Posting period", type text}, {"Year/month", type text}, {"Fiscal Year", type text}}),
  #"Added custom 1" = Table.AddColumn(#"Changed column type", "Status", each if [Clearing date] = null then
            if [Overdue Days] > 30 then "Open - Overdue" else "Open"
        else
            "Paid"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"Status", type text}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_outgoing_invoices_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_outgoing_invoices = let
  Source = Table.Combine({fact_outgoing_invoices_current, fact_outgoing_invoices_archive}),
  #"Filtered rows" = Table.SelectRows(Source, each ([Document Number] <> null))
in
  #"Filtered rows";
shared fact_outgoing_invoices_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_outgoing_invoices", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
