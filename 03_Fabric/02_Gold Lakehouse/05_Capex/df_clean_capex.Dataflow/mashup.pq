[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_capex_bookings_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_capex_bookings_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared fact_capex_bookings_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_capex_bookings_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_capex_bookings_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_capex_bookings = let
  Source = Table.Combine({fact_capex_bookings_archive, fact_capex_bookings_current}),
      #"Filtered Rows" = Table.SelectRows(Source, each ([Document Date] <> null)),
    #"Changed Type" = Table.TransformColumnTypes(#"Filtered Rows",{{"Document Date", type date}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type",{"Amount in local currency", "Local Currency", "Trading partner", "Posting Period", "Year/month", "Cost Element"}),
    #"Filtered Rows1" = Table.SelectRows(#"Removed Columns", each ([Asset] <> "" and [Asset] <> "*")),
    #"Renamed Columns" = Table.RenameColumns(#"Filtered Rows1",{{"Text", "Booking Text"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns",{{"Posting Date", type date}, {"Amount in loc.curr.2", Currency.Type}}),
    #"Renamed Columns1" = Table.RenameColumns(#"Changed Type1",{{"Amount in loc.curr.2", "Amount in EUR"}}),
    #"Changed Type2" = Table.TransformColumnTypes(#"Renamed Columns1",{{"Time Stamp", type datetime}}),
    #"Removed Columns1" = Table.RemoveColumns(#"Changed Type2",{"Local Currency 2"}),
    #"Added Custom" = Table.AddColumn(#"Removed Columns1", "Asset Number", each [Asset]&"/"&[Asset Subnumber]),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom",{{"Asset Number", type text}}),
    #"Added Custom1" = Table.AddColumn(#"Changed Type3", "Asset Number Country", each [Company Code]&"-"&[Asset Number]),
    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "Month Date", each Date.StartOfMonth([Posting Date])),
    #"Changed Type4" = Table.TransformColumnTypes(#"Added Custom2",{{"Month Date", type date}}),
    #"Filtered Rows2" = Table.SelectRows(#"Changed Type4", each [Amount in EUR] <> 0),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered Rows2", {{"Asset Number Country", type text}, {"Refresh Date", type datetime}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type", {"Reference Key 1", "Reference Key 2", "Item", "Cost Center"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"Debit/Credit Ind_", "Debit and Credit Indicator"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 1", {"Asset Subnumber"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 2", {{"Offsetting acct no_", "Offsetting Account Number"}})
in
   #"Renamed columns 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_capex_assets_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_capex_assets = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_capex_assets", ItemKind = "Table"]}[Data],
  #"Renamed Columns" = Table.RenameColumns(#"Navigation 2",{{"Capitaliz. date", "Capitalization date"}, {"WBS Element for Acct Assignment of Costs", "WBS Element"}, {"Object", "Asset Number"}}),
    #"Renamed Columns1" = Table.RenameColumns(#"Renamed Columns",{{"Ord.dep.start date", "Depreciation Start Date"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns1",{{"Depreciation Start Date", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed Type1", {{"Depr. 2025", "Depreciation in 2025"}, {"Accumulated deprec_", "Accumulated depreciation"}, {"Cuml. APC/repl.val_", "Cumulative Value"}}),
    #"Added Custom" = Table.AddColumn(#"Renamed columns 1", "Asset Number Country", each [Company Code]&"-"&[Asset Number]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added Custom", {{"Refresh Date", type datetime}}),
    #"Added Custom10" = Table.AddColumn(#"Changed column type", "Asset Number and Name", each [Asset Number] & " - " & [Description]),
    #"Filtered Rows" = Table.SelectRows(#"Added Custom10", each ([Asset Number Country] <> "-")),
    #"Changed Type4" = Table.TransformColumnTypes(#"Filtered Rows", {{"APC acquisition 2025", Currency.Type}, {"Cumulative Value", Currency.Type}, {"Depreciation in 2025", Currency.Type}, {"Accumulated depreciation", Currency.Type}}),
  #"Added custom 1" = Table.AddColumn(#"Changed Type4", "Residual Value", each [Cumulative Value] + [Accumulated depreciation] + [Depreciation in 2025] + [APC acquisition 2025]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"Residual Value", Currency.Type}, {"Asset Number and Name", type text}, {"Asset Number Country", type text}, {"Capitalization date", type date}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type 1", "Capitalization Year", each Date.ToText([Capitalization date], [Format="yyyy", Culture="de-DE"])),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Capitalization Month", each Date.ToText([Capitalization date], [Format="MMM", Culture="en-US"])),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Capitalization Quarter", each "Q" & Text.From(Date.QuarterOfYear([Capitalization date]))),
  #"Renamed columns 2" = Table.RenameColumns(#"Added custom 4", {{"Capitalization date", "Capitalization Date"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 2", {{"Capitalization Year", type text}, {"Capitalization Month", type text}, {"Capitalization Quarter", type text}})
in
  #"Changed column type 2";
shared fact_capex_bookings_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_capex_bookings", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_capex_assets_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_capex_assets", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
