[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared dim_cost_center_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_cost_center_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_cost_center_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_cost_center_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cost_center_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Cost Center", DestinationColumnName = "Cost Center"], [SourceColumnName = "Cost Center Name", DestinationColumnName = "Cost Center Name"], [SourceColumnName = "Company Code", DestinationColumnName = "Company Code"], [SourceColumnName = "Company Code Name", DestinationColumnName = "Company Code Name"], [SourceColumnName = "Profit Center", DestinationColumnName = "Profit Center"], [SourceColumnName = "Profit Center Name", DestinationColumnName = "Profit Center Name"], [SourceColumnName = "Pillar", DestinationColumnName = "Pillar"], [SourceColumnName = "Person Responsible", DestinationColumnName = "Person Responsible"], [SourceColumnName = "Department", DestinationColumnName = "Department"], [SourceColumnName = "Costcenter Typ 1", DestinationColumnName = "Costcenter Typ 1"], [SourceColumnName = "Costcenter Typ 2", DestinationColumnName = "Costcenter Typ 2"], [SourceColumnName = "Costcenter Typ 3", DestinationColumnName = "Costcenter Typ 3"], [SourceColumnName = "Valid From", DestinationColumnName = "Valid From"], [SourceColumnName = "Valid To", DestinationColumnName = "Valid To"], [SourceColumnName = "Refresh Date", DestinationColumnName = "Refresh Date"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "Cost Center Number and Name", DestinationColumnName = "Cost Center Number and Name"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_cost_center = let
  Source = Table.Combine({dim_cost_center_current, dim_cost_center_archive}),
  #"Removed columns" = Table.RemoveColumns(Source, {"File Name", "Lock Indicator  Actuals Primary Cost", "Lock Indicator Actuals Secondary Cost"}),
  #"Added custom" = Table.AddColumn(#"Removed columns", "Cost Center Number and Name", each [Cost Center] & " - " & [Cost Center Name]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Refresh Date", type datetime}, {"Year", Int64.Type}, {"Cost Center Number and Name", type text}})
in
  #"Changed column type";
shared dim_material_prices_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_material_prices_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_material_prices_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_material_prices_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_material_price_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_material_price = let
  Source = Table.Combine({dim_material_prices_archive, dim_material_prices_current}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Refresh Date", type datetime}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "Price Key", each [Sales Organization] & "-" & [Material]),
  #"Renamed columns" = Table.RenameColumns(#"Added custom", {{"Condition Unit", "Price Unit"}, {"Indicator Quantity", "Price Type"}, {"Conditonal Pricing Unit", "Price Quantity"}, {"Price Group_2", "Price Group Description"}}),
  #"Removed other columns" = Table.SelectColumns(#"Renamed columns", {"Price Type", "Price Group Description", "Price Key", "Price Quantity", "Amount", "Currency", "Price Unit"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed other columns", {{"Amount", Currency.Type}}),
  #"Unpivoted columns" = Table.Pivot(#"Changed column type 1", List.Distinct(#"Changed column type 1"[#"Price Group Description"]), "Price Group Description", "Amount", List.Sum),
  #"Renamed columns 1" = Table.RenameColumns(#"Unpivoted columns", {{"External Market Eur.", "Price External Market"}, {"outside VAT group", "Price Outside VAT Group"}, {"inside VAT group", "Price Inside VAT Group"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 1", {"not in use"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed columns", {{"Price Type", type text}, {"Price Unit", type text}, {"Currency", type text}, {"Price Quantity", type text}, {"Price Key", type text}})

in
  #"Changed column type 2";
shared dim_materials_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_materials_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_materials_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_materials_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_material_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_material = let
    // 1️⃣ Combine main tables
    Combined = Table.Combine({dim_materials_current, dim_materials_archive}),

    // 2️⃣ Fix column types
    Typed = Table.TransformColumnTypes(Combined, {{"Refresh Date", type datetime}}),

    // 3️⃣ Pad text keys
    Padded = Table.TransformColumns(
        Typed,
        {
            {"AZT Landscape", each if _ <> null then Text.PadStart(Text.From(_), 2, "0") else null, type text},
            {"AZT Service Class", each if _ <> null then Text.PadStart(Text.From(_), 2, "0") else null, type text},
            {"AZT Service Package", each if _ <> null then Text.PadStart(Text.From(_), 3, "0") else null, type text}
        }
    ),
  #"Merged queries" = Table.NestedJoin(Padded, {"AZT Service Package"}, sup_material_landscape, {"Service Package Key"}, "sup_material_landscape", JoinKind.LeftOuter),
  #"Expanded sup_material_landscape" = Table.ExpandTableColumn(#"Merged queries", "sup_material_landscape", {"Service Landscape Name", "Service Class Name", "Service Package Name"}, {"Service Landscape Name", "Service Class Name", "Service Package Name"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded sup_material_landscape", each ([Service Landscape Name] <> null)),
  #"Added custom" = Table.AddColumn(#"Filtered rows", "Profit Center and Name", each [Profit center] & " - " & [Profit center_4]),
  #"Renamed columns" = Table.RenameColumns(#"Added custom", {{"Profit center_4", "Profit Center Name"}, {"Profit center", "Profit Center"}}),
  #"Added custom 1" = Table.AddColumn(#"Renamed columns", "Material Key", each [Sales Organization] & "-" & [Material]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 1", {{"Profit Center and Name", type text}, {"Material Key", type text}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type", "Material Number and Name", each [Material] & " - " & [Material Name]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 2", {{"Material Number and Name", type text}})
in
    #"Changed column type 1";
shared dim_payer_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_payer_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_payer_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_payer_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_payer_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_payer = let
  Source = Table.Combine({dim_payer_current, dim_payer_archive}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Refresh Date", type datetime}})
in
  #"Changed column type";
shared dim_profit_center_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_profit_center_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_profit_center_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_profit_center_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_profit_center_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_profit_center = let
  Source = Table.Combine({dim_profit_center_current, dim_profit_center_archive}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Refresh Date", type datetime}})
in
    #"Changed column type";
shared dim_rates_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_rates_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_rates_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_rates_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_rate_card_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_rate_card = let
  Source = Table.Combine({dim_rates_archive, dim_rates_current}),
  #"Added custom" = Table.AddColumn(Source, "Rate Key Short", each [Sales Organization] & "-" & [Material]),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "Rate Key Long", each [Sales Organization] & "-" & [Material] & "-" & [Cost Center]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 1", {{"Refresh Date", type datetime}, {"Rate", Currency.Type}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type", "Rate per Day", each [Rate] * 8),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 2", {{"Rate per Day", Currency.Type}, {"Rate Key Long", type text}, {"Rate Key Short", type text}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type 1", {"Sales Organization", "Material"})
in
  #"Removed duplicates";
shared dim_wbs_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_wbs_archive", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dim_wbs_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_wbs_current", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_wbs_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_wbs = let
  Source = Table.Combine({dim_wbs_current, dim_wbs_archive}),
  #"Added custom" = Table.AddColumn(Source, "WBS Element and Name", each [WBS Element] & " - " & [WBS Element Name]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"Refresh Date", type datetime}, {"WBS Element and Name", type text}})
in
  #"Changed column type";
shared dim_material_price_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_material_price", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_payer_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_payer", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_rate_card_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_rate_card", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_profit_center_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_profit_center", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_wbs_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_wbs", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared sup_material_landscape = let
  Source = dim_profit_center,
  #"Removed other columns" = Table.SelectColumns(Source, {"Service Landscape", "Service Landscape Name", "Service Class", "Service Class Name", "Service Package", "Service Package Name"}),
  #"Added custom 2" = Table.AddColumn(#"Removed other columns", "Service Package Key", each if [Service Package] <> "#" then Text.End([Service Package], 3) else ""),
  #"Filtered rows" = Table.SelectRows(#"Added custom 2", each ([Service Landscape] <> "#")),
  #"Removed duplicates" = Table.Distinct(#"Filtered rows", {"Service Landscape", "Service Class", "Service Package"})
in
 #"Removed duplicates";
shared dim_material_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_material", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_cost_center_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cost_center", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
