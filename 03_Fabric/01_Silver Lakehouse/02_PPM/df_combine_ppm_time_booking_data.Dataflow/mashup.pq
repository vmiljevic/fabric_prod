[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_ppm_time_cards_archive = let
    // --- Navigate to Lakehouse ---
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    LH_Table = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Filtered rows" = Table.SelectRows(LH_Table, each Text.Contains([Id], "time_cards_current") or Text.Contains([Id], "time_cards_archive") ),

    // Convert lakehouse metadata table → list of records
    LH_List = Table.ToRecords(#"Filtered rows"),

    // Load each table via its [Data] field
    LoadedTables = List.Transform(LH_List, each _[Data]),

    // Combine all fact_ppm_* tables
    Combined = Table.Combine(LoadedTables),
  #"Removed duplicates" = Table.Distinct(Combined, {"ExportFor", "tc_sys_id"})
in
    #"Removed duplicates";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_ppm_time_cards_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_ppm_time_cards = let
    // --- Navigate to Lakehouse ---
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    LH_Table = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  Navigation = LH_Table{[Id = "fact_ppm_time_cards_daily", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(Navigation, {{"sys_updated_on", Order.Descending}}),
  #"Removed duplicates" = Table.Distinct(#"Sorted rows", {"tc_sys_id", "ExportFor"}),
  #"Appended query" = Table.Combine({#"Removed duplicates", fact_ppm_time_cards_archive}),
  #"Removed duplicates 1" = Table.Distinct(#"Appended query", {"tc_sys_id", "ExportFor"}),
  #"Filtered rows" = Table.SelectRows(#"Removed duplicates 1", each [tc_sys_id] <> null)
in
    #"Filtered rows";
shared fact_ppm_time_card_interface_archive = let
    // --- Navigate to Lakehouse ---
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    LH_Table = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Filtered rows" = Table.SelectRows(LH_Table, each Text.Contains([Id], "interface") and (Text.Contains([Id], "current") or Text.Contains([Id], "archive"))),

    // Convert lakehouse metadata table → list of records
    LH_List = Table.ToRecords(#"Filtered rows"),

    // Load each table via its [Data] field
    LoadedTables = List.Transform(LH_List, each _[Data]),

    // Combine all fact_ppm_* tables
    Combined = Table.Combine(LoadedTables),
  #"Removed duplicates" = Table.Distinct(Combined, {"ExportFor", "tc_sys_id"})
in
    #"Removed duplicates";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_ppm_time_card_interface_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_ppm_time_card_interface = let
    // --- Navigate to Lakehouse ---
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
    LH_Table = Workspace{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  Navigation = LH_Table{[Id = "fact_ppm_time_card_interface_daily", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(Navigation, {{"sys_updated_on", Order.Descending}}),
  #"Removed duplicates" = Table.Distinct(#"Sorted rows", {"tc_sys_id", "ExportFor"}),
  #"Appended query" = Table.Combine({#"Removed duplicates", fact_ppm_time_card_interface_archive}),
  #"Removed duplicates 1" = Table.Distinct(#"Appended query", {"tc_sys_id", "ExportFor"}),
  #"Filtered rows" = Table.SelectRows(#"Removed duplicates 1", each [tc_sys_id] <> null)
in
    #"Filtered rows";
shared fact_ppm_time_card_interface_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_ppm_time_card_interface", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_ppm_time_cards_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_ppm_time_cards", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
